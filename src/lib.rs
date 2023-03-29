//! A BDK's [`PersistBackend`] implementation for [`sled`].
//!
//! [`PersistBackend`]: bdk::chain::keychain::PersistBackend

use bdk::chain::{
    keychain::{KeychainChangeSet, KeychainTracker, PersistBackend},
    sparse_chain::ChainPosition,
};
use sled::IVec;

/// Implements [`PersistBackend`] for [`sled::Tree`].
///
/// [`PersistBackend`]: bdk::chain::keychain::PersistBackend
pub struct SledStore<K, P> {
    db: sled::Tree,
    counter: u64,
    phantom: std::marker::PhantomData<(K, P)>,
}

impl<K, P> SledStore<K, P> {
    /// Creates a new `SledStore` from a `sled::Tree`.
    ///
    /// Returns an error if `db` is corrupted. You must only use either empty
    /// `sled::Tree` or one previously used by [`SledStore`].
    pub fn new(db: sled::Tree) -> Result<Self, sled::Error> {
        let counter_bytes = db
            .get("counter")?
            .unwrap_or_else(|| IVec::from(0u64.to_le_bytes().to_vec()))
            .to_vec()
            .as_slice()
            .try_into()
            .expect("Invalid counter");

        Ok(Self {
            db,
            counter: u64::from_le_bytes(counter_bytes),
            phantom: std::marker::PhantomData,
        })
    }

    fn iter_changesets(&self) -> impl Iterator<Item = Result<KeychainChangeSet<K, P>, sled::Error>>
    where
        KeychainChangeSet<K, P>: serde::de::DeserializeOwned,
    {
        self.db.iter().filter_map(|k_v| {
            let Ok((k, v)) = k_v else {
                return None;
            };
            if k != "counter".as_bytes() {
                let changeset = bincode::deserialize(&v).expect("Failed to deserialize changeset");
                Some(Ok(changeset))
            } else {
                None
            }
        })
    }
}

impl<K, P> PersistBackend<K, P> for SledStore<K, P>
where
    K: Ord + Clone + std::fmt::Debug,
    P: ChainPosition,
    KeychainChangeSet<K, P>: serde::Serialize + serde::de::DeserializeOwned,
{
    type WriteError = sled::Error;
    type LoadError = sled::Error;

    fn append_changeset(
        &mut self,
        changeset: &KeychainChangeSet<K, P>,
    ) -> Result<(), Self::WriteError> {
        if changeset.is_empty() {
            return Ok(());
        }

        self.db.insert(
            self.counter.to_le_bytes(),
            bincode::serialize(changeset).expect("Failed to serialize changeset"),
        )?;
        self.counter += 1;
        self.db.insert("counter", &self.counter.to_le_bytes())?;

        Ok(())
    }

    fn load_into_keychain_tracker(
        &mut self,
        tracker: &mut KeychainTracker<K, P>,
    ) -> Result<(), Self::LoadError> {
        for changeset in self.iter_changesets() {
            tracker.apply_changeset(changeset?)
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use bdk::chain::{keychain::DerivationAdditions, TxHeight};
    use serde::{Deserialize, Serialize};

    use super::*;

    #[derive(Ord, PartialOrd, Eq, PartialEq, Clone, Debug, Serialize, Deserialize)]
    enum TestKeychain {
        External,
        Internal,
    }

    fn test_changesets() -> Vec<KeychainChangeSet<TestKeychain, TxHeight>> {
        vec![
            KeychainChangeSet {
                derivation_indices: DerivationAdditions(
                    vec![(TestKeychain::External, 42)].into_iter().collect(),
                ),
                chain_graph: Default::default(),
            },
            KeychainChangeSet {
                derivation_indices: DerivationAdditions(
                    vec![(TestKeychain::External, 43)].into_iter().collect(),
                ),
                chain_graph: Default::default(),
            },
        ]
    }

    fn new_tree() -> sled::Tree {
        let db: sled::Db = sled::Config::new().temporary(true).open().unwrap();
        db.open_tree(b"abra").unwrap()
    }

    #[test]
    fn works() {
        let tree = new_tree();

        let mut store = SledStore::new(tree).unwrap();
        assert_eq!(store.counter, 0);

        for (i, changeset) in test_changesets().into_iter().enumerate() {
            store.append_changeset(&changeset).expect("Should apply");

            assert_eq!(store.counter, i as u64 + 1);
            assert_eq!(
                store.db.get("counter").unwrap().unwrap().to_vec(),
                store.counter.to_le_bytes().to_vec()
            );
        }

        // Compare serialized changesets because `PartialEq` isn't implemented for
        // `KeychainChangeSet`.
        assert_eq!(
            bincode::serialize(&test_changesets()).unwrap(),
            bincode::serialize(&store.iter_changesets().collect::<Result<Vec<_>, _>>().unwrap())
                .unwrap()
        );

        // TODO: test `load_into_keychain_tracker`.
    }

    #[test]
    fn restores_counter() {
        let tree = new_tree();
        tree.insert("counter", &42u64.to_le_bytes()).unwrap();

        let store: SledStore<TestKeychain, TxHeight> = SledStore::new(tree).unwrap();
        assert_eq!(store.counter, 42);
    }
}
