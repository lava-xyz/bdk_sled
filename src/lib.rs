use bdk::chain::{
    keychain::{KeychainChangeSet, KeychainTracker, PersistBackend},
    sparse_chain::ChainPosition,
};

pub struct SledStore<K, P> {
    db: sled::Tree,
    counter: usize,
    phantom: std::marker::PhantomData<(K, P)>,
}

impl<K, P> SledStore<K, P> {
    pub fn new(db: sled::Tree) -> Self {
        Self { db, counter: 0, phantom: std::marker::PhantomData }
    }

    fn iter_changesets(&self) -> impl Iterator<Item = Result<KeychainChangeSet<K, P>, sled::Error>>
    where
        KeychainChangeSet<K, P>: serde::de::DeserializeOwned,
    {
        self.db.iter().map(|k_v| {
            let (_k, v) = k_v?;
            let changeset = bincode::deserialize(&v).expect("Failed to deserialize changeset");
            Ok(changeset)
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
        self.db.flush().expect("Failed to flush changeset");

        self.counter += 1;
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

    #[tokio::test]
    async fn works() {
        let db: sled::Db = sled::Config::new().temporary(true).open().unwrap();
        let tree: sled::Tree = db.open_tree(b"abra").unwrap();

        let mut store = SledStore::new(tree);
        for changeset in test_changesets() {
            store.append_changeset(&changeset).expect("Should apply");
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
}
