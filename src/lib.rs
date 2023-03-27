use std::ops::RangeFull;

use bdk::chain::{
    keychain::{KeychainChangeSet, KeychainTracker, PersistBackend},
    sparse_chain::ChainPosition,
};
use sled::IVec;

pub struct SledStore<K, P> {
    db: sled::Tree,
    counter: usize,
    phantom: std::marker::PhantomData<(K, P)>,
}

impl<K, P> SledStore<K, P> {
    pub fn new(db: sled::Tree) -> Self {
        Self {
            db,
            counter: 0,
            phantom: std::marker::PhantomData,
        }
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

        Ok(())
    }

    fn load_into_keychain_tracker(
        &mut self,
        tracker: &mut KeychainTracker<K, P>,
    ) -> Result<(), Self::LoadError> {
        for k_v in self.db.range::<IVec, RangeFull>(..) {
            let (_k, v) = k_v?;
            tracker
                .apply_changeset(bincode::deserialize(&v).expect("Failed to deserialize changeset"))
        }
        Ok(())
    }
}
