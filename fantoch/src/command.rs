use crate::executor::ExecutorResult;
use crate::id::{Rifl, ShardId};
use crate::store::Value;
use crate::store::{Key, StorageOp, StorageOpResult, Store};
use crate::HashMap;
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Debug};
use std::iter::FromIterator;
use std::sync::Arc;

pub const DEFAULT_SHARD_ID: ShardId = 0;

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Command {
    rifl: Rifl,
    shard_to_ops: HashMap<ShardId, HashMap<Key, Arc<Vec<StorageOp>>>>,
    shard_to_n_deps_ops: HashMap<ShardId, HashMap<Key, Arc<Vec<Vec<usize>>>>>,
    // mapping from shard to the keys on that shard; this will be used by
    // `Tempo` to exchange `MStable` messages between shards
    shard_to_keys: Arc<HashMap<ShardId, Vec<Key>>>,
    // field used to output and empty iterator of keys when rustc can't figure
    // out what we mean
    _empty_keys: HashMap<Key, Arc<Vec<StorageOp>>>,
}

impl Command {
    /// Create a new `Command`.
    pub fn new(
        rifl: Rifl,
        shard_to_ops: HashMap<ShardId, HashMap<Key, Vec<StorageOp>>>,
    ) -> Self {
        let mut shard_to_keys: HashMap<ShardId, Vec<Key>> = Default::default();
        let new_shard_to_ops: HashMap<
            ShardId,
            HashMap<Key, Arc<Vec<StorageOp>>>,
        > = shard_to_ops
            .into_iter()
            .map(|(shard_id, shard_ops)| {
                (
                    shard_id,
                    shard_ops
                        .into_iter()
                        .map(|(key, ops)| {
                            // populate `shard_to_keys`
                            shard_to_keys
                                .entry(shard_id)
                                .or_default()
                                .push(key.clone());

                            // `Arc` the ops on this key
                            (key, Arc::new(ops))
                        })
                        .collect(),
                )
            })
            .collect();

        let shard_to_n_deps_ops: HashMap<
            ShardId,
            HashMap<Key, Arc<Vec<Vec<usize>>>>,
        > = new_shard_to_ops
            .iter()
            .map(|(&shard_id, shard_ops)| {
                (
                    shard_id,
                    shard_ops
                        .iter()
                        .map(|(key, _)| {
                            // populate `shard_to_keys`
                            shard_to_keys
                                .entry(shard_id)
                                .or_default()
                                .push(key.clone());

                            // `Arc` the ops on this key with an empty Vec
                            (key.clone(), Arc::new(vec![]))
                        })
                        .collect(),
                )
            })
            .collect();

        Self {
            rifl,
            shard_to_ops: new_shard_to_ops,
            shard_to_n_deps_ops,
            shard_to_keys: Arc::new(shard_to_keys),
            _empty_keys: HashMap::new(),
        }
    }

    // Create a new `Command` from an iterator.
    pub fn from<I: IntoIterator<Item = (Key, StorageOp)>>(
        rifl: Rifl,
        iter: I,
    ) -> Self {
        // store all keys in the default shard
        let inner = HashMap::from_iter(
            iter.into_iter().map(|(key, op)| (key, vec![op])),
        );
        let shard_to_ops =
            HashMap::from_iter(std::iter::once((DEFAULT_SHARD_ID, inner)));
        Self::new(rifl, shard_to_ops)
    }

    /// Checks if the NFR optimization can be applied.
    pub fn nfr_allowed(&self) -> bool {
        self.read_only() && self.total_key_count() == 1
    }

    /// Checks if the command is read-only.
    pub fn read_only(&self) -> bool {
        // a command is read-only if all ops are `Get`s
        self.shard_to_ops.values().all(|shard_ops| {
            shard_ops
                .values()
                .all(|ops| ops.iter().all(|op| op == &StorageOp::Get))
        })
    }

    /// Checks if the command is replicated by `shard_id`.
    pub fn replicated_by(&self, shard_id: &ShardId) -> bool {
        self.shard_to_ops.contains_key(&shard_id)
    }

    /// Returns the command identifier.
    pub fn rifl(&self) -> Rifl {
        self.rifl
    }

    /// Returns the number of keys accessed by this command on the shard
    /// provided.
    pub fn key_count(&self, shard_id: ShardId) -> usize {
        self.shard_to_ops
            .get(&shard_id)
            .map(|shard_ops| shard_ops.len())
            .unwrap_or(0)
    }

    /// Returns the total number of keys accessed by this command.
    pub fn total_key_count(&self) -> usize {
        self.shard_to_ops.values().map(|ops| ops.len()).sum()
    }

    /// Returns references to the keys accessed by this command on the shard
    /// provided.
    pub fn keys(&self, shard_id: ShardId) -> impl Iterator<Item = &Key> {
        self.shard_to_ops
            .get(&shard_id)
            .map(|shard_ops| shard_ops.keys())
            .unwrap_or_else(|| self._empty_keys.keys())
    }

    /// Returns references to the operations accessed by this command on the shard and key
    /// provided.
    pub fn operations(
        &self,
        shard_id: ShardId,
        key: &Key,
    ) -> impl Iterator<Item = &StorageOp> {
        self.shard_to_ops
            .get(&shard_id)
            .and_then(|shard_ops| shard_ops.get(key))
            .into_iter()
            .flat_map(|ops| ops.iter())
    }

    /// Returns references to all the keys accessed by this command.
    pub fn all_keys(&self) -> impl Iterator<Item = (&ShardId, &Key)> {
        self.shard_to_ops.iter().flat_map(|(shard_id, shard_ops)| {
            shard_ops.keys().map(move |key| (shard_id, key))
        })
    }

    /// Returns a mapping from shard identifier to the keys being accessed on
    /// that shard.
    pub fn shard_to_keys(&self) -> &Arc<HashMap<ShardId, Vec<Key>>> {
        &self.shard_to_keys
    }

    /// Returns the number of shards accessed by this command.
    pub fn shard_count(&self) -> usize {
        self.shard_to_ops.len()
    }

    /// Returns the shards accessed by this command.
    pub fn shards(&self) -> impl Iterator<Item = &ShardId> {
        self.shard_to_ops.keys()
    }

    /// Executes self in a `KVStore`, returning the resulting an iterator of
    /// `ExecutorResult`.
    pub fn execute<'a>(
        self,
        shard_id: ShardId,
        store: &'a mut Store,
    ) -> impl Iterator<Item = ExecutorResult> + 'a {
        let rifl = self.rifl;
        self.into_iter(shard_id).map(move |(key, ops)| {
            // take the ops inside the arc if we're the last with a
            // reference to it (otherwise, clone them)
            let ops =
                Arc::try_unwrap(ops).unwrap_or_else(|ops| ops.as_ref().clone());
            // execute this op
            let partial_results = store.execute(&key, ops, rifl);
            ExecutorResult::new(rifl, key, partial_results)
        })
    }

    // Creates an iterator with ops on keys that belong to `shard_id`.
    pub fn iter(
        &self,
        shard_id: ShardId,
    ) -> impl Iterator<Item = (&Key, &Arc<Vec<StorageOp>>)> {
        self.shard_to_ops
            .get(&shard_id)
            .map(|shard_ops| shard_ops.iter())
            .unwrap_or_else(|| self._empty_keys.iter())
    }

    // Creates an iterator with ops on keys that belong to `shard_id`.
    pub fn into_iter(
        mut self,
        shard_id: ShardId,
    ) -> impl Iterator<Item = (Key, Arc<Vec<StorageOp>>)> {
        self.shard_to_ops
            .remove(&shard_id)
            .map(|shard_ops| shard_ops.into_iter())
            .unwrap_or_else(|| self._empty_keys.into_iter())
    }

    /// Checks if a command conflicts with another given command.
    pub fn conflicts(&self, other: &Command) -> bool {
        self.shard_to_ops.iter().any(|(shard_id, shard_ops)| {
            shard_ops
                .iter()
                .any(|(key, _)| other.contains_key(*shard_id, key))
        })
    }

    /// Checks if `key` is accessed by this command.
    fn contains_key(&self, shard_id: ShardId, key: &Key) -> bool {
        self.shard_to_ops
            .get(&shard_id)
            .map(|shard_ops| shard_ops.contains_key(key))
            .unwrap_or(false)
    }

    /// Adds the operations in the `other` command to this command.
    pub fn merge(&mut self, other: Command) {
        for (shard_id, shard_ops) in other.shard_to_ops {
            let current_shard_ops =
                self.shard_to_ops.entry(shard_id).or_default();
            for (key, ops) in shard_ops {
                let ops = Arc::try_unwrap(ops).expect("a command to be merged into another command should have not been cloned");
                let current_ops = current_shard_ops.entry(key).or_default();
                Arc::get_mut(current_ops).expect("a command should only be cloned after all merges have occurred").extend(ops);
            }
        }
    }
}

impl fmt::Debug for Command {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let keys: std::collections::BTreeSet<_> = self
            .shard_to_ops
            .iter()
            .flat_map(|(shard_id, ops)| {
                ops.keys().map(move |key| (shard_id, key))
            })
            .collect();
        write!(f, "({:?} -> {:?})", self.rifl, keys)
    }
}

/// Structure that aggregates partial results of multi-key commands.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommandResultBuilder {
    rifl: Rifl,
    key_count: usize,
    results: HashMap<Key, Vec<StorageOpResult>>,
}

impl CommandResultBuilder {
    /// Creates a new `CommandResultBuilder` given the number of keys accessed
    /// by the command.
    pub fn new(rifl: Rifl, key_count: usize) -> Self {
        CommandResultBuilder {
            rifl,
            key_count,
            results: HashMap::new(),
        }
    }

    /// Adds a partial command result to the overall result.
    /// Returns a boolean indicating whether the full result is ready.
    pub fn add_partial(
        &mut self,
        key: Key,
        partial_results: Vec<StorageOpResult>,
    ) {
        // add op result for `key`
        let res = self.results.insert(key, partial_results);

        // assert there was nothing about this `key` previously
        assert!(res.is_none());
    }

    pub fn ready(&self) -> bool {
        // we're ready if the number of partial results equals `key_count`
        self.results.len() == self.key_count
    }
}

/// Structure that aggregates partial results of multi-key commands.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommandResult {
    rifl: Rifl,
    results: HashMap<Key, Vec<StorageOpResult>>,
}

impl CommandResult {
    /// Creates a new `CommandResult`.
    pub fn new(
        rifl: Rifl,
        results: HashMap<Key, Vec<StorageOpResult>>,
    ) -> Self {
        CommandResult { rifl, results }
    }

    /// Returns the command identifier.
    pub fn rifl(&self) -> Rifl {
        self.rifl
    }

    /// Returns the commands results.
    pub fn results(&self) -> &HashMap<Key, Vec<StorageOpResult>> {
        &self.results
    }
}

impl From<CommandResultBuilder> for CommandResult {
    fn from(cmd_result_builder: CommandResultBuilder) -> Self {
        assert!(cmd_result_builder.ready());
        Self {
            rifl: cmd_result_builder.rifl,
            results: cmd_result_builder.results,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn multi_put(rifl: Rifl, keys: Vec<String>) -> Command {
        let value = rand::thread_rng().gen_range(0..Value::MAX);
        Command::from(
            rifl,
            keys.into_iter()
                .map(|key| (key.clone(), StorageOp::Put(value))),
        )
    }

    #[test]
    fn conflicts() {
        let rifl = Rifl::new(1, 1);
        let cmd_a = multi_put(rifl, vec![String::from("A")]);
        let cmd_b = multi_put(rifl, vec![String::from("B")]);
        let cmd_c = multi_put(rifl, vec![String::from("C")]);
        let cmd_ab =
            multi_put(rifl, vec![String::from("A"), String::from("B")]);

        // check command a conflicts
        assert!(cmd_a.conflicts(&cmd_a));
        assert!(!cmd_a.conflicts(&cmd_b));
        assert!(!cmd_a.conflicts(&cmd_c));
        assert!(cmd_a.conflicts(&cmd_ab));

        // check command b conflicts
        assert!(!cmd_b.conflicts(&cmd_a));
        assert!(cmd_b.conflicts(&cmd_b));
        assert!(!cmd_b.conflicts(&cmd_c));
        assert!(cmd_b.conflicts(&cmd_ab));

        // check command c conflicts
        assert!(!cmd_c.conflicts(&cmd_a));
        assert!(!cmd_c.conflicts(&cmd_b));
        assert!(cmd_c.conflicts(&cmd_c));
        assert!(!cmd_c.conflicts(&cmd_ab));

        // check command ab conflicts
        assert!(cmd_ab.conflicts(&cmd_a));
        assert!(cmd_ab.conflicts(&cmd_b));
        assert!(!cmd_ab.conflicts(&cmd_c));
        assert!(cmd_ab.conflicts(&cmd_ab));
    }
}
