use crate::protocol::{DEFAULT_K_SUB_MRV, DEFAULT_N_MRV};

use super::{Dependency, LatestDep, LatestRWDep};
use fantoch::command::Command;
use fantoch::id::{Dot, ShardId};
use fantoch::store::{Key, StorageOp};
use fantoch::{HashMap, HashSet};
use rand::Rng;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LatestRWDepArray {
    data: Vec<LatestRWDep>,
    n: usize,
}

// Implemente Default para LatestRWDepArray
impl Default for LatestRWDepArray {
    fn default() -> Self {
        let mut data = Vec::new();

        for _ in 0..DEFAULT_N_MRV {
            data.push(LatestRWDep {
                read: None,
                write: None,
            })
        }

        LatestRWDepArray {
            data,
            n: DEFAULT_N_MRV,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MultiRecordValues {
    shard_id: ShardId,
    nfr: bool,
    n_mrv: usize,
    latest: HashMap<Key, LatestRWDepArray>,
    latest_noop: LatestDep,
}

pub type KeyDepsMRV = HashMap<Key, Vec<Vec<usize>>>;

impl MultiRecordValues {
    fn maybe_add_noop_latest(&self, deps: &mut HashSet<Dependency>) {
        if let Some(dep) = self.latest_noop.as_ref() {
            deps.insert(dep.clone());
        }
    }

    fn do_add_cmd(
        &mut self,
        dot: Dot,
        cmd: &Command,
        mut deps: HashSet<Dependency>,
        mut keys_deps: KeyDepsMRV,
    ) -> (HashSet<Dependency>, KeyDepsMRV) {
        // create cmd dep
        let cmd_dep = Dependency::from_cmd(dot, cmd);

        // flag indicating whether the command is read-only
        let read_only = cmd.read_only();
        // we only support single-key read commands with NFR
        assert!(if self.nfr && read_only {
            cmd.total_key_count() == 1
        } else {
            true
        });

        // iterate through all command keys, get their current latest and set
        // ourselves to be the new latest
        cmd.keys(self.shard_id).for_each(|key| {
            let operations = cmd.operations(self.shard_id, key);
            let n_key_dep: &mut Vec<Vec<usize>> =
                keys_deps.entry(key.clone()).or_insert_with(Vec::new);

            for (index, op) in operations.enumerate() {
                // get latest read and write on this key

                let op_n_deps = match n_key_dep.get(index) {
                    Some(value) => value.clone(),
                    None => match op {
                        StorageOp::Add(_) => {
                            let n =
                                rand::thread_rng().gen_range(0..DEFAULT_N_MRV);
                            let vec = vec![n];
                            n_key_dep.insert(index, vec.clone());

                            vec
                        }
                        StorageOp::Delete
                        | StorageOp::Get
                        | StorageOp::Put(_) => {
                            let mut vec = Vec::new();
                            for i in 0..DEFAULT_N_MRV {
                                vec.push(i);
                            }
                            n_key_dep.insert(index, vec.clone());
                            vec
                        }
                        StorageOp::Subtract(_) => {
                            let n =
                                rand::thread_rng().gen_range(0..DEFAULT_N_MRV);

                            let mut vec = Vec::new();
                            for i in n..DEFAULT_K_SUB_MRV {
                                vec.push((n + i) % DEFAULT_N_MRV);
                            }

                            n_key_dep.insert(index, vec.clone());

                            vec
                        }
                    },
                };

                for n in op_n_deps {
                    let latest_rw = match self.latest.get_mut(key) {
                        Some(vec) => {
                            if let Some(value) = vec.data.get_mut(n) {
                                value
                            } else {
                                panic!("Something went wrong");
                            }
                        }
                        None => {
                            &mut self
                                .latest
                                .entry(key.clone())
                                .or_default()
                                .data[n]
                        }
                    };

                    super::maybe_add_deps(
                        read_only, self.nfr, latest_rw, &mut deps,
                    );

                    // finally, store the command
                    if read_only {
                        // if a command is read-only, then added it as the latest read
                        latest_rw.read = Some(cmd_dep.clone());
                    } else {
                        // otherwise, add it as the latest write
                        latest_rw.write = Some(cmd_dep.clone());
                    }
                }
            }
        });

        // always include latest noop, if any
        self.maybe_add_noop_latest(&mut deps);

        // and finally return the computed deps
        (deps, keys_deps)
    }

    fn do_noop_deps(&self, deps: &mut HashSet<Dependency>) {
        // iterate through all keys, grab a read lock, and include their latest
        // in the final `deps`
        self.latest.values().for_each(|vec| {
            for latest_rw in &vec.data {
                if let Some(rdep) = latest_rw.read.as_ref() {
                    deps.insert(rdep.clone());
                }
                if let Some(wdep) = latest_rw.write.as_ref() {
                    deps.insert(wdep.clone());
                }
            }
        });
    }

    #[cfg(test)]
    fn do_cmd_deps(&self, cmd: &Command, deps: &mut HashSet<Dependency>) {
        // flag indicating whether the command is read-only
        let read_only = cmd.read_only();

        cmd.keys(self.shard_id).for_each(|key| {
            // get latest command on this key
            if let Some(vec) = self.latest.get(key) {
                for latest_rw in &vec.data {
                    super::maybe_add_deps(
                        read_only, self.nfr, &latest_rw, deps,
                    );
                }
            }
        });
    }

    fn do_add_noop(
        &mut self,
        dot: Dot,
        mut deps: HashSet<Dependency>,
    ) -> HashSet<Dependency> {
        // set self to be the new latest
        if let Some(dep) = self.latest_noop.replace(Dependency::from_noop(dot))
        {
            // if there was a previous latest, then it's a dependency
            deps.insert(dep);
        }

        // compute deps for this noop
        self.do_noop_deps(&mut deps);

        deps
    }

    /// Create a new `MultiRecordValuesKeyValues` instance.
    pub fn new(shard_id: ShardId, nfr: bool, n_mrv: usize) -> Self {
        Self {
            shard_id,
            nfr,
            latest: HashMap::new(),
            latest_noop: None,
            n_mrv,
        }
    }

    pub fn add_cmd(
        &mut self,
        dot: Dot,
        cmd: &Command,
        past: Option<HashSet<Dependency>>,
        keys_deps: Option<KeyDepsMRV>,
    ) -> (HashSet<Dependency>, KeyDepsMRV) {
        // we start with past in case there's one, or bottom otherwise
        let deps = match past {
            Some(past) => past,
            None => HashSet::new(),
        };
        match keys_deps {
            None => self.do_add_cmd(dot, cmd, deps, HashMap::new()),
            Some(value) => self.do_add_cmd(dot, cmd, deps, value),
        }
    }

    pub fn add_noop(&mut self, dot: Dot) -> HashSet<Dependency> {
        // start with an empty set of dependencies
        let deps = HashSet::new();
        self.do_add_noop(dot, deps)
    }

    #[cfg(test)]
    pub fn cmd_deps(&self, cmd: &Command) -> HashSet<Dot> {
        let mut deps = HashSet::new();
        self.maybe_add_noop_latest(&mut deps);
        self.do_cmd_deps(cmd, &mut deps);
        super::extract_dots(deps)
    }

    #[cfg(test)]
    pub fn noop_deps(&self) -> HashSet<Dot> {
        let mut deps = HashSet::new();
        self.maybe_add_noop_latest(&mut deps);
        self.do_noop_deps(&mut deps);
        super::extract_dots(deps)
    }

    pub fn parallel() -> bool {
        false
    }
}
