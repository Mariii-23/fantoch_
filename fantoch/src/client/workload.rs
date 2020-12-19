use crate::client::key_gen::{KeyGen, KeyGenState};
use crate::command::Command;
use crate::id::{RiflGen, ShardId};
use crate::kvs::{KVOp, Key, Value};
use crate::trace;
use crate::HashMap;
use rand::{distributions::Alphanumeric, Rng};
use serde::{Deserialize, Serialize};
use std::iter;

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct Workload {
    /// number of shards
    shard_count: u64,
    // key generator
    key_gen: KeyGen,
    /// number of keys accessed by the command
    keys_per_command: usize,
    /// number of commands to be submitted in this workload
    commands_per_client: usize,
    /// percentage of read-only commands
    read_only_percentage: usize,
    /// size of payload in command (in bytes)
    payload_size: usize,
    /// number of commands already issued in this workload
    command_count: usize,
}

impl Workload {
    pub fn new(
        shard_count: usize,
        key_gen: KeyGen,
        keys_per_command: usize,
        commands_per_client: usize,
        payload_size: usize,
    ) -> Self {
        // check for valid workloads
        if let KeyGen::ConflictPool {
            pool_size,
            conflict_rate,
        } = key_gen
        {
            assert!(
                conflict_rate <= 100,
                "the conflict rate must be less or equal to 100"
            );
            assert!(pool_size >= 1, "the pool size should be at least 1");
            if conflict_rate == 100 && keys_per_command > 1 {
                panic!("invalid workload; can't generate more than one key when the conflict_rate is 100");
            }
            if keys_per_command > 2 {
                panic!("invalid workload; can't generate more than two keys with the conflict_rate key generator");
            }
        }
        // by default, the read-only percentage is 0
        let read_only_percentage = 0;
        Self {
            shard_count: shard_count as u64,
            keys_per_command,
            key_gen,
            commands_per_client,
            read_only_percentage,
            payload_size,
            command_count: 0,
        }
    }

    /// Returns the number of shards in the system.
    pub fn shard_count(&self) -> usize {
        self.shard_count as usize
    }

    /// Returns the key generator.
    pub fn key_gen(&self) -> KeyGen {
        self.key_gen
    }

    /// Returns the total number of commands to be generated by this workload.
    pub fn commands_per_client(&self) -> usize {
        self.commands_per_client
    }

    /// Returns the number of keys accessed by commands generated by this
    /// workload.
    pub fn keys_per_command(&self) -> usize {
        self.keys_per_command
    }

    /// Returns the percentage of read-only commands to be generated by this
    /// workload.
    pub fn read_only_percentage(&self) -> usize {
        self.read_only_percentage
    }

    /// Sets the percentage of read-only commands to be generated by this
    /// workload.
    pub fn set_read_only_percentage(&mut self, read_only_percentage: usize) {
        assert!(
            read_only_percentage <= 100,
            "the percentage of read-only commands must be less or equal to 100"
        );
        self.read_only_percentage = read_only_percentage;
    }

    /// Returns the payload size of the commands to be generated by this
    /// workload.
    pub fn payload_size(&self) -> usize {
        self.payload_size
    }

    /// Generate the next command.
    pub fn next_cmd(
        &mut self,
        rifl_gen: &mut RiflGen,
        key_gen_state: &mut KeyGenState,
    ) -> Option<(ShardId, Command)> {
        // check if we should generate more commands
        if self.command_count < self.commands_per_client {
            // increment command count
            self.command_count += 1;
            // generate new command
            Some(self.gen_cmd(rifl_gen, key_gen_state))
        } else {
            trace!("c{:?}: done!", rifl_gen.source());
            None
        }
    }

    /// Returns the number of commands already issued.
    pub fn issued_commands(&self) -> usize {
        self.command_count
    }

    /// Returns a boolean indicating whether the workload has finished, i.e. all
    /// commands have been issued.
    pub fn finished(&self) -> bool {
        self.command_count == self.commands_per_client
    }

    /// Generate a command.
    fn gen_cmd(
        &mut self,
        rifl_gen: &mut RiflGen,
        key_gen_state: &mut KeyGenState,
    ) -> (ShardId, Command) {
        // generate rifl
        let rifl = rifl_gen.next_id();

        // generate all the key-value pairs
        let mut ops: HashMap<_, HashMap<_, _>> = HashMap::new();

        // generate unique keys:
        // - since we store them in Vec, this ensures that the target shard will
        // be the shard of the first key generated
        let keys = self.gen_unique_keys(key_gen_state);
        // check if the command should be read-only
        let read_only = super::key_gen::true_if_random_is_less_than(
            self.read_only_percentage,
        );
        let mut target_shard = None;

        for key in keys {
            // compute op
            let op = if read_only {
                // if read-only, the op is a `Get`
                KVOp::Get
            } else {
                // if not read-only, the op is a `Put`:
                // - generate payload for `Put` op
                let value = self.gen_cmd_value();
                KVOp::Put(value)
            };
            // compute key's shard and save op
            let shard_id = self.shard_id(&key);
            ops.entry(shard_id).or_default().insert(key, op);

            // target shard is the shard of the first key generated
            target_shard = target_shard.or(Some(shard_id));
        }
        let target_shard =
            target_shard.expect("there should be a target shard");

        // create command
        (target_shard, Command::new(rifl, ops))
    }

    fn gen_unique_keys(&self, key_gen_state: &mut KeyGenState) -> Vec<Key> {
        let mut keys = Vec::with_capacity(self.keys_per_command);
        while keys.len() != self.keys_per_command {
            let key = key_gen_state.gen_cmd_key();
            if !keys.contains(&key) {
                keys.push(key);
            }
        }
        keys
    }

    /// Generate a command payload with the payload size provided.
    fn gen_cmd_value(&self) -> Value {
        let mut rng = rand::thread_rng();
        iter::repeat(())
            .map(|_| rng.sample(Alphanumeric))
            .take(self.payload_size)
            .collect()
    }

    /// Computes which shard the key belongs to.
    fn shard_id(&self, key: &Key) -> ShardId {
        crate::util::key_hash(key) % self.shard_count
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kvs::KVOp;

    const POOL_SIZE: usize = 1;
    // since the pool size is 1, the conflict color must be the following
    const CONFLICT_COLOR: &str = "CONFLICT0";

    #[test]
    fn gen_cmd_key() {
        // create rilf gen
        let client_id = 1;
        let mut rifl_gen = RiflGen::new(client_id);

        // general config
        let shard_count = 1;
        let keys_per_command = 1;
        let commands_per_client = 100;
        let payload_size = 100;

        // create conflicting workload
        let key_gen = KeyGen::ConflictPool {
            conflict_rate: 100,
            pool_size: POOL_SIZE,
        };
        let mut workload = Workload::new(
            shard_count,
            key_gen,
            keys_per_command,
            commands_per_client,
            payload_size,
        );
        let mut key_gen_state =
            key_gen.initial_state(workload.shard_count(), client_id);
        let (target_shard, command) =
            workload.gen_cmd(&mut rifl_gen, &mut key_gen_state);
        assert_eq!(target_shard, 0);
        assert_eq!(
            command.keys(target_shard).collect::<Vec<_>>(),
            vec![CONFLICT_COLOR]
        );

        // create non-conflicting workload
        let key_gen = KeyGen::ConflictPool {
            conflict_rate: 0,
            pool_size: POOL_SIZE,
        };
        let mut workload = Workload::new(
            shard_count,
            key_gen,
            keys_per_command,
            commands_per_client,
            payload_size,
        );
        let mut key_gen_state =
            key_gen.initial_state(workload.shard_count(), client_id);
        let (target_shard, command) =
            workload.gen_cmd(&mut rifl_gen, &mut key_gen_state);
        assert_eq!(target_shard, 0);
        assert_eq!(command.keys(target_shard).collect::<Vec<_>>(), vec!["1"]);
    }

    #[test]
    fn next_cmd() {
        // create rilf gen
        let client_id = 1;
        let mut rifl_gen = RiflGen::new(client_id);

        // general config
        let shard_count = 1;
        let keys_per_command = 1;
        let commands_per_client = 10000;
        let payload_size = 10;

        // create workload
        let key_gen = KeyGen::ConflictPool {
            conflict_rate: 100,
            pool_size: POOL_SIZE,
        };
        let mut workload = Workload::new(
            shard_count,
            key_gen,
            keys_per_command,
            commands_per_client,
            payload_size,
        );
        let mut key_gen_state =
            key_gen.initial_state(workload.shard_count(), client_id);

        // check total and issued commands
        assert_eq!(workload.commands_per_client(), commands_per_client);
        assert_eq!(workload.issued_commands(), 0);

        // the first `total_commands` commands are `Some`
        for i in 1..=commands_per_client {
            if let Some((target_shard, cmd)) =
                workload.next_cmd(&mut rifl_gen, &mut key_gen_state)
            {
                // since there's a single shard, keys should be on shard 0
                assert_eq!(target_shard, 0);
                let (key, value) = cmd.into_iter(target_shard).next().unwrap();
                // since the conflict is 100, the key should be `CONFLICT_COLOR`
                assert_eq!(key, CONFLICT_COLOR);
                // check that the value size is `payload_size`
                if let KVOp::Put(payload) = value {
                    assert_eq!(payload.len(), payload_size);
                } else {
                    panic!("workload should generate PUT commands");
                }

                // check total and issued commands
                assert_eq!(workload.commands_per_client(), commands_per_client);
                assert_eq!(workload.issued_commands(), i);
            } else {
                panic!("there should be a next command in this workload");
            }
        }

        // check the workload is finished
        assert!(workload.finished());

        // after this, no more commands are generated
        for _ in 1..=10 {
            assert!(workload
                .next_cmd(&mut rifl_gen, &mut key_gen_state)
                .is_none());
        }

        // check the workload is still finished
        assert!(workload.finished());
    }

    #[test]
    fn conflict_rate() {
        for conflict_rate in vec![1, 2, 10, 50] {
            // create rilf gen
            let client_id = 1;
            let mut rifl_gen = RiflGen::new(client_id);

            // total commands
            let shard_count = 1;
            let keys_per_command = 1;
            let commands_per_client = 1000000;
            let payload_size = 0;

            // create workload
            let key_gen = KeyGen::ConflictPool {
                conflict_rate,
                pool_size: POOL_SIZE,
            };
            let mut workload = Workload::new(
                shard_count,
                key_gen,
                keys_per_command,
                commands_per_client,
                payload_size,
            );
            let mut key_gen_state =
                key_gen.initial_state(workload.shard_count(), client_id);

            // count conflicting commands
            let mut conflict_color_count = 0;

            while let Some((target_shard, cmd)) =
                workload.next_cmd(&mut rifl_gen, &mut key_gen_state)
            {
                // since there's a single shard, keys should be on shard 0
                assert_eq!(target_shard, 0);
                // get command key and check if it's conflicting
                let (key, _) = cmd.into_iter(target_shard).next().unwrap();
                if key == CONFLICT_COLOR {
                    conflict_color_count += 1;
                }
            }

            // compute percentage of conflicting commands
            let percentage = (conflict_color_count * 100) as f64
                / commands_per_client as f64;
            assert_eq!(percentage.round() as usize, conflict_rate);
        }
    }

    #[test]
    fn two_shards() {
        // in order for this test to pass, `check_two_shards` should generate a
        // command that accesses two shards
        std::iter::repeat(()).any(|_| check_two_shards());
    }

    fn check_two_shards() -> bool {
        // create rilf gen
        let client_id = 1;
        let mut rifl_gen = RiflGen::new(client_id);

        // general config
        let shard_count = 2;
        let keys_per_command = 2;
        let commands_per_client = 1;
        let payload_size = 0;

        // create workload
        let key_gen = KeyGen::Zipf {
            coefficient: 0.1,
            total_keys_per_shard: 1_000_000,
        };
        let mut workload = Workload::new(
            shard_count,
            key_gen,
            keys_per_command,
            commands_per_client,
            payload_size,
        );
        let mut key_gen_state =
            key_gen.initial_state(workload.shard_count(), client_id);

        let (target_shard, cmd) = workload
            .next_cmd(&mut rifl_gen, &mut key_gen_state)
            .expect("there should be at least one command");

        assert!(
            target_shard == 0 || target_shard == 1,
            "target shard should be either 0 or 1"
        );

        assert!(
            cmd.key_count(0) + cmd.key_count(1) == keys_per_command,
            "the number of keys accessed by the command should be 2"
        );

        // we want an execution in which the two shards are accessed, i.e.:
        // - 1 key in shard 0
        // - 1 key in shard 1
        cmd.key_count(0) == 1 && cmd.key_count(1) == 1
    }
}
