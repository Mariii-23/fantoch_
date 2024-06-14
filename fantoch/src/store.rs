use std::vec;

use crate::id::Rifl;
use crate::HashMap;
use crate::{command::KeyDepsMRV, executor::ExecutionOrderMonitor};
use rand::Rng;
use serde::{Deserialize, Serialize};

// Definition of `Key` and `Value` types.
pub type Key = String;
pub type Value = u16;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum StorageOp {
    Get,
    Put(Value),
    Add(Value),
    Subtract(Value),
    Delete,
}

pub type StorageOpResult = Option<Value>;

#[derive(Default, Clone)]
pub struct Storage {
    store: HashMap<Key, Vec<Value>>,
    monitor: Option<ExecutionOrderMonitor>,
    is_kv_storage: bool,
    number: usize,
}

impl Storage {
    /// Creates a new `KVStore` instance.
    pub fn new(
        monitor_execution_order: bool,
        is_kv_storage: bool,
        n: Option<usize>,
    ) -> Self {
        let monitor = if monitor_execution_order {
            Some(ExecutionOrderMonitor::new())
        } else {
            None
        };

        Self {
            store: Default::default(),
            monitor,
            is_kv_storage,
            number: n.unwrap_or_else(|| 1),
        }
    }

    pub fn monitor(&self) -> Option<&ExecutionOrderMonitor> {
        self.monitor.as_ref()
    }

    /// Executes `StorageOp`s in the `KVStore`.
    #[cfg(test)]
    pub fn test_execute(
        &mut self,
        key: &Key,
        op: StorageOp,
    ) -> StorageOpResult {
        let mut results = self.do_execute(key, vec![op], &Vec::new());
        assert_eq!(results.len(), 1);
        results.pop().unwrap()
    }

    pub fn execute(
        &mut self,
        key: &Key,
        ops: Vec<StorageOp>,
        rifl: Rifl,
        n_deps: &Vec<Vec<usize>>,
    ) -> Vec<StorageOpResult> {
        // update monitor, if we're monitoring
        if let Some(monitor) = self.monitor.as_mut() {
            let read_only = ops.iter().all(|op| op == &StorageOp::Get);
            monitor.add(&key, read_only, rifl);
        }
        self.do_execute(&key, ops, n_deps)
    }

    pub fn get_n_deps_by_cmd(
        &self,
        key: Key,
        op: StorageOp,
    ) -> Option<Vec<usize>> {
        match op {
            StorageOp::Delete | StorageOp::Get | StorageOp::Put(_) => {
                let mut vec = Vec::new();
                for i in 0..self.number {
                    vec.push(i);
                }

                Some(vec)
            }
            StorageOp::Add(_) => {
                let n = rand::thread_rng().gen_range(0..self.number);
                let vec = vec![n];

                Some(vec)
            }
            StorageOp::Subtract(value) => {
                let n = rand::thread_rng().gen_range(0..self.number);
                let mut vec = vec![n];
                let mut value_consumed = 0;

                match self.store.get(&key) {
                    None => None,
                    Some(values) => {
                        for i in n..self.number {
                            if value_consumed >= value {
                                return Some(vec);
                            }

                            value_consumed += values[i];
                            vec.push(i);
                        }

                        for i in 0..n {
                            if value_consumed >= value {
                                return Some(vec);
                            }

                            value_consumed += values[i];
                            vec.push(i);
                        }

                        if value_consumed >= value {
                            return Some(vec);
                        }
                        None
                    }
                }
            }
        }
    }

    #[allow(clippy::ptr_arg)]
    fn do_execute(
        &mut self,
        key: &Key,
        ops: Vec<StorageOp>,
        n_deps: &Vec<Vec<usize>>,
    ) -> Vec<StorageOpResult> {
        ops.into_iter()
            .enumerate()
            .map(|(index, op)| {
                self.do_execute_op(
                    key,
                    op,
                    n_deps.get(index).unwrap_or(&vec![]).clone(),
                )
            })
            .collect()
    }

    fn do_execute_op(
        &mut self,
        key: &Key,
        op: StorageOp,
        n_deps: Vec<usize>,
    ) -> StorageOpResult {
        match op {
            StorageOp::Get => match self.store.get(key) {
                None => None,
                Some(values) => Some(values.iter().sum()),
            },
            StorageOp::Delete => match self.store.get(key) {
                None => None,
                Some(values) => {
                    let sum = values.iter().sum();
                    self.store.remove(key);
                    Some(sum)
                }
            },
            StorageOp::Put(value) => {
                if self.is_kv_storage {
                    self.store.insert(key.to_string(), vec![value]);
                    return Some(value);
                } else {
                    if !n_deps.is_empty() {
                        let index = n_deps[0];
                        let mut vec = vec![0; self.number];
                        vec[index] = value;

                        self.store.insert(key.to_string(), vec);
                        return Some(value);
                    } else {
                        let mut vec = vec![0; self.number];
                        vec[0] = value;

                        self.store.insert(key.to_string(), vec);
                        return Some(value);
                    }
                }
            }
            StorageOp::Add(value) => {
                let index = if self.is_kv_storage {
                    0
                } else {
                    if n_deps.is_empty() {
                        0
                    } else {
                        n_deps[0]
                    }
                };

                if let Some(old_value) = self.store.get_mut(key) {
                    // In case the sum overflows, we will put the maximum possible value
                    return match old_value[index].checked_add(value) {
                        Some(new_value) => {
                            old_value[index] = new_value;
                            Some(new_value)
                        }
                        None => {
                            let new_value = Value::MAX;
                            old_value[index] = new_value;
                            Some(new_value)
                        }
                    };
                } else {
                    let mut vec = vec![0; self.number];
                    vec[index] = value;

                    self.store.insert(key.to_string(), vec);
                    return Some(value);
                }
            }
            StorageOp::Subtract(value) => {
                if self.is_kv_storage {
                    // don't return the previous value
                    if let Some(old_value) = self.store.get_mut(key) {
                        // In case the subtraction overflows, we will put the minimum possible value
                        return match old_value[0].checked_sub(value) {
                            Some(new_value) => {
                                old_value[0] = new_value;
                                Some(new_value)
                            }
                            None => {
                                let new_value = Value::MIN;
                                old_value[0] = new_value;
                                Some(new_value)
                            }
                        };
                    }
                } else {
                    if let Some(old_vec) = self.store.get_mut(key) {
                        let sum: Value = n_deps
                            .iter()
                            .map(|&index| *old_vec.get(index).unwrap_or(&0))
                            .sum();

                        if sum < value {
                            return None;
                        }
                        let mut remaining_value = value;

                        for index in n_deps {
                            if let Some(entry) = old_vec.get_mut(index) {
                                if *entry <= remaining_value {
                                    remaining_value -= *entry;
                                    *entry = 0;
                                } else {
                                    *entry -= remaining_value;
                                    // remaining_value = 0;
                                    break;
                                }
                            }
                        }

                        return Some(value);
                    }
                }
                None
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use serde::de::value;

    use super::*;

    #[test]
    fn store_flow() {
        // key and values
        let key_a = String::from("A");
        let key_b = String::from("B");
        let x = 12;
        let y = 10;
        let z = 28;

        // store
        let monitor = false;
        let mut store = Storage::new(monitor, true, None);

        // get key_a    -> none
        assert_eq!(store.test_execute(&key_a, StorageOp::Get), None);
        // get key_b    -> none
        assert_eq!(store.test_execute(&key_b, StorageOp::Get), None);

        // put key_a x -> none
        assert_eq!(store.test_execute(&key_a, StorageOp::Put(x)), None);
        // get key_a    -> some(x)
        assert_eq!(store.test_execute(&key_a, StorageOp::Get), Some(x));

        // put key_b y -> none
        assert_eq!(store.test_execute(&key_b, StorageOp::Put(y)), None);
        // get key_b    -> some(y)
        assert_eq!(store.test_execute(&key_b, StorageOp::Get), Some(y));

        // put key_a z -> some(x)
        assert_eq!(
            store.test_execute(&key_a, StorageOp::Put(z)),
            None,
            /*
            the following is correct if Put returns the previous value
            Some(x.clone())
             */
        );
        // get key_a    -> some(z)
        assert_eq!(store.test_execute(&key_a, StorageOp::Get), Some(z));
        // get key_b    -> some(y)
        assert_eq!(store.test_execute(&key_b, StorageOp::Get), Some(y));

        // delete key_a -> some(z)
        assert_eq!(store.test_execute(&key_a, StorageOp::Delete), Some(z));
        // get key_a    -> none
        assert_eq!(store.test_execute(&key_a, StorageOp::Get), None);
        // get key_b    -> some(y)
        assert_eq!(store.test_execute(&key_b, StorageOp::Get), Some(y));

        // delete key_b -> some(y)
        assert_eq!(store.test_execute(&key_b, StorageOp::Delete), Some(y));
        // get key_b    -> none
        assert_eq!(store.test_execute(&key_b, StorageOp::Get), None);
        // get key_a    -> none
        assert_eq!(store.test_execute(&key_a, StorageOp::Get), None);

        // put key_a x -> none
        assert_eq!(store.test_execute(&key_a, StorageOp::Put(x)), None);
        // get key_a    -> some(x)
        assert_eq!(store.test_execute(&key_a, StorageOp::Get), Some(x));
        // get key_b    -> none
        assert_eq!(store.test_execute(&key_b, StorageOp::Get), None);

        // delete key_a -> some(x)
        assert_eq!(store.test_execute(&key_a, StorageOp::Delete), Some(x));
        // get key_a    -> none
        assert_eq!(store.test_execute(&key_a, StorageOp::Get), None);
    }

    #[test]
    fn add_flow() {
        // store
        let monitor = false;
        let mut store = Storage::new(monitor, true, None);

        let key_c = String::from("Add");
        let value_x = 12;
        let value_y = 10;

        // put key_c value_x -> 12
        assert_eq!(store.test_execute(&key_c, StorageOp::Put(value_x)), None);
        // add key_a value_y -> some(value_x + value_y)
        assert_eq!(
            store.test_execute(&key_c, StorageOp::Add(value_y)),
            Some(value_x + value_y)
        );

        // add key_a Maximum_value -> some(MAX)
        assert_eq!(
            store.test_execute(&key_c, StorageOp::Add(Value::MAX)),
            Some(Value::MAX)
        );
    }

    #[test]
    fn subtract_flow() {
        // store
        let monitor = false;
        let mut store = Storage::new(monitor, false, None);

        let key_c = String::from("Add");
        let value_x = 12;
        let value_y = 10;

        // put key_c value_x -> None
        assert_eq!(store.test_execute(&key_c, StorageOp::Put(value_x)), None);
        // subtract key_a value_y -> some(value_x - value_y)
        assert_eq!(
            store.test_execute(&key_c, StorageOp::Subtract(value_y)),
            Some(value_x - value_y)
        );

        // subtract key_a Maximum_Value -> some(MIM)
        assert_eq!(
            store.test_execute(&key_c, StorageOp::Subtract(Value::MAX)),
            Some(Value::MIN)
        );
    }

    #[test]
    fn add_and_subtract_flow() {
        // store
        let monitor = false;
        let mut store = Storage::new(monitor, true, None);

        let key_c = String::from("Add");
        let value_x = 12;
        let value_y = 10;

        // put key_c value_x -> 12
        assert_eq!(store.test_execute(&key_c, StorageOp::Put(value_x)), None);
        // add key_a value_y -> some(value_x + value_y)
        assert_eq!(
            store.test_execute(&key_c, StorageOp::Add(value_y)),
            Some(value_x + value_y)
        );

        // subtract key_a value_x -> some(value_y)
        assert_eq!(
            store.test_execute(&key_c, StorageOp::Subtract(value_x)),
            Some(value_y)
        );

        // add key_a Maximum_value -> some(MAX)
        assert_eq!(
            store.test_execute(&key_c, StorageOp::Add(Value::MAX)),
            Some(Value::MAX)
        );

        // subtract key_a value_x -> some(MAX - value_x)
        assert_eq!(
            store.test_execute(&key_c, StorageOp::Subtract(value_x)),
            Some(Value::MAX - value_x)
        );

        // subtract key_a Maximum_Value -> some(MIM)
        assert_eq!(
            store.test_execute(&key_c, StorageOp::Subtract(Value::MAX)),
            Some(Value::MIN)
        );
    }
}
