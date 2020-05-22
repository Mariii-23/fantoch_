// This module contains the definition of `Pending`.
mod pending;

// This module contains the implementation of a basic executor that executes
// operations as soon as it receives them.
mod basic;

// Re-exports.
pub use basic::{BasicExecutionInfo, BasicExecutor};
pub use pending::Pending;

use crate::command::{Command, CommandResult};
use crate::config::Config;
use crate::id::{ClientId, Rifl};
use crate::kvs::{KVOpResult, Key};
use crate::metrics::Metrics;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::fmt::{self, Debug};

pub trait Executor {
    // TODO why is Send needed?
    type ExecutionInfo: Debug
        + Clone
        + Serialize
        + DeserializeOwned
        + Send
        + Sync
        + MessageKey; // TODO why is Sync needed??

    fn new(config: Config) -> Self;

    fn wait_for(&mut self, cmd: &Command);

    // Parallel executors may receive several waits for the same `Rifl`.
    fn wait_for_rifl(&mut self, rifl: Rifl);

    #[must_use]
    fn handle(&mut self, infos: Self::ExecutionInfo) -> Vec<ExecutorResult>;

    fn parallel() -> bool;

    fn metrics(&self) -> &ExecutorMetrics {
        todo!()
    }
}

pub type ExecutorMetrics = Metrics<ExecutorMetricsKind, u64>;

pub enum ExecutorMetricsKind {
    ExecutionDelay,
}

impl Debug for ExecutorMetricsKind {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ExecutorMetricsKind::ExecutionDelay => write!(f, "execution_delay"),
        }
    }
}

pub trait MessageKey {
    /// If `None` is returned, then the message is sent the *single* executor
    /// process. If there's more than one executor, and this function
    /// returns `None`, the runtime will panic.
    fn key(&self) -> Option<&Key> {
        None
    }
}

#[derive(Debug)]
pub enum ExecutorResult {
    /// this contains a complete command result
    Ready(CommandResult),
    /// this contains a partial command result
    Partial(Rifl, Key, KVOpResult),
}

impl ExecutorResult {
    /// Check which client should receive this result.
    pub fn client(&self) -> ClientId {
        match self {
            ExecutorResult::Ready(cmd_result) => cmd_result.rifl().source(),
            ExecutorResult::Partial(rifl, _, _) => rifl.source(),
        }
    }

    /// Extracts a ready results from self. Panics if not ready.
    pub fn unwrap_ready(self) -> CommandResult {
        match self {
            ExecutorResult::Ready(cmd_result) => cmd_result,
            ExecutorResult::Partial(_, _, _) => panic!(
                "called `ExecutorResult::unwrap_ready()` on a `ExecutorResult::Partial` value"
            ),
        }
    }
    /// Extracts a partial result from self. Panics if not partial.
    pub fn unwrap_partial(self) -> (Rifl, Key, KVOpResult) {
        match self {
            ExecutorResult::Partial(rifl, key, result) => (rifl, key, result),
            ExecutorResult::Ready(_) => panic!(
                "called `ExecutorResult::unwrap_partial()` on a `ExecutorResult::Ready` value"
            ),
        }
    }
}