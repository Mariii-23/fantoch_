// This module contains the definition of `KeyDeps`.
mod keys;

// // This module contains the definition of `QuorumClocks`.
mod quorum;

// Re-exports.
pub use keys::{Dependency, KeyDeps, LockedKeyDeps, SequentialKeyDeps, MultiRecordValues, Key_Deps_MRV};
pub use quorum::QuorumDeps;
