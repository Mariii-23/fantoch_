use crate::base::ProcId;
use crate::command::MultiCommand;
use crate::store::Key;
use std::collections::btree_map::{self, BTreeMap};
use std::fmt;

/// `ProcVotes` are the Votes by some process on some command.
pub type ProcVotes = BTreeMap<Key, VoteRange>;

/// Votes are all Votes on some command.
#[derive(Debug, Clone, PartialEq)]
pub struct Votes {
    votes: BTreeMap<Key, Vec<VoteRange>>,
}

impl Votes {
    /// Creates an uninitialized `Votes` instance.
    pub fn uninit() -> Self {
        Votes {
            votes: BTreeMap::new(),
        }
    }

    /// Creates an initialized `Votes` instance.
    pub fn from(cmd: &MultiCommand) -> Self {
        // create empty votes
        let votes = cmd
            .keys()
            .into_iter()
            // map each key tuple (key, empty_votes)
            .map(|key| (key.clone(), vec![]))
            .collect();

        // return new `Votes`
        Votes { votes }
    }

    /// Add `ProcVotes` to `Votes`.
    pub fn add(&mut self, proc_votes: ProcVotes) {
        // create proc_votes iterator
        let mut proc_votes = proc_votes.into_iter();

        // while we iterate self
        for (key, key_votes) in self.votes.iter_mut() {
            // the next in proc_votes must be about the same key
            let (vote_key, vote) = proc_votes.next().unwrap();
            assert_eq!(*key, vote_key);

            // add vote to this key's votes
            key_votes.push(vote);
        }

        // check there's nothing else in the proc votes iterator
        assert!(proc_votes.next().is_none());
    }
}

impl IntoIterator for Votes {
    type Item = (Key, Vec<VoteRange>);
    type IntoIter = btree_map::IntoIter<Key, Vec<VoteRange>>;

    /// Returns a `Votes` into-iterator ordered by `Key` (ASC).
    fn into_iter(self) -> Self::IntoIter {
        self.votes.into_iter()
    }
}

// `VoteRange` encodes a set of votes performed by some processed:
// - this will be used to fill the `VotesTable`
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct VoteRange {
    by: ProcId,
    start: u64,
    end: u64,
}

impl VoteRange {
    /// Create a new `VoteRange` instance.
    pub fn new(by: ProcId, start: u64, end: u64) -> Self {
        assert!(start <= end);
        VoteRange { by, start, end }
    }

    /// Get which process voted.
    pub fn voter(&self) -> ProcId {
        self.by
    }

    /// Get all votes in this range.
    pub fn votes(&self) -> Vec<u64> {
        (self.start..=self.end).collect()
    }
}

impl fmt::Debug for VoteRange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.start == self.end {
            write!(f, "<{}, {}>", self.by, self.start)
        } else {
            write!(f, "<{}, {}-{}>", self.by, self.start, self.end)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::newt::clocks::Clocks;
    use std::cmp::max;

    #[test]
    fn votes_flow() {
        // create clocks
        let mut clocks_p0 = Clocks::new(0);
        let mut clocks_p1 = Clocks::new(1);

        // keys
        let key_a = String::from("A");
        let key_b = String::from("B");

        // command a
        let cmd_a_id = (100, 1); // client 100, 1st op
        let cmd_a = MultiCommand::get(cmd_a_id, key_a.clone());
        let mut votes_a = Votes::from(&cmd_a);

        // command b
        let cmd_ab_id = (101, 1); // client 101, 1st op
        let cmd_ab = MultiCommand::multi_get(
            cmd_ab_id,
            vec![key_a.clone(), key_b.clone()],
        );
        let mut votes_ab = Votes::from(&cmd_ab);

        // orders on each process:
        // - p0: Submit(a),  MCommit(a),  MCollect(ab)
        // - p1: Submit(ab), MCollect(a), MCommit(ab)

        // -------------------------
        // submit command a by p0
        let clock_a = clocks_p0.clock(&cmd_a) + 1;
        assert_eq!(clock_a, 1);

        // -------------------------
        // (local) MCollect handle by p0 (command a)
        let clock_a_p0 = max(clock_a, clocks_p0.clock(&cmd_a) + 1);
        let proc_votes_a_p0 = clocks_p0.proc_votes(&cmd_a, clock_a_p0);
        clocks_p0.bump_to(&cmd_a, clock_a_p0);

        // -------------------------
        // submit command ab by p1
        let clock_ab = clocks_p1.clock(&cmd_ab) + 1;
        assert_eq!(clock_ab, 1);

        // -------------------------
        // (local) MCollect handle by p1 (command ab)
        let clock_ab_p1 = max(clock_ab, clocks_p1.clock(&cmd_ab) + 1);
        let proc_votes_ab_p1 = clocks_p1.proc_votes(&cmd_ab, clock_ab_p1);
        clocks_p1.bump_to(&cmd_ab, clock_ab_p1);

        // -------------------------
        // (remote) MCollect handle by p1 (command a)
        let clock_a_p1 = max(clock_a, clocks_p1.clock(&cmd_a) + 1);
        let proc_votes_a_p1 = clocks_p1.proc_votes(&cmd_a, clock_a_p1);
        clocks_p1.bump_to(&cmd_a, clock_a_p1);

        // -------------------------
        // (remote) MCollect handle by p0 (command ab)
        let clock_ab_p0 = max(clock_ab, clocks_p0.clock(&cmd_ab) + 1);
        let proc_votes_ab_p0 = clocks_p0.proc_votes(&cmd_ab, clock_ab_p0);
        clocks_p0.bump_to(&cmd_ab, clock_ab_p0);

        // -------------------------
        // MCollectAck handles by p0 (command a)
        votes_a.add(proc_votes_a_p0);
        votes_a.add(proc_votes_a_p1);

        // there's a single key
        assert_eq!(votes_a.votes.len(), 1);

        // there are two voters
        let key_votes = votes_a.votes.get(&key_a).unwrap();
        assert_eq!(key_votes.len(), 2);

        // p0 voted with 1
        let mut key_votes = key_votes.into_iter();
        let key_votes_by_p0 = key_votes.next().unwrap();
        assert_eq!(key_votes_by_p0.voter(), 0);
        assert_eq!(key_votes_by_p0.votes(), vec![1]);

        // p1 voted with 2
        let key_votes_by_p1 = key_votes.next().unwrap();
        assert_eq!(key_votes_by_p1.voter(), 1);
        assert_eq!(key_votes_by_p1.votes(), vec![2]);

        // -------------------------
        // MCollectAck handles by p1 (command ab)
        votes_ab.add(proc_votes_ab_p1);
        votes_ab.add(proc_votes_ab_p0);

        // there are two keys
        assert_eq!(votes_ab.votes.len(), 2);

        // key a:
        // there are two voters
        let key_votes = votes_ab.votes.get(&key_a).unwrap();
        assert_eq!(key_votes.len(), 2);

        // p1 voted with 1
        let mut key_votes = key_votes.into_iter();
        let key_votes_by_p1 = key_votes.next().unwrap();
        assert_eq!(key_votes_by_p1.voter(), 1);
        assert_eq!(key_votes_by_p1.votes(), vec![1]);

        // p0 voted with 2
        let key_votes_by_p0 = key_votes.next().unwrap();
        assert_eq!(key_votes_by_p0.voter(), 0);
        assert_eq!(key_votes_by_p0.votes(), vec![2]);

        // key b:
        // there are two voters
        let key_votes = votes_ab.votes.get(&key_b).unwrap();
        assert_eq!(key_votes.len(), 2);

        // p1 voted with 1
        let mut key_votes = key_votes.into_iter();
        let key_votes_by_p1 = key_votes.next().unwrap();
        assert_eq!(key_votes_by_p1.voter(), 1);
        assert_eq!(key_votes_by_p1.votes(), vec![1]);

        // p0 voted with 1 and 2
        let key_votes_by_p0 = key_votes.next().unwrap();
        assert_eq!(key_votes_by_p0.voter(), 0);
        assert_eq!(key_votes_by_p0.votes(), vec![1, 2]);
    }
}
