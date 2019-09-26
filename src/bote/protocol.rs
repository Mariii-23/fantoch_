pub enum Protocol {
    Atlas,
    FPaxos,
    EPaxos,
}

impl Protocol {
    pub fn quorum_size(&self, n: usize, f: usize) -> usize {
        match self {
            Protocol::Atlas => {
                let half = n / 2 as usize;
                half + f
            }
            Protocol::FPaxos => f + 1,
            Protocol::EPaxos => {
                // ignore the f passed as argument, and compute f to be a
                // minority of n processes
                let f = (n / 2) as usize;
                f + ((f + 1) / 2 as usize)
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn quorum_size() {
        assert_eq!(Protocol::Atlas.quorum_size(3, 1), 2);
        assert_eq!(Protocol::Atlas.quorum_size(5, 1), 3);
        assert_eq!(Protocol::Atlas.quorum_size(5, 2), 4);
        assert_eq!(Protocol::FPaxos.quorum_size(3, 1), 2);
        assert_eq!(Protocol::FPaxos.quorum_size(5, 1), 2);
        assert_eq!(Protocol::FPaxos.quorum_size(5, 2), 3);
        assert_eq!(Protocol::EPaxos.quorum_size(3, 0), 2);
        assert_eq!(Protocol::EPaxos.quorum_size(5, 0), 3);
        assert_eq!(Protocol::EPaxos.quorum_size(7, 0), 5);
        assert_eq!(Protocol::EPaxos.quorum_size(9, 0), 6);
        assert_eq!(Protocol::EPaxos.quorum_size(11, 0), 8);
        assert_eq!(Protocol::EPaxos.quorum_size(13, 0), 9);
    }
}
