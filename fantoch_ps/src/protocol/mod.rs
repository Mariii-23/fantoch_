// This module contains common data-structures between protocols.
pub mod common;

// This module contains the definition of `Atlas`.
mod atlas;

// This module contains the definition of `EPaxos`.
mod epaxos;

// This module contains the definition of `Newt`.
mod newt;

// This module contains the definition of `FPaxos`.
mod fpaxos;

// // This module contains the definition of `Caesar`.
// mod caesar;

// Re-exports.
pub use atlas::{AtlasLocked, AtlasSequential};
pub use epaxos::{EPaxosLocked, EPaxosSequential};
pub use fpaxos::FPaxos;
pub use newt::{NewtAtomic, NewtSequential};
// pub use caesar::Caesar;

#[cfg(test)]
mod tests {
    use super::*;
    use fantoch::client::Workload;
    use fantoch::config::Config;
    use fantoch::planet::Planet;
    use fantoch::protocol::{Protocol, ProtocolMetricsKind};
    use fantoch::run::tests::run_test;
    use fantoch::sim::Runner;

    #[test]
    fn sim_newt_3_1_test() {
        let slow_paths = sim_test::<NewtSequential>(3, 1, false);
        assert_eq!(slow_paths, 0);
    }

    #[test]
    fn sim_newt_5_1_test() {
        let slow_paths = sim_test::<NewtSequential>(5, 1, false);
        assert_eq!(slow_paths, 0);
    }

    // TODO ignore since the slow path is not yet implemented
    #[ignore]
    #[test]
    fn sim_newt_5_2_test() {
        let slow_paths = sim_test::<NewtSequential>(5, 2, false);
        assert!(slow_paths > 0);
    }

    #[test]
    fn sim_atlas_3_1_test() {
        let slow_paths = sim_test::<AtlasSequential>(3, 1, false);
        assert_eq!(slow_paths, 0);
    }

    #[test]
    fn sim_atlas_5_1_test() {
        let slow_paths = sim_test::<AtlasSequential>(3, 1, false);
        assert_eq!(slow_paths, 0);
    }

    #[test]
    fn sim_atlas_5_2_test() {
        let slow_paths = sim_test::<AtlasSequential>(5, 2, false);
        assert!(slow_paths > 0);
    }

    #[test]
    fn sim_epaxos_3_1_test() {
        let slow_paths = sim_test::<EPaxosSequential>(3, 1, false);
        assert_eq!(slow_paths, 0);
    }

    #[test]
    fn sim_epaxos_5_2_test() {
        let slow_paths = sim_test::<EPaxosSequential>(5, 2, false);
        assert!(slow_paths > 0);
    }

    #[test]
    fn sim_fpaxos_3_1_test() {
        sim_test::<FPaxos>(3, 1, true);
    }

    #[test]
    fn sim_fpaxos_5_2_test() {
        sim_test::<FPaxos>(5, 2, true);
    }

    #[tokio::test]
    async fn run_newt_sequential_test() {
        // newt sequential can only handle one worker but many executors
        let workers = 1;
        let executors = 2;
        let with_leader = false;
        run_test::<NewtSequential>(workers, executors, with_leader).await
    }

    #[tokio::test]
    async fn run_newt_atomic_test() {
        // newt atomic can handle as many workers as we want but we may want to
        // only have one executor
        let workers = 3;
        let executors = 1;
        let with_leader = false;
        run_test::<NewtAtomic>(workers, executors, with_leader).await
    }

    #[tokio::test]
    async fn run_atlas_sequential_test() {
        // atlas sequential can only handle one worker and one executor
        let workers = 1;
        let executors = 1;
        let with_leader = false;
        run_test::<AtlasSequential>(workers, executors, with_leader).await
    }

    #[tokio::test]
    async fn run_atlas_locked_test() {
        // atlas locked can handle as many workers as we want but only one
        // executor
        let workers = 3;
        let executors = 1;
        let with_leader = false;
        run_test::<AtlasLocked>(workers, executors, with_leader).await
    }

    #[tokio::test]
    async fn run_epaxos_sequential_test() {
        // epaxos sequential can only handle one worker and one executor
        let workers = 1;
        let executors = 1;
        let with_leader = false;
        run_test::<EPaxosSequential>(workers, executors, with_leader).await
    }

    #[tokio::test]
    async fn run_epaxos_locked_test() {
        // epaxos locked can handle as many workers as we want but only one
        // executor
        let workers = 3;
        let executors = 1;
        let with_leader = false;
        run_test::<EPaxosLocked>(workers, executors, with_leader).await
    }

    #[tokio::test]
    async fn run_fpaxos_sequential_test() {
        // run fpaxos in sequential mode
        let workers = 1;
        let executors = 1;
        let with_leader = true;
        run_test::<FPaxos>(workers, executors, with_leader).await
    }

    #[tokio::test]
    async fn run_fpaxos_parallel_test() {
        // run fpaxos in paralel mode (in terms of workers, since execution is
        // never parallel)
        let workers = 3;
        let executors = 1;
        let with_leader = true;
        run_test::<FPaxos>(workers, executors, with_leader).await
    }

    fn sim_test<P: Protocol>(n: usize, f: usize, with_leader: bool) -> u64 {
        // planet
        let planet = Planet::new();

        // config
        let mut config = Config::new(n, f);

        // set process 0 as leader if we need one
        if with_leader {
            config.set_leader(1);
        }

        // clients workload
        let conflict_rate = 100;
        let commands_per_client = 100;
        let payload_size = 100;
        let workload =
            Workload::new(conflict_rate, commands_per_client, payload_size);
        let clients_per_region = 10;

        // process and client regions
        let mut regions = planet.regions();
        regions.truncate(n);
        let process_regions = regions.clone();
        let client_regions = regions.clone();

        // create runner
        let mut runner: Runner<P> = Runner::new(
            planet,
            config,
            workload,
            clients_per_region,
            process_regions,
            client_regions,
        );

        // run simulation until the clients end + another 2 seconds (2000ms)
        let (processes_metrics, _) = runner.run(Some(2000));

        // slow path count
        let mut total_slow_paths = 0;

        // check process stats
        let gced = processes_metrics
            .values()
            .into_iter()
            .filter(|metrics| {
                // get slow paths
                let slow_paths = metrics
                    .get_aggregated(ProtocolMetricsKind::SlowPath)
                    .cloned()
                    .unwrap_or_default();
                total_slow_paths += slow_paths;

                // check stability has run
                let stable_count = metrics
                    .get_aggregated(ProtocolMetricsKind::Stable)
                    .expect("stability should have happened");

                // check if this process gc-ed all commands
                *stable_count
                    == (commands_per_client * clients_per_region * n) as u64
            })
            .count();

        // check if the correct number of processed gc-ed:
        // - if there's a leader (i.e. FPaxos), GC will only prune commands at
        //   f+1 acceptors
        // - otherwise, GC will prune comands at all processes
        let expected = if with_leader { f + 1 } else { n };
        assert_eq!(gced, expected);

        // return number of slow paths
        total_slow_paths
    }
}