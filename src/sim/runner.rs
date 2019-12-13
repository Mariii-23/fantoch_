use crate::client::{Client, Workload};
use crate::command::{Command, CommandResult};
use crate::config::Config;
use crate::id::{ClientId, ProcessId};
use crate::planet::{Planet, Region};
use crate::protocol::{Process, ToSend};
use crate::sim::Router;
use crate::sim::Schedule;
use crate::stats::Stats;
use crate::time::SimTime;
use std::collections::HashMap;

pub enum ScheduleAction<P: Process> {
    SubmitToProc(ProcessId, Command),
    SendToProc(ProcessId, P::Message),
    SendToClient(ClientId, CommandResult),
}

pub struct Runner<P: Process> {
    planet: Planet,
    router: Router<P>,
    time: SimTime,
    schedule: Schedule<ScheduleAction<P>>,
    // mapping from process identifier to its region
    process_to_region: HashMap<ProcessId, Region>,
    // mapping from client identifier to its region
    client_to_region: HashMap<ClientId, Region>,
}

impl<P> Runner<P>
where
    P: Process,
{
    /// Create a new `Runner` from a `planet`, a `config`, and two lists of regions:
    /// - `process_regions`: list of regions where processes are located
    /// - `client_regions`: list of regions where clients are located
    pub fn new<F>(
        planet: Planet,
        config: Config,
        create_process: F,
        workload: Workload,
        process_regions: Vec<Region>,
        client_regions: Vec<Region>,
    ) -> Self
    where
        F: Fn(ProcessId, Region, Planet, Config) -> P,
    {
        // check that we have the correct number of `process_regions`
        let process_count = process_regions.len();
        let client_count = client_regions.len();
        assert_eq!(process_count, config.n());

        // create router
        let mut router = Router::new();
        let mut processes = Vec::with_capacity(config.n());

        // create processes
        let to_discover: Vec<_> = process_regions
            .into_iter()
            .zip(1..=process_count)
            .map(|(region, process_id)| {
                let process_id = process_id as u64;
                // create process and save it
                let process = create_process(process_id, region.clone(), planet.clone(), config);
                processes.push(process);

                (process_id, region)
            })
            .collect();

        // create processs to region mapping
        let process_to_region = to_discover.clone().into_iter().collect();

        // register processes
        processes.into_iter().for_each(|mut process| {
            // discover
            assert!(process.discover(to_discover.clone()));
            // and register it
            router.register_process(process);
        });

        // register clients and create client to region mapping
        let client_to_region = client_regions
            .into_iter()
            .zip(1..=client_count)
            .map(|(region, client_id)| {
                let client_id = client_id as u64;
                // create client
                let mut client = Client::new(client_id, region.clone(), planet.clone(), workload);
                // discover
                assert!(client.discover(to_discover.clone()));
                // and register it
                router.register_client(client);
                (client_id, region)
            })
            .collect();

        // create runner
        Self {
            planet,
            router,
            time: SimTime::new(),
            schedule: Schedule::new(),
            process_to_region,
            client_to_region,
        }
    }

    /// Run the simulation.
    pub fn run(&mut self) {
        // start clients
        self.router
            .start_clients(&self.time)
            .into_iter()
            .for_each(|(client_region, to_send)| {
                // schedule client commands
                self.try_to_schedule(client_region, to_send);
            });

        // run the simulation while there are things scheduled
        while let Some(actions) = self.schedule.next_actions(&mut self.time) {
            // for each scheduled action
            actions.into_iter().for_each(|action| {
                match action {
                    ScheduleAction::SubmitToProc(process_id, cmd) => {
                        // get process's region
                        let process_region = self.process_region(process_id);
                        // submit to process and schedule output messages
                        let to_send = self.router.process_submit(process_id, cmd);
                        self.try_to_schedule(process_region, to_send);
                    }
                    ScheduleAction::SendToProc(process_id, msg) => {
                        // get process's region
                        let process_region = self.process_region(process_id);
                        // route to process and schedule output messages
                        let to_send = self.router.route_to_process(process_id, msg);
                        self.try_to_schedule(process_region, to_send);
                    }
                    ScheduleAction::SendToClient(client_id, cmd_result) => {
                        // get client's region
                        let client_region = self.client_region(client_id);
                        // route to client and schedule output command
                        let to_send = self
                            .router
                            .route_to_client(client_id, cmd_result, &self.time);
                        self.try_to_schedule(client_region, to_send);
                    }
                }
            })
        }
    }

    /// Get client's stats.
    /// TODO does this need to be mut?
    pub fn clients_stats(&mut self) -> HashMap<&Region, Stats> {
        let router = &mut self.router;
        self.client_to_region
            .iter()
            .map(|(client_id, region)| {
                let client_stats = router.client_stats(*client_id);
                (region, client_stats)
            })
            .collect()
    }

    /// Try to schedule a `ToSend`. When scheduling, we shoud never route!
    fn try_to_schedule(&mut self, from: Region, to_send: ToSend<P::Message>) {
        match to_send {
            ToSend::ToCoordinator(process_id, cmd) => {
                // create action and schedule it
                let action = ScheduleAction::SubmitToProc(process_id, cmd);
                // get process's region
                let to = self.process_region(process_id);
                self.schedule_it(&from, &to, action);
            }
            ToSend::ToProcesses(target, msg) => {
                // for each process in target, schedule message delivery
                target.into_iter().for_each(|process_id| {
                    // create action and schedule it
                    let action = ScheduleAction::SendToProc(process_id, msg.clone());
                    // get process's region
                    let to = self.process_region(process_id);
                    self.schedule_it(&from, &to, action);
                });
            }
            ToSend::ToClients(cmd_results) => {
                // for each command result, schedule its delivery
                cmd_results.into_iter().for_each(|cmd_result| {
                    // create action and schedule it
                    let client_id = cmd_result.rifl().source();
                    let action = ScheduleAction::SendToClient(client_id, cmd_result);
                    // get client's region
                    let to = self.client_region(client_id);
                    self.schedule_it(&from, &to, action);
                });
            }
            ToSend::Nothing => {
                // nothing to do
            }
        }
    }

    fn schedule_it(&mut self, from: &Region, to: &Region, action: ScheduleAction<P>) {
        // compute distance between regions and schedule action
        let distance = self.distance(from, to);
        self.schedule.schedule(&self.time, distance, action);
    }

    /// Retrieves the region of process with identifier `process_id`.
    // TODO can we avoid cloning here?
    fn process_region(&self, process_id: ProcessId) -> Region {
        self.process_to_region
            .get(&process_id)
            .expect("process region should be known")
            .clone()
    }

    /// Retrieves the region of client with identifier `client_id`.
    // TODO can we avoid cloning here?
    fn client_region(&self, client_id: ClientId) -> Region {
        self.client_to_region
            .get(&client_id)
            .expect("client region should be known")
            .clone()
    }

    /// Computes the distance between two regions which is half the ping latency.
    fn distance(&self, from: &Region, to: &Region) -> u64 {
        let ping_latency = self
            .planet
            .ping_latency(from, to)
            .expect("both regions should exist on the planet");
        println!(
            "{:?} -> {:?}: ping {} | distance {}",
            from,
            to,
            ping_latency,
            ping_latency / 2
        );
        // distance is half the ping latency
        ping_latency / 2
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::id::{ProcessId, Rifl};
    use crate::protocol::BaseProcess;
    use crate::stats::F64;

    #[derive(Clone)]
    enum Message {
        Ping(ProcessId, Rifl),
        Pong(Rifl),
    }

    // Protocol that pings the closest f + 1 processes and returns reply to client.
    struct PingPong {
        bp: BaseProcess,
        acks: HashMap<Rifl, usize>,
    }

    impl Process for PingPong {
        type Message = Message;

        fn new(process_id: ProcessId, region: Region, planet: Planet, config: Config) -> Self {
            // quorum size is f + 1
            let q = config.f() + 1;
            let bp = BaseProcess::new(process_id, region, planet, config, q);

            // create `PingPong`
            Self {
                bp,
                acks: HashMap::new(),
            }
        }

        fn id(&self) -> ProcessId {
            self.bp.process_id
        }

        fn discover(&mut self, processes: Vec<(ProcessId, Region)>) -> bool {
            self.bp.discover(processes)
        }

        fn submit(&mut self, cmd: Command) -> ToSend<Self::Message> {
            // get rifl
            let rifl = cmd.rifl();
            // create entry in acks
            self.acks.insert(rifl, 0);

            // create message
            let msg = Message::Ping(self.id(), rifl);
            // clone the fast quorum
            let fast_quorum = self
                .bp
                .fast_quorum
                .clone()
                .expect("should have a valid fast quorum upon submit");

            // send message to fast quorum
            ToSend::ToProcesses(fast_quorum, msg)
        }

        fn handle(&mut self, msg: Self::Message) -> ToSend<Self::Message> {
            match msg {
                Message::Ping(from, rifl) => {
                    // create msg
                    let msg = Message::Pong(rifl);
                    // reply back
                    ToSend::ToProcesses(vec![from], msg)
                }
                Message::Pong(rifl) => {
                    // increase ack count
                    let ack_count = self
                        .acks
                        .get_mut(&rifl)
                        .expect("there should be an ack count for this rifl");
                    *ack_count += 1;

                    // notify client if enough acks
                    if *ack_count == self.bp.q {
                        // remove from acks
                        self.acks.remove(&rifl);
                        // create fake command result
                        let cmd_result = CommandResult::new(rifl, 0);
                        // notify client
                        ToSend::ToClients(vec![cmd_result])
                    } else {
                        ToSend::Nothing
                    }
                }
            }
        }
    }

    fn run(f: usize) -> (Stats, Stats) {
        // planet
        let planet = Planet::new("latency/");

        // config
        let n = 3;
        let config = Config::new(n, f);

        // function that creates ping pong processes
        let create_process =
            |process_id, region, planet, config| PingPong::new(process_id, region, planet, config);

        // clients workload
        let conflict_rate = 100;
        let total_commands = 10;
        let workload = Workload::new(conflict_rate, total_commands);

        // process regions
        let process_regions = vec![
            Region::new("asia-east1"),
            Region::new("us-central1"),
            Region::new("us-west1"),
        ];

        // client regions
        let client_regions = vec![Region::new("us-west1"), Region::new("us-west2")];

        // create runner
        let mut runner = Runner::new(
            planet,
            config,
            create_process,
            workload,
            process_regions,
            client_regions,
        );

        // run simulation and return stats
        runner.run();
        let mut stats = runner.clients_stats();
        let us_west1 = stats
            .remove(&Region::new("us-west1"))
            .expect("there should stats from us-west1 region");
        let us_west2 = stats
            .remove(&Region::new("us-west2"))
            .expect("there should stats from us-west2 region");
        (us_west1, us_west2)
    }

    #[test]
    fn runner_flow() {
        // expected stats:
        // - client us-west1: since us-west1 is a process, from client's perspective it should be
        //   the latency of accessing the coordinator (0ms) plus the latency of accessing the
        //   closest fast quorum
        // - client us-west2: since us-west2 is _not_ a process, from client's perspective it should
        //   be the latency of accessing the coordinator us-west1 (12ms + 12ms) plus the latency of
        //   accessing the closest fast quorum

        // f = 0
        let f = 0;
        let (us_west1, us_west2) = run(f);
        assert_eq!(us_west1.mean(), F64::new(0.0));
        assert_eq!(us_west2.mean(), F64::new(24.0));

        // f = 1
        let f = 1;
        let (us_west1, us_west2) = run(f);
        assert_eq!(us_west1.mean(), F64::new(34.0));
        assert_eq!(us_west2.mean(), F64::new(58.0));

        // f = 2
        let f = 2;
        let (us_west1, us_west2) = run(f);
        assert_eq!(us_west1.mean(), F64::new(118.0));
        assert_eq!(us_west2.mean(), F64::new(142.0));
    }
}