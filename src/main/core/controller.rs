use std::io::IsTerminal;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use shadow_shim_helper_rs::emulated_time::EmulatedTime;
use shadow_shim_helper_rs::simulation_time::SimulationTime;
use shadow_shim_helper_rs::util::time::TimeParts;

use crate::core::configuration::ConfigOptions;
use crate::core::distributed::{
    DistributedControlClient, DistributedPacketExchangeBackend, DistributedPacketExchangeContext,
    DistributedSynchronizer, NoopRemotePacketExchange, RemotePacketExchange, ShardId,
};
use crate::core::manager::{Manager, ManagerConfig};
use crate::core::sim_config::SimConfig;
use crate::core::worker;
use crate::utility::status_bar::{self, StatusBar, StatusPrinter};

pub struct Controller<'a> {
    // general options and user configuration for the simulation
    config: &'a ConfigOptions,
    sim_config: Option<SimConfig>,

    // the simulator should attempt to end immediately after this time
    end_time: EmulatedTime,

    // set only for distributed shard child processes launched by the parent process
    distributed_ipc_socket_dir: Option<PathBuf>,
    distributed_control: Option<Box<dyn DistributedSynchronizer>>,
}

impl<'a> Controller<'a> {
    pub fn new(sim_config: SimConfig, config: &'a ConfigOptions) -> Self {
        let end_time: Duration = config.general.stop_time.unwrap().into();
        let end_time: SimulationTime = end_time.try_into().unwrap();
        let end_time = EmulatedTime::SIMULATION_START + end_time;

        #[cfg(feature = "distributed_mpi")]
        let distributed_control = if config.experimental.distributed_shard_count.unwrap() > 1 {
            Some(Box::new(
                crate::core::distributed::mpi_backend::MpiSynchronizer::new()
                    .expect("MPI synchronizer init failed"),
            ) as Box<dyn DistributedSynchronizer>)
        } else {
            None
        };
        #[cfg(not(feature = "distributed_mpi"))]
        let distributed_control = None;

        Self {
            config,
            sim_config: Some(sim_config),
            end_time,
            distributed_ipc_socket_dir: None,
            distributed_control,
        }
    }

    pub fn new_distributed_shard(
        sim_config: SimConfig,
        config: &'a ConfigOptions,
        ipc_socket_dir: PathBuf,
    ) -> anyhow::Result<Self> {
        let current_shard = ShardId(config.experimental.distributed_shard_id.unwrap());
        let distributed_control =
            DistributedControlClient::connect(&ipc_socket_dir, current_shard)?;

        Ok(Self {
            distributed_ipc_socket_dir: Some(ipc_socket_dir),
            distributed_control: Some(Box::new(distributed_control)),
            ..Self::new(sim_config, config)
        })
    }

    pub fn run(mut self) -> anyhow::Result<()> {
        let sim_config = self.sim_config.take().unwrap();

        let status_logger = self.config.general.progress.unwrap().then(|| {
            let state = ShadowStatusBarState::new(self.end_time);

            if std::io::stderr().lock().is_terminal() {
                let redraw_interval = Duration::from_millis(1000);
                StatusLogger::Bar(StatusBar::new(state, redraw_interval))
            } else {
                StatusLogger::Printer(StatusPrinter::new(state))
            }
        });

        let current_shard = ShardId(self.config.experimental.distributed_shard_id.unwrap());
        let remote_packet_exchange = self.remote_packet_exchange(current_shard)?;
        self.wait_for_distributed_peers()?;

        let manager_config =
            ManagerConfig::from_sim_config(sim_config, current_shard, remote_packet_exchange);

        let manager = Manager::new(manager_config, &self, self.config, self.end_time)
            .context("Failed to initialize the manager")?;

        log::info!("Running simulation");
        let num_plugin_errors = manager.run(status_logger.as_ref().map(|x| x.status()))?;
        log::info!("Finished simulation");

        if num_plugin_errors > 0 {
            return Err(anyhow::anyhow!(
                "{num_plugin_errors} managed processes in unexpected final state"
            ));
        }

        Ok(())
    }

    fn remote_packet_exchange(
        &self,
        current_shard: ShardId,
    ) -> anyhow::Result<Box<dyn RemotePacketExchange>> {
        let Some(socket_dir) = self.distributed_ipc_socket_dir.as_ref() else {
            #[cfg(feature = "distributed_mpi")]
            if self.config.experimental.distributed_shard_count.unwrap() > 1 {
                return Ok(Box::new(
                    crate::core::distributed::mpi_backend::MpiRemotePacketExchange::new()?,
                ));
            }

            return Ok(Box::new(NoopRemotePacketExchange));
        };

        let context = DistributedPacketExchangeContext::external(
            DistributedPacketExchangeBackend::UnixSocket,
            socket_dir,
        )?;
        Ok(context.build_exchange(current_shard)?)
    }

    fn wait_for_distributed_peers(&self) -> anyhow::Result<()> {
        let Some(control) = self.distributed_control.as_ref() else {
            return Ok(());
        };

        let start = std::time::Instant::now();
        control.wait()?;
        worker::with_global_sim_stats(|stats| {
            stats.record_distributed_barrier_wait(start.elapsed())
        });
        Ok(())
    }
}

/// Controller methods that are accessed by the manager.
pub trait SimController {
    fn remote_packet_send_complete(&self) -> anyhow::Result<()> {
        Ok(())
    }

    fn manager_finished_current_round(
        &self,
        min_next_event_time: EmulatedTime,
    ) -> anyhow::Result<Option<(EmulatedTime, EmulatedTime)>>;
}

impl SimController for Controller<'_> {
    fn remote_packet_send_complete(&self) -> anyhow::Result<()> {
        self.wait_for_distributed_peers()
    }

    fn manager_finished_current_round(
        &self,
        min_next_event_time: EmulatedTime,
    ) -> anyhow::Result<Option<(EmulatedTime, EmulatedTime)>> {
        let min_next_event_time = match self.distributed_control.as_ref() {
            Some(control) => {
                let start = std::time::Instant::now();
                let min_next_event_time =
                    control.wait_for_global_min_next_event(min_next_event_time)?;
                worker::with_global_sim_stats(|stats| {
                    stats.record_distributed_barrier_wait(start.elapsed())
                });
                min_next_event_time
            }
            None => min_next_event_time,
        };

        let runahead = worker::WORKER_SHARED
            .borrow()
            .as_ref()
            .unwrap()
            .runahead
            .get();
        assert_ne!(runahead, SimulationTime::ZERO);

        let new_start = min_next_event_time;

        // update the new window end as one interval past the new window start, making sure we don't
        // run over the experiment end time
        let new_end = new_start.checked_add(runahead).unwrap_or(EmulatedTime::MAX);
        let new_end = std::cmp::min(new_end, self.end_time);

        let continue_running = new_start < new_end;
        Ok(continue_running.then_some((new_start, new_end)))
    }
}

#[derive(Debug)]
pub struct ShadowStatusBarState {
    start: std::time::Instant,
    pub current: EmulatedTime,
    end: EmulatedTime,
    pub num_failed_processes: u32,
}

impl std::fmt::Display for ShadowStatusBarState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let sim_current = self.current.duration_since(&EmulatedTime::SIMULATION_START);
        let sim_end = self.end.duration_since(&EmulatedTime::SIMULATION_START);
        let frac = sim_current.as_millis() as f32 / sim_end.as_millis() as f32;

        let sim_current = TimeParts::from_nanos(sim_current.as_nanos());
        let sim_end = TimeParts::from_nanos(sim_end.as_nanos());
        let realtime = TimeParts::from_nanos(self.start.elapsed().as_nanos());

        write!(
            f,
            "{}% — simulated: {}/{}, realtime: {}, processes failed: {}",
            (frac * 100.0).round() as i8,
            sim_current.fmt_hr_min_sec_milli(),
            sim_end.fmt_hr_min_sec(),
            realtime.fmt_hr_min_sec(),
            self.num_failed_processes,
        )
    }
}

impl ShadowStatusBarState {
    pub fn new(end: EmulatedTime) -> Self {
        Self {
            start: std::time::Instant::now(),
            current: EmulatedTime::SIMULATION_START,
            end,
            num_failed_processes: 0,
        }
    }
}

enum StatusLogger<T: 'static + status_bar::StatusBarState> {
    Printer(StatusPrinter<T>),
    Bar(StatusBar<T>),
}

impl<T: 'static + status_bar::StatusBarState> StatusLogger<T> {
    pub fn status(&self) -> &Arc<status_bar::Status<T>> {
        match self {
            Self::Printer(x) => x.status(),
            Self::Bar(x) => x.status(),
        }
    }
}
