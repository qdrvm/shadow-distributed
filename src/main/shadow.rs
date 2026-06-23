//! The main entrypoint to Shadow.
//!
//! This is called from a small C wrapper for build complexity reasons.

use std::borrow::Borrow;
use std::ffi::{CStr, OsStr, OsString};
use std::fmt::Write;
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::process::{Child, Command};
use std::thread;

use anyhow::Context;
use clap::Parser;
use nix::sys::{personality, resource, signal};
use signal_hook::{consts, iterator::Signals};

use crate::core::configuration::{CliOptions, ConfigFileOptions, ConfigOptions};
use crate::core::controller::Controller;
use crate::core::distributed::{
    DistributedControlServer, DistributedPacketExchangeBackend, DistributedPacketExchangeContext,
    RemotePacketExchangeError, ShardId,
};
use crate::core::logger::shadow_logger;
use crate::core::sim_config::SimConfig;
use crate::core::worker;
use crate::cshadow as c;
use crate::utility::shm_cleanup;

use shadow_build_info::{BUILD_TIMESTAMP, GIT_BRANCH, GIT_COMMIT_INFO, GIT_DATE};

const HELP_INFO_STR: &str =
    "For more information, visit https://shadow.github.io or https://github.com/shadow";

const DISTRIBUTED_SHARD_ID_ARG: &str = "--distributed-shard-id";
const DISTRIBUTED_SHARD_COUNT_ARG: &str = "--distributed-shard-count";
const DISTRIBUTED_IPC_SOCKET_DIR_ARG: &str = "--distributed-ipc-socket-dir";
const DATA_DIRECTORY_ARG: &str = "--data-directory";
const DATA_DIRECTORY_SHORT_ARG: &str = "-d";

/// Main entry point for the simulator.
pub fn run_shadow(args: Vec<&OsStr>) -> anyhow::Result<()> {
    // Install the shared memory allocator's clean up routine on exit. Once this guard is dropped,
    // all shared memory allocations will become invalid.
    let _guard = unsafe { crate::shadow_shmem::allocator::SharedMemAllocatorDropGuard::new() };

    verify_glib_version().context("Unsupported GLib version")?;

    let mut signals_list = Signals::new([consts::signal::SIGINT, consts::signal::SIGTERM])?;
    thread::spawn(move || {
        // `next()` should block until we've received a signal, or `signals_list` is closed and
        // `None` is returned
        if let Some(signal) = signals_list.forever().next() {
            log::info!("Received signal {signal}. Flushing log and exiting");
            log::logger().flush();
            std::process::exit(1);
        }
        log::debug!("Finished waiting for a signal");
    });

    // unblock all signals in shadow and child processes since cmake's ctest blocks
    // SIGTERM (and maybe others)
    signal::sigprocmask(
        signal::SigmaskHow::SIG_SETMASK,
        Some(&signal::SigSet::empty()),
        None,
    )?;

    // parse the options from the command line
    let options = match CliOptions::try_parse_from(args.clone()) {
        Ok(x) => x,
        Err(e) => {
            // will print to either stdout or stderr with formatting
            e.print().unwrap();
            if e.use_stderr() {
                // the `clap::Error` represents an error (ex: invalid flag)
                std::process::exit(1);
            } else {
                // the `clap::Error` represents a non-error, but we'll want to exit anyways (ex:
                // '--help')
                std::process::exit(0);
            }
        }
    };

    if options.show_build_info {
        write_build_info(std::io::stderr()).unwrap();
        std::process::exit(0);
    }

    if options.shm_cleanup {
        // clean up any orphaned shared memory
        shm_cleanup::shm_cleanup(shm_cleanup::SHM_DIR_PATH)
            .context("Cleaning shared memory files")?;
        std::process::exit(0);
    }

    // read from stdin if the config filename is given as '-'
    let config_filename: String = match options.config.as_ref().unwrap().as_str() {
        "-" => "/dev/stdin",
        x => x,
    }
    .into();

    // load the configuration yaml
    let config_file = load_config_file(&config_filename, true)
        .with_context(|| format!("Failed to load configuration file {config_filename}"))?;

    // generate the final shadow configuration from the config file and cli options
    #[cfg_attr(not(feature = "distributed_mpi"), allow(unused_mut))]
    let mut shadow_config = ConfigOptions::new(config_file, options.clone());

    if options.show_config {
        eprintln!("{shadow_config:#?}");
        return Ok(());
    }

    // configure other global state
    if shadow_config.experimental.use_object_counters.unwrap() {
        worker::enable_object_counters();
    }

    // get the log level
    let log_level = shadow_config.general.log_level.unwrap();
    let log_level: log::Level = log_level.into();

    // start up the logging subsystem to handle all future messages
    shadow_logger::init(
        log_level.to_level_filter(),
        shadow_config.experimental.report_errors_to_stderr.unwrap(),
    )
    .unwrap();

    // disable log buffering during startup so that we see every message immediately in the terminal
    shadow_logger::set_buffering_enabled(false);

    // check if some log levels have been compiled out
    if log_level > log::STATIC_MAX_LEVEL {
        log::warn!(
            "Log level set to {}, but messages higher than {} have been compiled out",
            log_level,
            log::STATIC_MAX_LEVEL,
        );
    }

    let distributed_shard_count = shadow_config.experimental.distributed_shard_count.unwrap();
    let is_mpi_distributed_process = cfg!(feature = "distributed_mpi")
        && (std::env::var_os("OMPI_COMM_WORLD_SIZE").is_some()
            || std::env::var_os("PMI_SIZE").is_some()
            || std::env::var_os("PMIX_RANK").is_some());
    if distributed_shard_count > 1
        && options.distributed_ipc_socket_dir.is_none()
        && !is_mpi_distributed_process
    {
        return launch_distributed_shards(&args, &options, &shadow_config, distributed_shard_count);
    }

    // warn if running with root privileges
    if nix::unistd::getuid().is_root() {
        // a real-world example is opentracker, which will attempt to drop privileges if it detects
        // that the effective user is root, but this fails in shadow and opentracker exits with an
        // error
        log::warn!(
            "Shadow is running as root. Shadow does not emulate Linux permissions, and some
            applications may behave differently when running as root. It is recommended to run
            Shadow as a non-root user."
        );
    } else if nix::unistd::geteuid().is_root() {
        log::warn!(
            "Shadow is running with root privileges. Shadow does not emulate Linux permissions,
            and some applications may behave differently when running with root privileges. It
            is recommended to run Shadow as a non-root user."
        );
    }

    // before we run the simulation, clean up any orphaned shared memory
    if let Err(e) = shm_cleanup::shm_cleanup(shm_cleanup::SHM_DIR_PATH) {
        log::warn!("Unable to clean up shared memory files: {e:?}");
    }

    // save the platform data required for CPU pinning
    if shadow_config.experimental.use_cpu_pinning.unwrap() {
        #[allow(clippy::collapsible_if)]
        if unsafe { c::affinity_initPlatformInfo() } != 0 {
            return Err(anyhow::anyhow!("Unable to initialize platform info"));
        }
    }

    // raise fd soft limit to hard limit
    raise_rlimit(resource::Resource::RLIMIT_NOFILE).context("Could not raise fd limit")?;

    // raise number of processes/threads soft limit to hard limit
    raise_rlimit(resource::Resource::RLIMIT_NPROC).context("Could not raise proc limit")?;

    if shadow_config.experimental.use_sched_fifo.unwrap() {
        set_sched_fifo().context("Could not set real-time scheduler mode to SCHED_FIFO")?;
        log::debug!("Successfully set real-time scheduler mode to SCHED_FIFO");
    }

    // Disable address space layout randomization of processes forked from this
    // one to improve determinism in cases when an executable under simulation
    // branch on memory addresses.
    match disable_aslr() {
        Ok(()) => log::debug!("ASLR disabled for processes forked from this parent process"),
        Err(e) => log::warn!(
            "Could not disable address space layout randomization. This may affect determinism: {e:#}"
        ),
    };

    // check sidechannel mitigations
    if sidechannel_mitigations_enabled().context("Failed to get sidechannel mitigation status")? {
        log::warn!(
            "Speculative Store Bypass sidechannel mitigation is enabled (perhaps by seccomp?). \
             This typically adds ~30% performance overhead."
        );
    }

    // Dynamic runahead is unsafe in distributed mode: each shard observes only its
    // local hosts' latencies, so its runahead can diverge from other shards. Shards
    // would then compute different execution windows, which breaks cross-shard event
    // ordering (a packet can arrive in a shard's past) and can desynchronize the
    // per-round global-min collective. Reject the combination up front, before any MPI
    // collectives are started. This guard is intentionally not feature-gated: every
    // distributed backend (MPI and in-process/unix-socket) has the same divergence.
    if shadow_config
        .experimental
        .distributed_shard_count
        .unwrap_or(1)
        > 1
        && shadow_config.experimental.use_dynamic_runahead == Some(true)
    {
        anyhow::bail!(
            "use_dynamic_runahead is not supported in distributed mode \
             (distributed_shard_count > 1): per-shard runahead would diverge and break \
             cross-shard event ordering. Disable dynamic runahead and use a fixed \
             runahead instead."
        );
    }

    // log some information
    // Initialize MPI if running in distributed MPI mode.
    #[cfg(feature = "distributed_mpi")]
    if shadow_config.experimental.distributed_shard_count.unwrap() > 1 {
        crate::core::distributed::mpi_backend::initialize_mpi()
            .context("Failed to initialize MPI for distributed mode")?;

        // Override shard_id from MPI rank. The MPI init sets up rank/size,
        // but the config still has the default shard_id (0) or whatever was
        // passed on the CLI. In MPI mode, each process should use its MPI rank
        // as its shard id.
        let (rank, size) = crate::core::distributed::mpi_backend::mpi_rank_size()
            .context("Failed to get MPI rank/size")?;
        shadow_config.experimental.distributed_shard_id = Some(rank as u32);
        shadow_config.experimental.distributed_shard_count = Some(size as u32);

        // Rewrite the data directory to <base>.shard-N so each rank writes
        // to a distinct output directory, avoiding filesystem races.
        let orig_dir = shadow_config
            .general
            .data_directory
            .as_ref()
            .unwrap()
            .clone();
        let shard_dir = format!("{}.shard-{rank}", orig_dir);
        shadow_config.general.data_directory = Some(shard_dir.clone());
        log::info!("MPI distributed mode: rank={rank} size={size} data_directory={shard_dir}");
    }

    eprintln!("** Starting Shadow {}", env!("CARGO_PKG_VERSION"));
    let mut build_info = Vec::new();
    write_build_info(&mut build_info).unwrap();
    for line in std::str::from_utf8(&build_info).unwrap().trim().split('\n') {
        log::info!("{line}");
    }
    log::info!("Logging current startup arguments and environment");
    log_environment(args.clone());

    if let Err(e) = verify_supported_system() {
        log::warn!("Couldn't verify supported system: {e:?}")
    }

    log::debug!("Startup checks passed, we are ready to start the simulation");

    // allow gdb to attach before starting the simulation
    if options.gdb {
        pause_for_gdb_attach().context("Could not pause shadow to allow gdb to attach")?;
    }

    let sim_config = SimConfig::new(&shadow_config, &options.debug_hosts.unwrap_or_default())
        .context("Failed to initialize the simulation")?;

    // allocate and initialize our main simulation driver
    let controller = if let Some(ipc_socket_dir) = options.distributed_ipc_socket_dir.clone() {
        Controller::new_distributed_shard(sim_config, &shadow_config, ipc_socket_dir)?
    } else {
        Controller::new(sim_config, &shadow_config)
    };

    // enable log buffering if not at trace level
    let buffer_log = !log::log_enabled!(log::Level::Trace);
    shadow_logger::set_buffering_enabled(buffer_log);
    if buffer_log {
        log::info!("Log message buffering is enabled for efficiency");
    }

    // run the simulation
    let result = controller.run().context("Failed to run the simulation");

    // Finalize MPI if running in distributed MPI mode
    #[cfg(feature = "distributed_mpi")]
    {
        crate::core::distributed::mpi_backend::finalize_mpi();
    }

    let () = result?;

    // disable log buffering
    shadow_logger::set_buffering_enabled(false);
    if buffer_log {
        // only show if we disabled buffering above
        log::info!("Log message buffering is disabled during cleanup");
    }

    Ok(())
}

fn launch_distributed_shards(
    args: &[&OsStr],
    options: &CliOptions,
    config: &ConfigOptions,
    shard_count: u32,
) -> anyhow::Result<()> {
    if options.config.as_deref() == Some("-") {
        return Err(anyhow::anyhow!(
            "distributed shard launching does not support reading the config from stdin"
        ));
    }

    let context =
        DistributedPacketExchangeContext::temporary(DistributedPacketExchangeBackend::UnixSocket)
            .context("Failed to initialize distributed IPC context")?;
    let socket_dir = context.socket_dir().to_path_buf();
    let control_server = DistributedControlServer::start(&socket_dir, shard_count)
        .context("Failed to start distributed control server")?;
    log::info!(
        "Launching {shard_count} distributed Shadow shard processes using IPC socket directory '{}'",
        socket_dir.display()
    );

    let program = args
        .first()
        .ok_or_else(|| anyhow::anyhow!("missing argv[0] for distributed shard launch"))?;
    let mut children: Vec<(ShardId, Child)> = Vec::new();

    for shard_id in 0..shard_count {
        let child_args = distributed_child_args(
            args,
            shard_id,
            shard_count,
            &socket_dir,
            config.general.data_directory.as_ref().unwrap(),
        );
        match Command::new(program).args(child_args).spawn() {
            Ok(child) => children.push((ShardId(shard_id), child)),
            Err(e) => {
                for (_shard_id, child) in &mut children {
                    let _ = child.kill();
                    let _ = child.wait();
                }
                return Err(e).with_context(|| {
                    format!("Failed to launch distributed shard process {shard_id}")
                });
            }
        }
    }

    handle_distributed_shutdown_results(
        wait_for_distributed_children(children),
        control_server.shutdown(),
    )?;

    log::info!("All distributed Shadow shard processes finished successfully");
    Ok(())
}

fn handle_distributed_shutdown_results(
    child_result: anyhow::Result<()>,
    control_result: Result<(), RemotePacketExchangeError>,
) -> anyhow::Result<()> {
    match (child_result, control_result) {
        (Ok(()), Ok(())) => Ok(()),
        (Ok(()), Err(e)) => Err(e).context("Distributed control server failed"),
        (Err(e), Ok(())) => Err(e),
        (Err(child_error), Err(control_error)) => Err(anyhow::anyhow!(
            "{child_error}; distributed control server also failed: {control_error}"
        )),
    }
}

fn wait_for_distributed_children(mut children: Vec<(ShardId, Child)>) -> anyhow::Result<()> {
    while !children.is_empty() {
        let mut child_index = 0;
        let mut made_progress = false;

        while child_index < children.len() {
            let (shard_id, child) = &mut children[child_index];
            let status = child
                .try_wait()
                .with_context(|| format!("Failed to poll distributed shard {shard_id:?}"))?;

            let Some(status) = status else {
                child_index += 1;
                continue;
            };

            made_progress = true;
            let (shard_id, _child) = children.swap_remove(child_index);
            if !status.success() {
                kill_distributed_children(&mut children);
                return Err(anyhow::anyhow!(
                    "distributed shard {shard_id:?} exited with {status}"
                ));
            }
        }

        if !made_progress {
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
    }

    Ok(())
}

fn kill_distributed_children(children: &mut [(ShardId, Child)]) {
    for (shard_id, child) in children {
        if let Err(e) = child.kill() {
            log::debug!("failed to kill distributed shard {shard_id:?}: {e}");
        }
        if let Err(e) = child.wait() {
            log::debug!("failed to wait for killed distributed shard {shard_id:?}: {e}");
        }
    }
}

fn distributed_child_args(
    args: &[&OsStr],
    shard_id: u32,
    shard_count: u32,
    socket_dir: &Path,
    base_data_dir: &str,
) -> Vec<OsString> {
    let mut child_args = Vec::new();
    let mut skip_next = false;

    for arg in args.iter().skip(1) {
        if skip_next {
            skip_next = false;
            continue;
        }

        if is_distributed_child_arg(arg) {
            skip_next = true;
            continue;
        }

        if is_distributed_child_arg_with_inline_value(arg) {
            continue;
        }

        child_args.push((*arg).to_os_string());
    }

    child_args.push(OsString::from(DISTRIBUTED_SHARD_ID_ARG));
    child_args.push(OsString::from(shard_id.to_string()));
    child_args.push(OsString::from(DISTRIBUTED_SHARD_COUNT_ARG));
    child_args.push(OsString::from(shard_count.to_string()));
    child_args.push(OsString::from(DISTRIBUTED_IPC_SOCKET_DIR_ARG));
    child_args.push(socket_dir.as_os_str().to_os_string());
    child_args.push(OsString::from(DATA_DIRECTORY_ARG));
    child_args.push(OsString::from(format!("{base_data_dir}.shard-{shard_id}")));

    child_args
}

fn is_distributed_child_arg(arg: &OsStr) -> bool {
    arg == DISTRIBUTED_SHARD_ID_ARG
        || arg == DISTRIBUTED_SHARD_COUNT_ARG
        || arg == DISTRIBUTED_IPC_SOCKET_DIR_ARG
        || arg == DATA_DIRECTORY_ARG
        || arg == DATA_DIRECTORY_SHORT_ARG
}

fn is_distributed_child_arg_with_inline_value(arg: &OsStr) -> bool {
    let Some(arg) = arg.to_str() else {
        return false;
    };

    [
        DISTRIBUTED_SHARD_ID_ARG,
        DISTRIBUTED_SHARD_COUNT_ARG,
        DISTRIBUTED_IPC_SOCKET_DIR_ARG,
        DATA_DIRECTORY_ARG,
    ]
    .iter()
    .any(|name| {
        arg.strip_prefix(name)
            .is_some_and(|suffix| suffix.starts_with('='))
    })
}

pub fn version() -> String {
    let mut s = env!("CARGO_PKG_VERSION").to_string();

    if let (Some(commit), Some(date)) = (GIT_COMMIT_INFO, GIT_DATE) {
        write!(s, " — {commit} {date}").unwrap();
    }

    s
}

fn write_build_info(mut w: impl std::io::Write) -> std::io::Result<()> {
    writeln!(w, "Shadow {}", version())?;
    writeln!(
        w,
        "GLib {}.{}.{}",
        c::GLIB_MAJOR_VERSION,
        c::GLIB_MINOR_VERSION,
        c::GLIB_MICRO_VERSION,
    )?;
    writeln!(w, "Built on {BUILD_TIMESTAMP}")?;
    writeln!(
        w,
        "Built from git branch {}",
        GIT_BRANCH.unwrap_or("<unknown>"),
    )?;
    writeln!(w, "{}", env!("SHADOW_BUILD_INFO"))?;
    writeln!(w, "{HELP_INFO_STR}")?;

    Ok(())
}

fn verify_supported_system() -> anyhow::Result<()> {
    let uts_name = nix::sys::utsname::uname()?;
    let sysname = uts_name
        .sysname()
        .to_str()
        .with_context(|| "Decoding system name")?;
    if sysname != "Linux" {
        anyhow::bail!("Unsupported sysname: {sysname}");
    }
    let version = uts_name
        .release()
        .to_str()
        .with_context(|| "Decoding system release")?;
    let mut version_parts = version.split('.');
    let Some(major) = version_parts.next() else {
        anyhow::bail!("Couldn't find major version in : {version}");
    };
    let major: i32 = major
        .parse()
        .with_context(|| format!("Parsing major version number '{major}'"))?;
    let Some(minor) = version_parts.next() else {
        anyhow::bail!("Couldn't find minor version in : {version}");
    };
    let minor: i32 = minor
        .parse()
        .with_context(|| format!("Parsing minor version number '{minor}'"))?;

    // Keep in sync with `supported_platforms.md`.
    const MIN_KERNEL_VERSION: (i32, i32) = (5, 4);

    if (major, minor) < MIN_KERNEL_VERSION {
        anyhow::bail!(
            "kernel version {major}.{minor} is older than minimum supported version {}.{}",
            MIN_KERNEL_VERSION.0,
            MIN_KERNEL_VERSION.1
        );
    }

    Ok(())
}

fn verify_glib_version() -> anyhow::Result<()> {
    // Technically redundant, since our minimum glib version enforced by cmake is already larger
    // than this version. Still, doesn't hurt to keep this check for posterity in case we ever try
    // to go back to supporting older versions.
    if c::GLIB_MAJOR_VERSION == 2 && c::GLIB_MINOR_VERSION == 40 {
        anyhow::bail!(
            "You compiled against GLib version {}.{}.{}, which has bugs known to break \"
            Shadow. Please update to a newer version of GLib.",
            c::GLIB_MAJOR_VERSION,
            c::GLIB_MINOR_VERSION,
            c::GLIB_MICRO_VERSION,
        );
    }

    // check the that run-time GLib matches the compiled version
    let mismatch = unsafe {
        c::glib_check_version(
            c::GLIB_MAJOR_VERSION,
            c::GLIB_MINOR_VERSION,
            c::GLIB_MICRO_VERSION,
        )
    };

    if !mismatch.is_null() {
        let mismatch = unsafe { std::ffi::CStr::from_ptr(mismatch) };
        anyhow::bail!(
            "The version of the run-time GLib library ({}.{}.{}) is not compatible with \
            the version against which Shadow was compiled ({}.{}.{}). GLib message: '{}'.",
            unsafe { c::glib_major_version },
            unsafe { c::glib_minor_version },
            unsafe { c::glib_micro_version },
            c::GLIB_MAJOR_VERSION,
            c::GLIB_MINOR_VERSION,
            c::GLIB_MICRO_VERSION,
            mismatch.to_string_lossy(),
        );
    }

    Ok(())
}

fn load_config_file(
    filename: impl AsRef<std::path::Path>,
    extended_yaml: bool,
) -> anyhow::Result<ConfigFileOptions> {
    let file = std::fs::File::open(filename).context("Could not open config file")?;

    // serde's default behaviour is to silently ignore duplicate keys during deserialization so we
    // would typically need to use serde_with's `maps_duplicate_key_is_error()` on our
    // 'ConfigFileOptions' struct to prevent duplicate hostnames, but since we deserialize to
    // serde_yaml's `Value` type initially we don't need to prevent duplicate keys as serde_yaml
    // does this for us: https://github.com/dtolnay/serde-yaml/pull/301

    let mut config_file: serde_yaml::Value =
        serde_yaml::from_reader(file).context("Could not parse configuration file as yaml")?;

    if extended_yaml {
        // apply the merge before removing extension fields
        config_file
            .apply_merge()
            .context("Could not merge '<<' keys")?;

        // remove top-level extension fields
        if let serde_yaml::Value::Mapping(mapping) = &mut config_file {
            // remove entries having a key beginning with "x-" (follows docker's convention:
            // https://docs.docker.com/compose/compose-file/#extension)
            mapping.retain(|key, _value| {
                if let serde_yaml::Value::String(key) = key
                    && key.starts_with("x-")
                {
                    return false;
                }
                true
            });
        }
    }

    serde_yaml::from_value(config_file).context("Could not parse configuration file")
}

fn pause_for_gdb_attach() -> anyhow::Result<()> {
    let pid = nix::unistd::getpid();
    log::info!("Pausing with SIGTSTP to enable debugger attachment (pid {pid})");
    eprintln!("** Pausing with SIGTSTP to enable debugger attachment (pid {pid})");

    signal::raise(signal::Signal::SIGTSTP)?;

    log::info!("Resuming now");
    Ok(())
}

fn set_sched_fifo() -> anyhow::Result<()> {
    let mut param: libc::sched_param = unsafe { std::mem::zeroed() };
    param.sched_priority = 1;

    let rv = nix::errno::Errno::result(unsafe {
        libc::sched_setscheduler(0, libc::SCHED_FIFO, std::ptr::from_ref(&param))
    })
    .context("Could not set kernel SCHED_FIFO")?;

    assert_eq!(rv, 0);

    Ok(())
}

fn raise_rlimit(resource: resource::Resource) -> anyhow::Result<()> {
    let (_soft_limit, hard_limit) = resource::getrlimit(resource)?;
    resource::setrlimit(resource, hard_limit, hard_limit)?;
    Ok(())
}

fn disable_aslr() -> anyhow::Result<()> {
    let pers = personality::get().context("Could not get personality")?;
    personality::set(pers | personality::Persona::ADDR_NO_RANDOMIZE)
        .context("Could not set personality")?;
    Ok(())
}

fn sidechannel_mitigations_enabled() -> anyhow::Result<bool> {
    let state = nix::errno::Errno::result(unsafe {
        libc::prctl(
            libc::PR_GET_SPECULATION_CTRL,
            libc::PR_SPEC_STORE_BYPASS,
            0,
            0,
            0,
        )
    })
    .context("Failed prctl()")?;
    let state = state as u32;
    Ok((state & libc::PR_SPEC_DISABLE) != 0)
}

fn log_environment(args: Vec<&OsStr>) {
    for arg in args {
        log::info!("arg: {}", arg.to_string_lossy());
    }

    for (key, value) in std::env::vars_os() {
        let level = match key.to_string_lossy().borrow() {
            "LD_PRELOAD" | "LD_STATIC_TLS_EXTRA" | "G_DEBUG" | "G_SLICE" => log::Level::Info,
            _ => log::Level::Trace,
        };
        log::log!(level, "env: {key:?}={value:?}");
    }
}

mod export {
    use super::*;

    #[unsafe(no_mangle)]
    pub extern "C-unwind" fn main_runShadow(
        argc: libc::c_int,
        argv: *const *const libc::c_char,
    ) -> libc::c_int {
        let args = (0..argc).map(|x| unsafe { CStr::from_ptr(*argv.add(x as usize)) });
        let args = args.map(|x| OsStr::from_bytes(x.to_bytes()));

        let result = run_shadow(args.collect());
        log::logger().flush();

        if let Err(e) = result {
            // log the full error, its context, and its backtrace if enabled
            if log::log_enabled!(log::Level::Error) {
                for line in format!("{e:?}").split('\n') {
                    log::error!("{line}");
                }
                log::logger().flush();

                // print the short error
                eprintln!("** Shadow did not complete successfully: {e}");
                eprintln!("**   {}", e.root_cause());
                eprintln!("** See the log for details");
            } else {
                // logging may not be configured yet, so print to stderr
                eprintln!("{e:?}");
            }

            return 1;
        }

        eprintln!("** Shadow completed successfully");
        0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn distributed_child_args_append_internal_shard_settings() {
        let args = [
            OsStr::new("shadow"),
            OsStr::new("--log-level"),
            OsStr::new("debug"),
            OsStr::new("config.yaml"),
        ];

        let child_args =
            distributed_child_args(&args, 2, 4, Path::new("/tmp/shadow-ipc"), "shadow.data");

        assert_eq!(
            child_args,
            [
                OsString::from("--log-level"),
                OsString::from("debug"),
                OsString::from("config.yaml"),
                OsString::from(DISTRIBUTED_SHARD_ID_ARG),
                OsString::from("2"),
                OsString::from(DISTRIBUTED_SHARD_COUNT_ARG),
                OsString::from("4"),
                OsString::from(DISTRIBUTED_IPC_SOCKET_DIR_ARG),
                OsString::from("/tmp/shadow-ipc"),
                OsString::from(DATA_DIRECTORY_ARG),
                OsString::from("shadow.data.shard-2"),
            ]
        );
    }

    #[test]
    fn distributed_child_args_replace_existing_internal_shard_settings() {
        let args = [
            OsStr::new("shadow"),
            OsStr::new("--log-level"),
            OsStr::new("debug"),
            OsStr::new("--distributed-shard-id=9"),
            OsStr::new("--distributed-shard-count"),
            OsStr::new("9"),
            OsStr::new("--distributed-ipc-socket-dir"),
            OsStr::new("/tmp/old"),
            OsStr::new("--data-directory"),
            OsStr::new("old.data"),
            OsStr::new("config.yaml"),
        ];

        let child_args = distributed_child_args(&args, 1, 3, Path::new("/tmp/new"), "new.data");

        assert_eq!(
            child_args,
            [
                OsString::from("--log-level"),
                OsString::from("debug"),
                OsString::from("config.yaml"),
                OsString::from(DISTRIBUTED_SHARD_ID_ARG),
                OsString::from("1"),
                OsString::from(DISTRIBUTED_SHARD_COUNT_ARG),
                OsString::from("3"),
                OsString::from(DISTRIBUTED_IPC_SOCKET_DIR_ARG),
                OsString::from("/tmp/new"),
                OsString::from(DATA_DIRECTORY_ARG),
                OsString::from("new.data.shard-1"),
            ]
        );
    }

    #[test]
    fn distributed_child_wait_kills_remaining_children_on_first_failure() {
        let failing_child = Command::new("sh").arg("-c").arg("exit 17").spawn().unwrap();
        let long_running_child = Command::new("sh")
            .arg("-c")
            .arg("sleep 60")
            .spawn()
            .unwrap();

        let start = std::time::Instant::now();
        let err = wait_for_distributed_children(vec![
            (ShardId(0), failing_child),
            (ShardId(1), long_running_child),
        ])
        .unwrap_err();

        assert!(
            err.to_string()
                .contains("distributed shard ShardId(0) exited")
        );
        assert!(start.elapsed() < std::time::Duration::from_secs(5));
    }

    #[test]
    fn distributed_child_wait_kills_control_blocked_child_on_first_failure() {
        use std::io::BufRead;

        let dir = tempfile::tempdir().unwrap();
        let _server = DistributedControlServer::start(dir.path(), 2).unwrap();
        let socket_path = dir.path().join("shadow-control.sock");
        let mut control_blocked_child = Command::new("python3")
            .arg("-c")
            .arg(
                r#"
import socket
import struct
import sys

sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
sock.connect(sys.argv[1])
payload = b"SHDW" + struct.pack(">BBIQB", 1, 1, 0, 0, 0)
sock.sendall(struct.pack(">I", len(payload)) + payload)
print("ready", flush=True)
sock.recv(4)
"#,
            )
            .arg(socket_path)
            .stdout(std::process::Stdio::piped())
            .spawn()
            .unwrap();
        let stdout = control_blocked_child.stdout.take().unwrap();
        let mut stdout = std::io::BufReader::new(stdout);
        let mut ready = String::new();
        stdout.read_line(&mut ready).unwrap();
        assert_eq!(ready.trim(), "ready");
        drop(stdout);

        let failing_child = Command::new("sh").arg("-c").arg("exit 17").spawn().unwrap();

        let start = std::time::Instant::now();
        let err = wait_for_distributed_children(vec![
            (ShardId(0), failing_child),
            (ShardId(1), control_blocked_child),
        ])
        .unwrap_err();

        assert!(
            err.to_string()
                .contains("distributed shard ShardId(0) exited")
        );
        assert!(start.elapsed() < std::time::Duration::from_secs(5));
    }

    #[test]
    fn distributed_shutdown_reports_real_malformed_control_child() {
        let dir = tempfile::tempdir().unwrap();
        let server = DistributedControlServer::start(dir.path(), 1).unwrap();
        let socket_path = dir.path().join("shadow-control.sock");
        let malformed_child = Command::new("python3")
            .arg("-c")
            .arg(
                r#"
import socket
import struct
import sys

sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
sock.connect(sys.argv[1])
sock.sendall(struct.pack(">I", 4) + b"nope")
sock.close()
"#,
            )
            .arg(socket_path)
            .spawn()
            .unwrap();

        let err = handle_distributed_shutdown_results(
            wait_for_distributed_children(vec![(ShardId(0), malformed_child)]),
            server.shutdown(),
        )
        .unwrap_err();
        let message = err.to_string();
        let root_cause = err.root_cause().to_string();

        assert!(message.contains("Distributed control server failed"));
        assert!(root_cause.contains("failed to decode distributed control request"));
    }

    #[test]
    fn distributed_shutdown_reports_control_error_after_successful_children() {
        let err = handle_distributed_shutdown_results(
            Ok(()),
            Err(RemotePacketExchangeError::Backend(
                "protocol error".to_string(),
            )),
        )
        .unwrap_err();

        assert!(
            err.to_string()
                .contains("Distributed control server failed")
        );
    }

    #[test]
    fn distributed_shutdown_reports_child_and_control_errors() {
        let err = handle_distributed_shutdown_results(
            Err(anyhow::anyhow!("child failed")),
            Err(RemotePacketExchangeError::Backend(
                "protocol error".to_string(),
            )),
        )
        .unwrap_err();
        let message = err.to_string();

        assert!(message.contains("child failed"));
        assert!(message.contains("distributed control server also failed"));
        assert!(message.contains("protocol error"));
    }
}
