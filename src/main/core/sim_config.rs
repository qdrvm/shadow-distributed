//! Code for processing parsed Shadow configurations.
//!
//! This involves loading and verifying network graphs, converting options to types/formats that are
//! easier to use in Shadow, verifying that paths exist, etc.

use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::ffi::{OsStr, OsString};
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::RwLock;
use std::time::Duration;

use anyhow::Context;
use once_cell::sync::Lazy;
use rand::{Rng, SeedableRng};
use rand_xoshiro::Xoshiro256PlusPlus;
use shadow_shim_helper_rs::HostId;
use shadow_shim_helper_rs::simulation_time::SimulationTime;

use crate::core::configuration::{
    ConfigOptions, EnvName, Flatten, HostOptions, LogLevel, ProcessArgs, ProcessFinalState,
    ProcessOptions, QDiscMode, parse_string_as_args,
};
use crate::core::distributed::{PartitionMap, ShardId};
use crate::network::graph::{IpAssignment, NetworkGraph, RoutingInfo, load_network_graph};
use crate::utility::units::{self, Unit};
use crate::utility::{tilde_expansion, verify_plugin_path};

/// The simulation configuration after processing the configuration options and network graph.
pub struct SimConfig {
    // deterministic source of randomness for the simulation
    pub random: Xoshiro256PlusPlus,

    // map of ip addresses to graph nodes
    pub ip_assignment: IpAssignment<u32>,

    // routing information for paths between graph nodes
    pub routing_info: RoutingInfo<u32>,

    // bandwidths of hosts at ip addresses
    pub host_bandwidths: HashMap<std::net::IpAddr, Bandwidth>,

    // map from global host id to owning shard
    pub partition_map: PartitionMap,

    // DNS records for all configured hosts, including hosts that may be remote in future shards
    pub dns_hosts: Vec<HostDnsInfo>,

    // a list of hosts and their processes
    pub hosts: Vec<HostInfo>,
}

impl SimConfig {
    pub fn new(config: &ConfigOptions, hosts_to_debug: &HashSet<String>) -> anyhow::Result<Self> {
        // Xoshiro256PlusPlus is not ideal when a seed with many zeros is used, but
        // 'seed_from_u64()' uses SplitMix64 to derive the actual seed, so we are okay here
        let seed = config.general.seed.unwrap();
        let mut random = Xoshiro256PlusPlus::seed_from_u64(seed.into());

        // this should be the same for all hosts
        let randomness_for_seed_calc = random.random();

        // build the host list
        let mut hosts = vec![];
        for (i, (name, host_options)) in config.hosts.iter().enumerate() {
            let host_id = HostId::from(u32::try_from(i).unwrap());
            let new_host = build_host(
                host_id,
                config,
                host_options,
                name,
                randomness_for_seed_calc,
                hosts_to_debug,
            )
            .with_context(|| format!("Failed to configure host '{name}'"))?;
            hosts.push(new_host);
        }
        if hosts.is_empty() {
            return Err(anyhow::anyhow!(
                "The configuration did not contain any hosts"
            ));
        }

        // load and parse the network graph
        let graph: String = load_network_graph(config.network.graph.as_ref().unwrap())
            .map_err(|e| anyhow::anyhow!(e))
            .context("Failed to load the network graph")?;
        let graph = NetworkGraph::parse(&graph)
            .map_err(|e| anyhow::anyhow!(e))
            .context("Failed to parse the network graph")?;

        // check that each node ID is valid
        for host in &hosts {
            if graph.node_id_to_index(host.network_node_id).is_none() {
                return Err(anyhow::anyhow!(
                    "The network node id {} for host '{}' does not exist",
                    host.network_node_id,
                    host.name
                ));
            }
        }

        // assign a bandwidth to every host
        for host in &mut hosts {
            let node_index = graph.node_id_to_index(host.network_node_id).unwrap();
            let node = graph.graph().node_weight(*node_index).unwrap();

            let graph_bw_down_bits = node
                .bandwidth_down
                .map(|x| x.convert(units::SiPrefixUpper::Base).unwrap().value());
            let graph_bw_up_bits = node
                .bandwidth_up
                .map(|x| x.convert(units::SiPrefixUpper::Base).unwrap().value());

            host.bandwidth_down_bits = host.bandwidth_down_bits.or(graph_bw_down_bits);
            host.bandwidth_up_bits = host.bandwidth_up_bits.or(graph_bw_up_bits);

            // check if no bandwidth was provided in the host options or graph node
            if host.bandwidth_down_bits.is_none() {
                return Err(anyhow::anyhow!(
                    "No downstream bandwidth provided for host '{}'",
                    host.name
                ));
            }
            if host.bandwidth_up_bits.is_none() {
                return Err(anyhow::anyhow!(
                    "No upstream bandwidth provided for host '{}'",
                    host.name
                ));
            }
        }

        // check if any hosts in 'hosts_to_debug' don't exist
        for hostname in hosts_to_debug {
            if !hosts.iter().any(|y| &y.name == hostname) {
                return Err(anyhow::anyhow!(
                    "The host to debug '{hostname}' doesn't exist"
                ));
            }
        }

        // assign IP addresses to hosts and graph nodes
        let ip_assignment = assign_ips(&mut hosts)?;

        // generate routing info between every pair of in-use nodes
        let routing_info = generate_routing_info(
            &graph,
            &ip_assignment.get_nodes(),
            config.network.use_shortest_path.unwrap(),
        )?;

        // get all host bandwidths
        let host_bandwidths = hosts
            .iter()
            .map(|host| {
                // we made sure above that every host has a bandwidth set
                let bw = Bandwidth {
                    up_bytes: host.bandwidth_up_bits.unwrap() / 8,
                    down_bytes: host.bandwidth_down_bits.unwrap() / 8,
                };

                (host.ip_addr.unwrap(), bw)
            })
            .collect();

        let distributed_shard_id = config.experimental.distributed_shard_id.unwrap();
        let distributed_shard_count = config.experimental.distributed_shard_count.unwrap();
        if distributed_shard_id >= distributed_shard_count {
            return Err(anyhow::anyhow!(
                "distributed_shard_id ({distributed_shard_id}) must be less than distributed_shard_count ({distributed_shard_count})"
            ));
        }
        let partition_map =
            if let Some(path) = config.experimental.distributed_partition_file.as_ref() {
                load_partition_map(path, &hosts, distributed_shard_count)
            } else {
                PartitionMap::by_host_id_modulo(
                    hosts.iter().map(|host| host.id),
                    distributed_shard_count,
                )
                .map_err(anyhow::Error::from)
            }
            .context("Invalid distributed shard configuration")?;
        if !hosts.iter().any(|host| {
            partition_map
                .is_host_local(ShardId(distributed_shard_id), host.id)
                .unwrap()
        }) {
            return Err(anyhow::anyhow!(
                "distributed shard {distributed_shard_id} has no assigned hosts"
            ));
        }
        if distributed_shard_count > 1 && !config.experimental.use_new_tcp.unwrap() {
            return Err(anyhow::anyhow!(
                "distributed mode requires experimental.use_new_tcp=true; \
                 legacy C TCP packets cannot be serialized across shards"
            ));
        }
        let dns_hosts = hosts
            .iter()
            .map(|host| HostDnsInfo {
                id: host.id,
                name: host.name.clone(),
                ip_addr: host.ip_addr.unwrap(),
            })
            .collect();

        Ok(Self {
            random,
            ip_assignment,
            routing_info,
            host_bandwidths,
            partition_map,
            dns_hosts,
            hosts,
        })
    }
}

fn load_partition_map(
    path: &Path,
    hosts: &[HostInfo],
    shard_count: u32,
) -> anyhow::Result<PartitionMap> {
    let file = std::fs::File::open(path).with_context(|| {
        format!(
            "Failed to open distributed partition file '{}'",
            path.display()
        )
    })?;
    let assignments: BTreeMap<String, u32> = serde_yaml::from_reader(file).with_context(|| {
        format!(
            "Failed to parse distributed partition file '{}'",
            path.display()
        )
    })?;

    let hosts_by_name: HashMap<&str, HostId> = hosts
        .iter()
        .map(|host| (host.name.as_str(), host.id))
        .collect();
    let mut host_shards = Vec::with_capacity(hosts.len());
    let mut assigned_hosts = HashSet::new();

    for (hostname, shard_id) in assignments {
        let host_id = *hosts_by_name.get(hostname.as_str()).ok_or_else(|| {
            anyhow::anyhow!("distributed partition file assigns unknown host '{hostname}'")
        })?;
        if shard_id >= shard_count {
            return Err(anyhow::anyhow!(
                "distributed partition file assigns host '{hostname}' to shard {shard_id}, but shard count is {shard_count}"
            ));
        }
        assigned_hosts.insert(hostname);
        host_shards.push((host_id, ShardId(shard_id)));
    }

    for host in hosts {
        if !assigned_hosts.contains(&host.name) {
            return Err(anyhow::anyhow!(
                "distributed partition file does not assign host '{}'",
                host.name
            ));
        }
    }

    Ok(PartitionMap::from_host_shards(host_shards))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::configuration::{CliOptions, ConfigFileOptions};
    use clap::Parser;

    fn test_config(extra_experimental: &str) -> ConfigOptions {
        let yaml = format!(
            r#"
            general:
              stop_time: 1 s
            network:
              graph:
                type: 1_gbit_switch
            experimental:
              {extra_experimental}
            hosts:
              hosta:
                network_node_id: 0
                processes:
                - path: /bin/true
              hostb:
                network_node_id: 0
                processes:
                - path: /bin/true
        "#
        );
        let config_file: ConfigFileOptions = serde_yaml::from_str(&yaml).unwrap();
        let cli = CliOptions::try_parse_from(["shadow", "-"]).unwrap();
        ConfigOptions::new(config_file, cli)
    }

    #[test]
    fn distributed_shard_config_builds_modulo_partition() {
        let config = test_config(
            r#"
              distributed_shard_id: 1
              distributed_shard_count: 2
              use_new_tcp: true
        "#,
        );

        let sim_config = SimConfig::new(&config, &HashSet::new()).unwrap();

        assert_eq!(
            sim_config.partition_map.shard_for_host(HostId::from(0)),
            Some(ShardId(0))
        );
        assert_eq!(
            sim_config.partition_map.shard_for_host(HostId::from(1)),
            Some(ShardId(1))
        );
    }

    #[test]
    fn distributed_shard_config_loads_partition_file() {
        let dir = tempfile::tempdir().unwrap();
        let partition_path = dir.path().join("partition.yaml");
        std::fs::write(&partition_path, "hosta: 1\nhostb: 0\n").unwrap();
        let mut config = test_config(
            r#"
              distributed_shard_id: 1
              distributed_shard_count: 2
              use_new_tcp: true
        "#,
        );
        config.experimental.distributed_partition_file = Some(partition_path);

        let sim_config = SimConfig::new(&config, &HashSet::new()).unwrap();

        assert_eq!(
            sim_config.partition_map.shard_for_host(HostId::from(0)),
            Some(ShardId(1))
        );
        assert_eq!(
            sim_config.partition_map.shard_for_host(HostId::from(1)),
            Some(ShardId(0))
        );
    }

    #[test]
    fn distributed_shard_config_rejects_partition_file_missing_host() {
        let dir = tempfile::tempdir().unwrap();
        let partition_path = dir.path().join("partition.yaml");
        std::fs::write(&partition_path, "hosta: 0\n").unwrap();
        let mut config = test_config(
            r#"
              distributed_shard_id: 0
              distributed_shard_count: 2
              use_new_tcp: true
        "#,
        );
        config.experimental.distributed_partition_file = Some(partition_path);

        let err = SimConfig::new(&config, &HashSet::new()).err().unwrap();

        assert!(
            err.to_string()
                .contains("Invalid distributed shard configuration")
        );
        assert!(
            err.root_cause()
                .to_string()
                .contains("does not assign host 'hostb'")
        );
    }

    #[test]
    fn distributed_shard_config_rejects_partition_file_unknown_host() {
        let dir = tempfile::tempdir().unwrap();
        let partition_path = dir.path().join("partition.yaml");
        std::fs::write(&partition_path, "hosta: 0\nhostb: 1\nhostc: 0\n").unwrap();
        let mut config = test_config(
            r#"
              distributed_shard_id: 0
              distributed_shard_count: 2
              use_new_tcp: true
        "#,
        );
        config.experimental.distributed_partition_file = Some(partition_path);

        let err = SimConfig::new(&config, &HashSet::new()).err().unwrap();

        assert!(
            err.root_cause()
                .to_string()
                .contains("assigns unknown host 'hostc'")
        );
    }

    #[test]
    fn distributed_shard_config_rejects_partition_file_shard_past_count() {
        let dir = tempfile::tempdir().unwrap();
        let partition_path = dir.path().join("partition.yaml");
        std::fs::write(&partition_path, "hosta: 0\nhostb: 2\n").unwrap();
        let mut config = test_config(
            r#"
              distributed_shard_id: 0
              distributed_shard_count: 2
              use_new_tcp: true
        "#,
        );
        config.experimental.distributed_partition_file = Some(partition_path);

        let err = SimConfig::new(&config, &HashSet::new()).err().unwrap();

        assert!(
            err.root_cause()
                .to_string()
                .contains("shard 2, but shard count is 2")
        );
    }

    #[test]
    fn distributed_shard_config_rejects_shard_id_past_count() {
        let config = test_config(
            r#"
              distributed_shard_id: 2
              distributed_shard_count: 2
        "#,
        );

        let err = SimConfig::new(&config, &HashSet::new()).err().unwrap();

        assert!(err.to_string().contains("must be less than"));
    }

    #[test]
    fn distributed_shard_config_rejects_empty_selected_shard() {
        let config = test_config(
            r#"
              distributed_shard_id: 2
              distributed_shard_count: 3
        "#,
        );

        let err = SimConfig::new(&config, &HashSet::new()).err().unwrap();

        assert!(err.to_string().contains("has no assigned hosts"));
    }

    #[test]
    fn distributed_shard_config_requires_new_tcp() {
        let config = test_config(
            r#"
              distributed_shard_id: 0
              distributed_shard_count: 2
        "#,
        );

        let err = SimConfig::new(&config, &HashSet::new()).err().unwrap();

        assert!(err.to_string().contains("experimental.use_new_tcp=true"));
    }
}

#[derive(Clone)]
pub struct HostDnsInfo {
    pub id: HostId,
    pub name: String,
    pub ip_addr: std::net::IpAddr,
}

#[derive(Clone)]
pub struct HostInfo {
    pub id: HostId,
    pub name: String,
    pub processes: Vec<ProcessInfo>,
    pub seed: u64,
    pub network_node_id: u32,
    pub pause_for_debugging: bool,
    pub cpu_threshold: Option<SimulationTime>,
    pub cpu_precision: Option<SimulationTime>,
    pub bandwidth_down_bits: Option<u64>,
    pub bandwidth_up_bits: Option<u64>,
    pub ip_addr: Option<std::net::IpAddr>,
    pub log_level: Option<LogLevel>,
    pub pcap_config: Option<PcapConfig>,
    pub send_buf_size: u64,
    pub recv_buf_size: u64,
    pub autotune_send_buf: bool,
    pub autotune_recv_buf: bool,
    pub qdisc: QDiscMode,
}

#[derive(Clone)]
pub struct ProcessInfo {
    pub plugin: PathBuf,
    pub start_time: SimulationTime,
    pub shutdown_time: Option<SimulationTime>,
    pub shutdown_signal: nix::sys::signal::Signal,
    pub args: Vec<OsString>,
    pub env: BTreeMap<EnvName, String>,
    pub expected_final_state: ProcessFinalState,
}

#[derive(Debug, Clone)]
pub struct Bandwidth {
    pub up_bytes: u64,
    pub down_bytes: u64,
}

#[derive(Debug, Clone, Copy)]
pub struct PcapConfig {
    pub capture_size: u64,
}

/// For a host entry in the configuration options, build `HostInfo` object.
fn build_host(
    id: HostId,
    config: &ConfigOptions,
    host: &HostOptions,
    hostname: &str,
    randomness_for_seed_calc: u64,
    hosts_to_debug: &HashSet<String>,
) -> anyhow::Result<HostInfo> {
    let hostname = hostname.to_string();

    // hostname hash is used as part of the host's seed
    let hostname_hash = {
        let mut hasher = std::hash::DefaultHasher::new();
        hostname.hash(&mut hasher);
        hasher.finish()
    };

    let pause_for_debugging = hosts_to_debug.contains(&hostname);

    let processes: Vec<_> = host
        .processes
        .iter()
        .map(|proc| {
            build_process(proc, config)
                .with_context(|| format!("Failed to configure process '{}'", proc.path.display()))
        })
        .collect::<anyhow::Result<_>>()?;

    Ok(HostInfo {
        id,
        name: hostname,
        processes,

        seed: randomness_for_seed_calc ^ hostname_hash,
        network_node_id: host.network_node_id,
        pause_for_debugging,

        cpu_threshold: None,
        cpu_precision: Some(SimulationTime::from_nanos(200)),

        bandwidth_down_bits: host
            .bandwidth_down
            .map(|x| x.convert(units::SiPrefixUpper::Base).unwrap().value()),
        bandwidth_up_bits: host
            .bandwidth_up
            .map(|x| x.convert(units::SiPrefixUpper::Base).unwrap().value()),

        ip_addr: host.ip_addr.map(|x| x.into()),
        log_level: host.host_options.log_level.flatten(),
        pcap_config: host
            .host_options
            .pcap_enabled
            .unwrap()
            .then_some(PcapConfig {
                capture_size: host
                    .host_options
                    .pcap_capture_size
                    .unwrap()
                    .convert(units::SiPrefixUpper::Base)
                    .unwrap()
                    .value(),
            }),

        // some options come from the config options and not the host options
        send_buf_size: config
            .experimental
            .socket_send_buffer
            .unwrap()
            .convert(units::SiPrefixUpper::Base)
            .unwrap()
            .value(),
        recv_buf_size: config
            .experimental
            .socket_recv_buffer
            .unwrap()
            .convert(units::SiPrefixUpper::Base)
            .unwrap()
            .value(),
        autotune_send_buf: config.experimental.socket_send_autotune.unwrap(),
        autotune_recv_buf: config.experimental.socket_recv_autotune.unwrap(),
        qdisc: config.experimental.interface_qdisc.unwrap(),
    })
}

/// For a process entry in the configuration options, build a `ProcessInfo` object.
fn build_process(proc: &ProcessOptions, config: &ConfigOptions) -> anyhow::Result<ProcessInfo> {
    let start_time = Duration::from(proc.start_time).try_into().unwrap();
    let shutdown_time = proc
        .shutdown_time
        .map(|x| Duration::from(x).try_into().unwrap());
    let shutdown_signal = *proc.shutdown_signal;
    let sim_stop_time =
        SimulationTime::try_from(Duration::from(config.general.stop_time.unwrap())).unwrap();

    if start_time >= sim_stop_time {
        return Err(anyhow::anyhow!(
            "Process start time '{}' must be earlier than the simulation stop time '{}'",
            proc.start_time,
            config.general.stop_time.unwrap(),
        ));
    }

    if let Some(shutdown_time) = shutdown_time {
        if start_time >= shutdown_time {
            return Err(anyhow::anyhow!(
                "Process start time '{}' must be earlier than its shutdown_time time '{}'",
                proc.start_time,
                proc.shutdown_time.unwrap(),
            ));
        }
        if shutdown_time >= sim_stop_time {
            return Err(anyhow::anyhow!(
                "Process shutdown_time '{}' must be earlier than the simulation stop time '{}'",
                proc.shutdown_time.unwrap(),
                config.general.stop_time.unwrap(),
            ));
        }
    }

    let mut args = match &proc.args {
        ProcessArgs::List(x) => x.iter().map(|y| OsStr::new(y).to_os_string()).collect(),
        ProcessArgs::Str(x) => parse_string_as_args(OsStr::new(&x.trim()))
            .map_err(|e| anyhow::anyhow!(e))
            .with_context(|| format!("Failed to parse arguments: {x}"))?,
    };

    let expanded_path = tilde_expansion(proc.path.to_str().unwrap());

    // a cache so we don't resolve the same path multiple times
    static RESOLVED_PATHS: Lazy<RwLock<HashMap<PathBuf, PathBuf>>> =
        Lazy::new(|| RwLock::new(HashMap::new()));

    let canonical_path = RESOLVED_PATHS.read().unwrap().get(&proc.path).cloned();
    let canonical_path = match canonical_path {
        Some(x) => x,
        None => {
            match RESOLVED_PATHS.write().unwrap().entry(proc.path.clone()) {
                Entry::Occupied(entry) => entry.get().clone(),
                Entry::Vacant(entry) => {
                    // We currently use `which::which`, which searches the `PATH` similarly to a
                    // shell.
                    let canonical_path = which::which(&expanded_path)
                        .map_err(anyhow::Error::from)
                        // `which` returns an absolute path, but it may still contain
                        // symbolic links, .., etc.
                        .and_then(|p| Ok(p.canonicalize()?))
                        .with_context(|| {
                            format!("Failed to resolve plugin path '{expanded_path:?}'")
                        })?;

                    verify_plugin_path(&canonical_path).with_context(|| {
                        format!("Failed to verify plugin path '{canonical_path:?}'")
                    })?;
                    log::info!("Resolved binary path {:?} to {canonical_path:?}", proc.path);

                    entry.insert(canonical_path).clone()
                }
            }
        }
    };

    // set argv[0] as the user-provided expanded string, not the canonicalized version
    args.insert(0, expanded_path.into());

    Ok(ProcessInfo {
        plugin: canonical_path,
        start_time,
        shutdown_time,
        shutdown_signal,
        args,
        env: proc.environment.clone(),
        expected_final_state: proc.expected_final_state,
    })
}

/// Generate an IP assignment map using hosts' configured IP addresses and graph node IDs. For hosts
/// without IP addresses, they will be assigned an arbitrary IP address.
fn assign_ips(hosts: &mut [HostInfo]) -> anyhow::Result<IpAssignment<u32>> {
    let mut ip_assignment = IpAssignment::new();

    // first register hosts that have a specific IP address
    for host in hosts.iter().filter(|x| x.ip_addr.is_some()) {
        let ip = host.ip_addr.unwrap();
        let hostname = &host.name;
        let node_id = host.network_node_id;
        ip_assignment.assign_ip(node_id, ip).with_context(|| {
            format!("Failed to assign IP address {ip} for host '{hostname}' to node '{node_id}'")
        })?;
    }

    // then register remaining hosts
    for host in hosts.iter_mut().filter(|x| x.ip_addr.is_none()) {
        let ip = ip_assignment.assign(host.network_node_id);
        // assign the new IP to the host
        host.ip_addr = Some(ip);
    }

    Ok(ip_assignment)
}

/// Generate a map containing routing information (latency, packet loss, etc) for each pair of
/// nodes.
fn generate_routing_info(
    graph: &NetworkGraph,
    nodes: &std::collections::HashSet<u32>,
    use_shortest_paths: bool,
) -> anyhow::Result<RoutingInfo<u32>> {
    // convert gml node IDs to petgraph indexes
    let nodes: Vec<_> = nodes
        .iter()
        .map(|x| *graph.node_id_to_index(*x).unwrap())
        .collect();

    // helper to convert petgraph indexes back to gml node IDs
    let to_ids = |((src, dst), path)| {
        let src = graph.node_index_to_id(src).unwrap();
        let dst = graph.node_index_to_id(dst).unwrap();
        ((src, dst), path)
    };

    let paths = if use_shortest_paths {
        graph
            .compute_shortest_paths(&nodes[..])
            .map_err(|e| anyhow::anyhow!(e))
            .context("Failed to compute shortest paths between graph nodes")?
            .into_iter()
            .map(to_ids)
            .collect()
    } else {
        graph
            .get_direct_paths(&nodes[..])
            .map_err(|e| anyhow::anyhow!(e))
            .context("Failed to get the direct paths between graph nodes")?
            .into_iter()
            .map(to_ids)
            .collect()
    };

    Ok(RoutingInfo::new(paths))
}
