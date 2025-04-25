// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

// Code copied directly from TK repo.

use std::collections::{BTreeMap, BTreeSet};

use tinykube_hostlib::{
    common::config::{
        BranchOutputArm, ConfigGraph, ConfigGraphConnection, ConfigGraphConnectionOperator,
        ConfigGraphModule,
    },
    graph::{self, InputPin, OutputPin},
};

pub fn make_graph_config(config_graph: ConfigGraph) -> (BTreeMap<String, String>, graph::Config) {
    let mut graph_config_nodes = BTreeSet::default();
    let mut graph_config_connections = vec![];
    let mut module_names: BTreeMap<String, String> = BTreeMap::default();
    let modules_config: BTreeMap<String, ConfigGraphModule> = config_graph
        .operations
        .into_iter()
        .map(|m| (m.name.clone(), m))
        .collect();

    for ConfigGraphConnection { from, to } in config_graph.connections {
        let output_pin = if let Some(module_config) = modules_config.get(&from.name) {
            match module_config.operation_type {
                ConfigGraphConnectionOperator::Sink => {
                    panic!("Operation sink not supported in from connection");
                }
                ConfigGraphConnectionOperator::Source => {
                    graph_config_nodes.insert(graph::ConfigNode {
                        id: from.name.clone(),
                        operator: graph::ConfigOperator::Source,
                    });

                    OutputPin(0)
                }

                ConfigGraphConnectionOperator::Map => {
                    graph_config_nodes.insert(graph::ConfigNode {
                        id: from.name.clone(),
                        operator: graph::ConfigOperator::Map,
                    });

                    module_names.insert(from.name.clone(), module_config.module.clone().unwrap());

                    OutputPin(0)
                }

                ConfigGraphConnectionOperator::Filter => {
                    graph_config_nodes.insert(graph::ConfigNode {
                        id: from.name.clone(),
                        operator: graph::ConfigOperator::Filter,
                    });

                    module_names.insert(from.name.clone(), module_config.module.clone().unwrap());

                    OutputPin(0)
                }

                ConfigGraphConnectionOperator::Branch => {
                    graph_config_nodes.insert(graph::ConfigNode {
                        id: from.name.clone(),
                        operator: graph::ConfigOperator::Branch,
                    });

                    module_names.insert(from.name.clone(), module_config.module.clone().unwrap());

                    OutputPin(match from.arm {
                        Some(BranchOutputArm::False) | None => 0,
                        Some(BranchOutputArm::True) => 1,
                    })
                }

                ConfigGraphConnectionOperator::Concatenate => {
                    graph_config_nodes.insert(graph::ConfigNode {
                        id: from.name.clone(),
                        operator: graph::ConfigOperator::Concatenate,
                    });

                    OutputPin(0)
                }

                ConfigGraphConnectionOperator::Accumulate => {
                    graph_config_nodes.insert(graph::ConfigNode {
                        id: from.name.clone(),
                        operator: graph::ConfigOperator::Accumulate,
                    });

                    module_names.insert(from.name.clone(), module_config.module.clone().unwrap());

                    OutputPin(0)
                }

                ConfigGraphConnectionOperator::Delay => {
                    graph_config_nodes.insert(graph::ConfigNode {
                        id: from.name.clone(),
                        operator: graph::ConfigOperator::Delay,
                    });

                    module_names.insert(from.name.clone(), module_config.module.clone().unwrap());

                    OutputPin(0)
                }
            }
        } else {
            panic!(
                "Operation with name {} not found in operations definition",
                from.name
            );
        };

        let input_pin = if let Some(module_config) = modules_config.get(&to.name) {
            match module_config.operation_type {
                ConfigGraphConnectionOperator::Source => {
                    panic!("Operation source not supported in to connection");
                }
                ConfigGraphConnectionOperator::Sink => {
                    graph_config_nodes.insert(graph::ConfigNode {
                        id: to.name.clone(),
                        operator: graph::ConfigOperator::Sink,
                    });

                    InputPin(0)
                }

                ConfigGraphConnectionOperator::Map => {
                    graph_config_nodes.insert(graph::ConfigNode {
                        id: to.name.clone(),
                        operator: graph::ConfigOperator::Map,
                    });

                    module_names.insert(to.name.clone(), module_config.module.clone().unwrap());

                    InputPin(0)
                }

                ConfigGraphConnectionOperator::Filter => {
                    graph_config_nodes.insert(graph::ConfigNode {
                        id: to.name.clone(),
                        operator: graph::ConfigOperator::Filter,
                    });

                    module_names.insert(to.name.clone(), module_config.module.clone().unwrap());

                    InputPin(0)
                }

                ConfigGraphConnectionOperator::Branch => {
                    graph_config_nodes.insert(graph::ConfigNode {
                        id: to.name.clone(),
                        operator: graph::ConfigOperator::Branch,
                    });

                    module_names.insert(to.name.clone(), module_config.module.clone().unwrap());

                    InputPin(0)
                }

                ConfigGraphConnectionOperator::Concatenate => {
                    graph_config_nodes.insert(graph::ConfigNode {
                        id: to.name.clone(),
                        operator: graph::ConfigOperator::Concatenate,
                    });

                    InputPin(0)
                }

                ConfigGraphConnectionOperator::Accumulate => {
                    graph_config_nodes.insert(graph::ConfigNode {
                        id: to.name.clone(),
                        operator: graph::ConfigOperator::Accumulate,
                    });

                    module_names.insert(to.name.clone(), module_config.module.clone().unwrap());

                    InputPin(0)
                }

                ConfigGraphConnectionOperator::Delay => {
                    graph_config_nodes.insert(graph::ConfigNode {
                        id: to.name.clone(),
                        operator: graph::ConfigOperator::Delay,
                    });

                    module_names.insert(to.name.clone(), module_config.module.clone().unwrap());

                    InputPin(0)
                }
            }
        } else {
            panic!(
                "Operation with name {} not found in operations definition",
                to.name
            );
        };

        graph_config_connections.push(graph::ConfigConnection {
            from: graph::ConfigConnectionPoint {
                id: from.name,
                pin: output_pin,
            },
            to: graph::ConfigConnectionPoint {
                id: to.name,
                pin: input_pin,
            },
        });
    }

    (
        module_names,
        graph::Config {
            nodes: graph_config_nodes.into_iter().collect(),
            connections: graph_config_connections,
        },
    )
}

pub fn modules_to_subscribe(config_graph: ConfigGraph) -> Vec<String> {
    config_graph
        .operations
        .iter()
        .filter_map(|config| {
            config
                .module
                .as_ref()
                .map(|name| name.split(':').next().unwrap().to_owned())
        })
        .collect()
}
