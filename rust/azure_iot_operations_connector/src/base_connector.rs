// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Types for Azure IoT Operations Connectors.

use crate::connector_configuration::ConnectorConfig;

pub struct BaseConnector {
    connector_config: ConnectorConfig,
}

impl BaseConnector {
    /// Creates a new instance of `BaseConnector`.
    pub fn new() -> Self {
        let connector_config = ConnectorConfig::from_file_mount().unwrap();
        BaseConnector { connector_config }
    }
}
