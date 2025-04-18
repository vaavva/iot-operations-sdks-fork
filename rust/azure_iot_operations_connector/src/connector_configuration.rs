// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Types and helper functions for retreiving Azure IoT Operations Connector Configurations.

use std::{
    env::{self, VarError},
    path::PathBuf,
    time::Duration,
};

use azure_iot_operations_mqtt::{MqttConnectionSettings, MqttConnectionSettingsBuilder};

/// Configuration for the Azure IoT Operations Connector that comes from environment variables and file mounted configs.
pub struct ConnectorConfig {
    /// The MQTT client ID for the connector.
    pub connector_client_id: String,
    /// The AIO metadata for the connector.
    pub aio_metadata: AIOMetadata,
    /// The log level to be used for diagnostics for the connector.
    pub log_level: LogLevel,
    /// The MQTT connection settings for the connector.
    pub mqtt_connection_settings: MqttConnectionSettings,
}
impl ConnectorConfig {
    /// Construct a new `ConnectorConfig` from the configuration files mounted by the Akri Operator.
    pub fn from_file_mount() -> Result<Self, String> {
        // env vars for connector
        let connector_client_id = env::var("CONNECTOR_CLIENT_ID").expect("should be here");
        // CONNECTOR_CONFIGURATION_MOUNT_PATH
        let connector_config_mount_path =
            env::var("CONNECTOR_CONFIGURATION_MOUNT_PATH").expect("should be here"); // TODO: might be "etc/akri/config/connector_configuration" instead of env var

        // AIO Metadata
        let aio_metadata_path = format!("{connector_config_mount_path}/AIO_METADATA ");
        let aio_metadata: AIOMetadata =
            serde_json::from_str(&std::fs::read_to_string(&aio_metadata_path).unwrap()).unwrap();

        // Diagnostics config
        let diagnotistics_config_path = format!("{connector_config_mount_path}/DIAGNOSTICS");
        let diagnostics_config: DiagnosticsConfig =
            serde_json::from_str(&std::fs::read_to_string(&diagnotistics_config_path).unwrap())
                .unwrap();
        let log_level = diagnostics_config.logs.level;

        // for source endpoint
        // DEVICE_ENDPOINT_TLS_TRUST_BUNDLE_CA_CERT_MOUNT_PATH - need device name and inbound endpoint name
        // DEVICE_ENDPOINT_CREDENTIALS_MOUNT_PATH - need device name, inbound endpoint name, and whether to get username, password, or certificate

        // for adr?
        // ADR_RESOURCES_NAME_MOUNT_PATH

        // for connection settings for connector
        // BROKER_TLS_TRUST_BUNDLE_CACERT_MOUNT_PATH
        // BROKER_SAT_MOUNT_PATH

        let mqtt_connection_settings = from_file_mount().unwrap();

        Ok(ConnectorConfig {
            connector_client_id,
            aio_metadata,
            log_level,
            mqtt_connection_settings,
        })
    }
}

// pub fn connection_settings_from_file_mount(connector_client_id: String, connector_config_mount_path: String) -> Result<MqttConnectionSettings, MqttConnectionSettingsBuilderError> {

// }

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AIOMetadata {
    // #[serde(rename = "AioMinVersion")]
    aio_min_version: Option<String>,
    // #[serde(rename = "AioMaxVersion")]
    aio_max_version: Option<String>,
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MqttConnectionConfiguration {
    #[serde(default = "default_host")]
    host: String,
    // #[serde(rename = "keepAliveSeconds")]
    #[serde(default = "default_keep_alive_seconds")]
    keep_alive_seconds: u64,
    // #[serde(rename = "maxInflightMessages")]
    // #[serde(default = "100")]
    max_inflight_messages: u16,
    // #[serde(default = "Protocol::Mqtt")]
    protocol: Protocol,
    authentication: MqttAuthentication,
    // #[serde(rename = "sessionExpirySeconds")]
    // #[serde(default = "600")]
    session_expiry_seconds: u32,
    // #[serde(default = "Some(MqttTls {mode: Mode::Enabled, trusted_ca_certificate_config_map_ref: None})")]
    tls: Option<MqttTls>,
}

fn default_host() -> String {
    "aio-broker:18883".to_string()
}

fn default_keep_alive_seconds() -> u64 {
    60
}

#[derive(serde::Deserialize)]
pub enum Protocol {
    Mqtt,
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MqttTls {
    mode: Mode,
    trusted_ca_certificate_config_map_ref: Option<String>,
}

#[derive(serde::Deserialize)]
pub enum Mode {
    Enabled,
    enabled,
    Disabled,
    disabled,
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MqttAuthentication {
    method: Method,
    service_account_token_settings: ServiceAccountTokenSettings,
}

#[derive(serde::Deserialize)]
pub enum Method {
    ServiceAccountToken,
}

// #[derive(serde::Deserialize)]
// #[serde(rename_all = "camelCase")]
// pub enum MqttAuthentication {
//   ServiceAccountToken(ServiceAccountTokenSettings)
// }

#[derive(serde::Deserialize)]
pub struct ServiceAccountTokenSettings {
    audience: String,
}

#[derive(serde::Deserialize)]
pub struct DiagnosticsConfig {
    logs: DiagnosticsLogs,
}

#[derive(serde::Deserialize)]
pub struct DiagnosticsLogs {
    level: LogLevel,
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LogLevel {
    Trace,
    Log,
    Info,
    Warn,
    Error,
}

/// Construct a builder from the configuration files mounted by the Akri Operator.
/// This method is only usable for connector applications deployed as a kubernetes pod.
///
/// Values that are not present in the configuration file mounts will be set to defaults
/// (including those that are not possible to be provided by file mounts).
///
/// # Examples
///
/// ```
/// # use azure_iot_operations_mqtt::{MqttConnectionSettings, MqttConnectionSettingsBuilder, MqttConnectionSettingsBuilderError};
/// # fn try_main() -> Result<MqttConnectionSettings, String> {
/// let builder = MqttConnectionSettingsBuilder::from_file_mount()?;
/// let connection_settings = builder.build()
///     .map_err(|e| format!("Failed to build settings: {}", e))?;
/// # Ok(connection_settings)
/// # }
/// # fn main() {
/// #     // Example not run as part of docs
/// #     try_main().ok();
/// # }
/// ```
///
/// # Errors
///
/// Returns a `String` describing the error if:
/// - Required environment variables are missing
/// - Configuration files cannot be read
/// - Configuration values are invalid
pub fn from_file_mount() -> Result<MqttConnectionSettings, String> {
    let mut connection_settings_builder = MqttConnectionSettingsBuilder::default();

    // Read client ID
    let client_id = env::var("CONNECTOR_CLIENT_ID")
        .map_err(|e| format!("Env var CONNECTOR_CLIENT_ID missing: {e}"))?;
    connection_settings_builder = connection_settings_builder.client_id(client_id);

    // --- Mount 1: CONNECTOR_CONFIGURATION_MOUNT_PATH ---
    let connector_config_path = env::var("CONNECTOR_CONFIGURATION_MOUNT_PATH")
        .map_err(|_| "CONNECTOR_CONFIGURATION_MOUNT_PATH is not set in environment".to_string())?;
    let connector_config_pathbuf = PathBuf::from(&connector_config_path);
    let mqtt_connection_configuration_pathbuf =
        connector_config_pathbuf.join("MQTT_CONNECTION_CONFIGURATION");
    if !mqtt_connection_configuration_pathbuf.as_path().exists() {
        return Err(format!(
            "Config mount connection configuration file does not exist: {mqtt_connection_configuration_pathbuf:?}"
        ));
    }
    let mqtt_connection_configuration_file_contents =
        std::fs::read_to_string(mqtt_connection_configuration_pathbuf)
            .map_err(|e| format!("Malformed MQTT_CONNECTION_CONFIGURATION file: {e}"))?;
    let mqtt_connection_configuration: MqttConnectionConfiguration =
        serde_json::from_str(&mqtt_connection_configuration_file_contents)
            .map_err(|e| format!("Error deserializing MQTT_CONNECTION_CONFIGURATION file: {e}"))?;

    // Read target address (hostname:port)
    // Parse hostname and port from target address
    let (hostname, tcp_port) =
        mqtt_connection_configuration
            .host
            .split_once(':')
            .ok_or(format!(
                "BROKER_TARGET_ADDRESS is malformed. Expected format <hostname>:<port>. Found: {}",
                mqtt_connection_configuration.host
            ))?;
    connection_settings_builder = connection_settings_builder.hostname(hostname);
    connection_settings_builder = connection_settings_builder.tcp_port(
        tcp_port
            .parse::<u16>()
            .map_err(|e| format!("Cannot parse MQTT port from BROKER_TARGET_ADDRESS: {e}"))?,
    );

    // Read use TLS setting
    connection_settings_builder =
        connection_settings_builder.use_tls(mqtt_connection_configuration.tls.is_some());
    connection_settings_builder = connection_settings_builder.keep_alive(Duration::from_secs(
        mqtt_connection_configuration.keep_alive_seconds,
    ));
    connection_settings_builder = connection_settings_builder
        .receive_max(mqtt_connection_configuration.max_inflight_messages);
    if !matches!(mqtt_connection_configuration.protocol, Protocol::Mqtt) {
        return Err("Protocol must be Mqtt".to_string());
    }
    connection_settings_builder = connection_settings_builder.session_expiry(Duration::from_secs(
        mqtt_connection_configuration.session_expiry_seconds.into(),
    ));
    // --- Mount 2: BROKER_SAT_MOUNT_PATH ---
    // NOTE: This will be moved to be part of Mount 1 in the future.
    let sat_file = string_from_environment("BROKER_SAT_MOUNT_PATH")?;
    connection_settings_builder = connection_settings_builder.sat_file(sat_file);

    // --- Mount 3: BROKER_TLS_TRUST_BUNDLE_CACERT_MOUNT_PATH ---
    let ca_file = string_from_environment("BROKER_TLS_TRUST_BUNDLE_CACERT_MOUNT_PATH")?;
    connection_settings_builder = connection_settings_builder.ca_file(ca_file);

    connection_settings_builder
        .build()
        .map_err(|e| format!("Connection settings could not be built: {e}"))
}

/// Helper function to get an environment variable as a string.
fn string_from_environment(key: &str) -> Result<Option<String>, String> {
    match env::var(key) {
        Ok(value) => Ok(Some(value)),
        Err(VarError::NotPresent) => Ok(None), // Handled by the validate function if required
        Err(VarError::NotUnicode(_)) => {
            Err("Could not parse non-unicode environment variable".to_string())
        }
    }
}
