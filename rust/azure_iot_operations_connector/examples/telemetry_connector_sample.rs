// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use azure_iot_operations_connector::{
    base_connector::base_connector::{Connector, ConnectorOptionsBuilder},
    source_endpoint::temp_rest::RestSourceEndpointFactory,
};
use azure_iot_operations_protocol::application::ApplicationContextBuilder;
use env_logger::Builder;

#[tokio::main(flavor = "current_thread")]
async fn main() {
    Builder::new()
        .filter_level(log::LevelFilter::max())
        .format_timestamp(None)
        .filter_module("rumqttc", log::LevelFilter::Warn)
        .init();

    let application_context = ApplicationContextBuilder::default().build().unwrap();
    let rest_source_endpoint_factory = RestSourceEndpointFactory::new();
    let connector_options = ConnectorOptionsBuilder::default().build().unwrap();
    let rest_connector = Connector::new(
        rest_source_endpoint_factory,
        application_context,
        connector_options,
    );

    rest_connector.run().await;
}
