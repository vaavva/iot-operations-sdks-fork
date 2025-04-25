// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Traits, types, and implementations for Azure IoT Operations Connector Destination Endpoints.

use std::time::Duration;

use azure_iot_operations_mqtt::session::SessionManagedClient;
use azure_iot_operations_protocol::{
    application::ApplicationContext, common::payload_serialize::SerializedPayload,
};
use tokio::sync::{mpsc::UnboundedSender, oneshot};

use crate::file_mount_azure_device_registry::adr_client::AssetDefinition;

pub mod mqtt_telemetry;
pub mod state_store;

// Created at the highest level so anything that is shared across the entire connector can be added at creation.
pub trait DestinationEndpoint: Send + Sync {
    fn create_asset_forwarder_factory(
        &self,
        asset_definition: &AssetDefinition,
    ) -> Result<Box<dyn AssetForwarderFactory>, String>;
    // fn register_asset(&self, asset_name: String, asset_definition: AssetDefinition) -> Result<UnboundedSender<(String, Vec<u8>)>, String>;
    // fn deregister_asset(&self, asset_name: String) -> Result<(), String>;
    // fn register_dataset(&self, asset_name: String, dataset: Dataset, schema_info: String) -> Result<(), String>;
    // fn forward_message(&self, asset_name: String, dataset_name: String, message: Vec<u8>) -> Result<(), String>;
}

// handles creating a default forwarder if needed and provides the ability to create a forwarder
// for a specific dataset or event that can be used to forward messages in it's own task
pub trait AssetForwarderFactory: Send + Sync {
    fn create_forwarder(
        &self,
        // probably change this to the actual things we need from the dataset/event instead
        // of one specifically? Or maybe change this to an enum so relevant fields can be gotten from either.
        // Or split to create_event_forwarder and create_dataset_forwarder
        dataset: String,
        schema_info: Option<String>,
        // ) -> Result<UnboundedSender<(Vec<u8>, oneshot::Sender<Result<(), String>>)>, String>;
    ) -> Result<Forwarder, String>;
}

#[derive(Clone)]
pub struct Forwarder {
    tx: UnboundedSender<(SerializedPayload, oneshot::Sender<Result<(), String>>)>,
}
impl Forwarder {
    pub fn send_data(
        &self,
        data: SerializedPayload,
        response_tx: oneshot::Sender<Result<(), String>>,
    ) {
        self.tx.send((data, response_tx)).unwrap();
    }
}

pub struct TelemetryDestinationEndpoint {
    pub default_expiry: Duration,
    pub managed_client: SessionManagedClient,
    pub application_context: ApplicationContext,
    // default_telemetry_senders: TelemetrySender<Vec<u8>, SessionManagedClient>,
}
