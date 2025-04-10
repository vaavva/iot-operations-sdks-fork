// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Destination Endpoint implementation for MQTT Telemetry destination.

use std::{sync::Arc, time::Duration};

use azure_iot_operations_mqtt::session::SessionManagedClient;
use azure_iot_operations_protocol::{
    application::ApplicationContext, common::payload_serialize::SerializedPayload, telemetry,
};
use tokio::sync::{
    mpsc::{UnboundedReceiver, UnboundedSender},
    oneshot,
};

use crate::file_mount_azure_device_registry::adr_client::AssetDefinition;

use crate::destination_endpoint::{AssetForwarderFactory, DestinationEndpoint, Forwarder};

pub struct TelemetryDestinationEndpoint {
    pub default_expiry: Duration,
    pub managed_client: SessionManagedClient,
    pub application_context: ApplicationContext,
    // default_telemetry_senders: telemetry::Sender<Vec<u8>, SessionManagedClient>,
}

impl DestinationEndpoint for TelemetryDestinationEndpoint {
    fn create_asset_forwarder_factory(
        &self,
        asset_definition: &AssetDefinition,
    ) -> Result<Box<dyn AssetForwarderFactory>, String> {
        // create default telemetry::Sender (no additional options that we won't know here)
        let sender_options = telemetry::sender::OptionsBuilder::default()
            .topic_pattern(asset_definition.name.clone()) // asset_definition.topic.path
            .build()
            .unwrap();
        let telemetry_sender: telemetry::Sender<SerializedPayload, _> = telemetry::Sender::new(
            self.application_context.clone(),
            self.managed_client.clone(),
            sender_options,
        )
        .unwrap();
        Ok(Box::new(TelemetryAssetForwarderFactory {
            default_telemetry_sender: telemetry_sender,
            asset_definition: asset_definition.clone(),
            default_expiry: self.default_expiry,
            managed_client: self.managed_client.clone(),
            application_context: self.application_context.clone(),
        }))
    }
}

#[derive(Clone)]
pub struct TelemetryAssetForwarderFactory {
    default_telemetry_sender: telemetry::Sender<SerializedPayload, SessionManagedClient>,
    asset_definition: AssetDefinition,
    default_expiry: Duration,
    managed_client: SessionManagedClient,
    application_context: ApplicationContext,
}

impl AssetForwarderFactory for TelemetryAssetForwarderFactory {
    fn create_forwarder(
        &self,
        dataset: String,
        schema_info: Option<String>,
    ) -> Result<Forwarder, String> {
        let telemetry_sender = {
            if dataset == "asset_definition.default_topic.path" {
                self.default_telemetry_sender.clone()
            } else {
                let sender_options = telemetry::sender::OptionsBuilder::default()
                    .topic_pattern(self.asset_definition.name.clone()) // asset_definition.topic.path
                    .build()
                    .unwrap();
                telemetry::Sender::new(
                    self.application_context.clone(),
                    self.managed_client.clone(),
                    sender_options,
                )
                .unwrap()
            }
        };

        let (tx, mut rx): (
            UnboundedSender<(SerializedPayload, oneshot::Sender<Result<(), String>>)>,
            UnboundedReceiver<(SerializedPayload, oneshot::Sender<Result<(), String>>)>,
        ) = tokio::sync::mpsc::unbounded_channel();

        tokio::task::spawn({
            let expiry = self.default_expiry;
            async move {
                while let Some((message, result_tx)) = rx.recv().await {
                    // create MQTT message, setting schema id to response from SR (message_schema_uri)
                    // TODO: cloud event
                    // retain comes from asset definition
                    let message = telemetry::sender::MessageBuilder::default()
                        .payload(message)
                        .unwrap() // TODO: need a way to return this back to the read_telemetry func probably
                        .message_expiry(expiry) // TODO: value?
                        .custom_user_data(vec![(
                            "schemaId".to_string(),
                            schema_info.clone().unwrap_or_default(),
                        )])
                        .build()
                        .unwrap();
                    // send message with telemetry::Sender
                    match telemetry_sender.send(message).await {
                        Ok(()) => {
                            result_tx.send(Ok(()));
                        }
                        Err(e) => {
                            // TODO: translate this to a meaningful error type
                            result_tx.send(Err(e.to_string()));
                        }
                    }
                }
            }
        });
        Ok(Forwarder { tx })
    }
}
