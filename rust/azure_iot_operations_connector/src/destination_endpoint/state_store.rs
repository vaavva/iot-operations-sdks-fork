// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Destination Endpoint implementation for State Store destination.

use std::{sync::Arc, time::Duration};

use azure_iot_operations_mqtt::session::SessionManagedClient;
use azure_iot_operations_protocol::common::payload_serialize::SerializedPayload;
use azure_iot_operations_services::state_store::{self, SetOptions};
use tokio::sync::{
    mpsc::{UnboundedReceiver, UnboundedSender},
    oneshot,
};

use crate::file_mount_azure_device_registry::adr_client::AssetDefinition;

use crate::destination_endpoint::{AssetForwarderFactory, DestinationEndpoint, Forwarder};

pub struct StateStoreDestinationEndpoint {
    pub default_expiry: Duration,
    pub default_timeout: Duration,
    pub state_store_client: Arc<state_store::Client<SessionManagedClient>>,
}

impl DestinationEndpoint for StateStoreDestinationEndpoint {
    fn create_asset_forwarder_factory(
        &self,
        asset_definition: &AssetDefinition,
    ) -> Result<Box<dyn AssetForwarderFactory>, String> {
        Ok(Box::new(StateStoreAssetForwarderFactory {
            asset_definition: asset_definition.clone(),
            default_expiry: self.default_expiry,
            default_timeout: self.default_timeout,
            state_store_client: self.state_store_client.clone(),
        }))
    }
}

pub struct StateStoreAssetForwarderFactory {
    asset_definition: AssetDefinition, // TODO: probably can just save some of the fields we need
    default_expiry: Duration,
    default_timeout: Duration,
    state_store_client: Arc<state_store::Client<SessionManagedClient>>,
}

impl AssetForwarderFactory for StateStoreAssetForwarderFactory {
    fn create_forwarder(
        &self,
        dataset: String,
        _schema_info: Option<String>,
    ) -> Result<Forwarder, String> {
        let (tx, mut rx): (
            UnboundedSender<(SerializedPayload, oneshot::Sender<Result<(), String>>)>,
            UnboundedReceiver<(SerializedPayload, oneshot::Sender<Result<(), String>>)>,
        ) = tokio::sync::mpsc::unbounded_channel();

        tokio::task::spawn({
            let timeout = self.default_timeout;
            let expiry = self.default_expiry;
            let state_store_client = self.state_store_client.clone();
            let key = {
                if dataset.is_empty() {
                    self.asset_definition.name.clone()
                } else {
                    dataset.clone()
                }
            };
            async move {
                while let Some((message, result_tx)) = rx.recv().await {
                    // create DSS key/value
                    match state_store_client
                        .set(
                            key.clone().into(),
                            message.payload,
                            timeout,
                            None,
                            SetOptions {
                                expires: Some(expiry),
                                ..SetOptions::default()
                            },
                        )
                        .await
                    {
                        Ok(res) => {
                            if res.response {
                                result_tx.send(Ok(()));
                            } else {
                                // This shouldn't be possible since SetOptions are unconditional
                                result_tx.send(Err("Failed to set value".to_string()));
                            }
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
