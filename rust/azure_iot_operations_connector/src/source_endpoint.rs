// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Traits and types for Source Endpoint implementations.

use tokio_util::sync::CancellationToken;

// TODO: remove
pub mod temp_rest;

use crate::{
    MessageSchema,
    destination_endpoint::Forwarder,
    file_mount_azure_device_registry::adr_client::{
        AssetDefinition, AssetEndpointProfile, Dataset, Event,
    },
};

// pub enum ReplicaConfig {
//   ActiveActive,
//   /// Provide desired lease duration (default is 1 second)
//   ActivePassive(Duration)
// }

pub trait SourceEndpointFactory {
    type SE: SourceEndpoint + Send + Sync + 'static;

    /// Returns an error if the aep is invalid for this `SourceEndpoint` type
    /// TODO: need to give some config to the base connector for Active/Active vs Active/Passive and lease duration
    fn create_asset_endpoint_profile_source_endpoint(
        &self,
        aep: AssetEndpointProfile,
    ) -> Result<Self::SE, String>;
}

pub trait SourceEndpoint {
    /// Does any start tasks necessary for the `SourceEndpoint`
    /// May establish the connection for aep
    /// Returns an error if the connection could not be established
    fn start(&self) -> impl std::future::Future<Output = Result<(), String>> + std::marker::Send;

    /// aep_deleted
    fn shutdown(&self)
    -> impl std::future::Future<Output = Result<(), String>> + std::marker::Send;

    /// implementation can create a new SourceEndpoint or modify the existing one
    // fn update_aep(self) -> impl std::future::Future<Output = Result<Self, String>> + std::marker::Send;

    /// notifies source endpoint of a new asset
    fn asset_created_notification(&self, asset_name: String, asset_definition: &AssetDefinition);

    /// notifies source endpoint of an updated asset
    fn asset_updated_notification(&self, asset_name: String, asset_definition: &AssetDefinition);

    /// notifies source endpoint of a new asset
    fn asset_deleted_notification(&self, asset_name: String);

    // TODO: will need some way of notifying the source_endpoint that an aep has been stopped (maybe an asset as well?)
    // it could get this from drop, but it would be helpful to be more explicit

    /// Given an aep, `asset_definition`, and dataset, generates a `MessageSchema` to send to the Schema Registry Client.
    /// This could create a datasetSampler if the implementation wanted
    /// TODO: should this be able to return an error?
    fn get_dataset_message_schema(
        &self,
        asset_definition: &AssetDefinition,
        dataset_name: String,
        dataset: &Dataset,
    ) -> Option<MessageSchema>;
    // TODO: return content_type here too?
    // ) -> Result<MessageSchema, String>; TODO: switch to result - they should have to provide one otherwise ADR cloud is funky

    /// If a message schema is already on the asset,
    /// provide an opportunity to update the schema, otherwise use the existing one
    fn update_dataset_message_schema(
        &self,
        asset_definition: &AssetDefinition,
        dataset_name: String,
        dataset: &Dataset,
        current_message_schema: &MessageSchema,
    ) -> Option<MessageSchema>;

    /// Given an aep, `asset_definition`, and event, generates a `MessageSchema` to send to the Schema Registry Client.
    /// TODO: should this be able to return an error?
    fn get_event_message_schema(
        &self,
        asset_definition: &AssetDefinition,
        event_name: String,
        event: &Event,
    ) -> Option<MessageSchema>;
    // ) -> Result<MessageSchema, String>; TODO: switch to result - they should have to provide one otherwise ADR cloud is funky

    /// If a message schema is already on the asset,
    /// provide an opportunity to update the schema, otherwise use the existing one
    fn update_event_message_schema(
        &self,
        asset_definition: &AssetDefinition,
        event_name: String,
        event: &Event,
        current_message_schema: &MessageSchema,
    ) -> Option<MessageSchema>;

    fn dataset_created_notification(
        &self,
        asset_name: String,
        // dataset_name: String,
        dataset: &Dataset,
        forwarder: Forwarder, // Forwarder or DataTransformer+Forwarder? Depends whether DataTransformer lives in the source endpoint or base connector code
        ct: CancellationToken,
    );
    fn event_created_notification(
        &self,
        asset_name: String,
        // event_name: String,
        event: &Event,
        forwarder: Forwarder,
        ct: CancellationToken,
    );

    fn notify(&self, notification: Notification);

    // / Returns a receiver that will get transformed data in a raw byte format
    // / to be sent either as telemetry or a DSS key value (or others in the future)
    // / whenever an event has been sent from the endpoint
    // / Returns an error if a receiver couldn't be created TODO: error needed?
    // / `UnboundedReceiver` returns None if no more events will be sent
    // fn get_event_receiver(&self, event: &Event) -> Result<UnboundedReceiver<Vec<u8>>, String>;
    // fn get_event_receiver(&self, event: &Event) -> Result<UnboundedReceiver<SerializedPayload>, String>;
}

// self would have
// http client
// assetName
// credentials

pub enum EventType<T> {
    Created(T),
    Updated(T),
    Deleted(String),
}

pub enum Notification {
    Asset(EventType<AssetDefinition>),
    Dataset(EventType<Dataset>),
    Event(EventType<Event>),
}
