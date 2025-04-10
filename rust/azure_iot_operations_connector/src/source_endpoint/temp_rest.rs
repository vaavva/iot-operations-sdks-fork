// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use std::collections::HashMap;

use crate::{
    MessageSchema, base_connector,
    file_mount_azure_device_registry::adr_client::{
        AssetDefinition, AssetEndpointProfile, Dataset, Event,
    },
    source_endpoint::{SourceEndpoint, SourceEndpointFactory},
};

pub struct RestSourceEndpointFactory {
    aeps: HashMap<String, <crate::source_endpoint::temp_rest::RestSourceEndpointFactory as crate::source_endpoint::SourceEndpointFactory>::SE>,
}
impl Default for RestSourceEndpointFactory {
    fn default() -> Self {
        Self::new()
    }
}

impl RestSourceEndpointFactory {
    #[must_use]
    pub fn new() -> Self {
        RestSourceEndpointFactory {
            aeps: HashMap::new(),
        }
    }
}

impl SourceEndpointFactory for RestSourceEndpointFactory {
    type SE = RestSourceEndpoint;
    fn create_asset_endpoint_profile_source_endpoint(
        &self,
        aep: AssetEndpointProfile,
    ) -> Result<Self::SE, String> {
        // if let Some(source_endpoint) = self.aeps.get(&aep.name) {
        //     // update the existing source endpoint
        //     return Ok(source_endpoint);
        // }
        // let http_client_builder = reqwest::ClientBuilder::new();
        // if let Some(request_header) = aep.request_header {
        //     let mut default_headers = reqwest::header::HeaderMap::new();
        //     for (key, value) in aep.request_header {
        //         default_headers.insert(
        //             key,
        //             value,
        //         );
        //     }
        //     http_client_builder.default_headers(default_headers);
        // }
        // if aep.use_proxy {
        //     let proxy = {
        //         if let (Some(proxy_username), Some(proxy_password)) = (aep.proxy_username, aep.proxy_password) {
        //            reqwest::Proxy::all(&aep.proxy_url)?.basic_auth(proxy_username, proxy_password)
        //         } else {
        //             // TODO: all or http/https?
        //             reqwest::Proxy::all(aep.proxy_url)?
        //         }
        //     };
        //     http_client_builder.proxy(proxy);
        // }
        // TODO: TLS config is going to be complex

        // let http_client = http_client_builder
        //     .build()
        //     .unwrap();
        // // url
        // // x request header
        // // auth method (none, API key auth (header), SAT (service account name), OAUTH2 (access token))
        // // persistent connection (cookie_provider?)
        // // x use proxy
        // // x proxy url
        // // x proxy username
        // // x proxy password
        // Ok(RestAepConnection::new(aep, http_client))
        Ok(RestSourceEndpoint::new(aep))
    }
}

pub struct RestSourceEndpoint {
    aep: AssetEndpointProfile,
    asset_definitions: HashMap<String, AssetDefinition>,
    // http_client: reqwest::Client
}
impl RestSourceEndpoint {
    #[must_use]
    pub fn new(aep: AssetEndpointProfile) -> Self {
        RestSourceEndpoint { aep }
    }
    // pub fn new(aep: AssetEndpointProfile, http_client: reqwest::Client) -> Self {
    //     RestAepConnection { aep, http_client }
    // }
}

impl SourceEndpoint for RestSourceEndpoint {
    async fn start(&self) -> Result<(), String> {
        // No-op for REST connector
        Ok(())
    }

    fn get_dataset_message_schema(
        &self,
        asset_definition: &AssetDefinition,
        dataset_name: String,
        dataset: &Dataset,
    ) -> Option<MessageSchema> {
        None
    }

    fn get_event_message_schema(
        &self,
        asset_definition: &AssetDefinition,
        event_name: String,
        event: &Event,
    ) -> Option<MessageSchema> {
        None
    }

    fn shutdown(
        &self,
    ) -> impl std::future::Future<Output = Result<(), String>> + std::marker::Send {
        todo!()
    }

    fn asset_created_notification(&self, asset_name: String, asset_definition: &AssetDefinition) {
        todo!()
        // save information
    }

    fn asset_updated_notification(&self, asset_name: String, asset_definition: &AssetDefinition) {
        todo!()
    }

    fn asset_deleted_notification(&self, asset_name: String) {
        todo!()
    }

    fn update_dataset_message_schema(
        &self,
        asset_definition: &AssetDefinition,
        dataset_name: String,
        dataset: &Dataset,
        current_message_schema: &MessageSchema,
    ) -> Option<MessageSchema> {
        todo!()
    }

    fn update_event_message_schema(
        &self,
        asset_definition: &AssetDefinition,
        event_name: String,
        event: &Event,
        current_message_schema: &MessageSchema,
    ) -> Option<MessageSchema> {
        todo!()
    }

    fn dataset_created_notification(
        &self,
        asset_name: String,
        // dataset_name: String,
        dataset: &Dataset,
        forwarder: crate::destination_endpoint::Forwarder,
        ct: tokio_util::sync::CancellationToken,
    ) {
        todo!()
    }

    fn event_created_notification(
        &self,
        asset_name: String,
        // event_name: String,
        event: &Event,
        forwarder: crate::destination_endpoint::Forwarder,
        ct: tokio_util::sync::CancellationToken,
    ) {
        todo!()
    }

    // async fn sample_dataset(&self, dataset: &Dataset) -> Result<Vec<u8>, String> {
    //     // let request_builder = self.http_client.get(&self.aep.name); // TODO: .target
    //     // request_builder.body(dataset.name.into());
    //     // let request = request_builder.send().await.unwrap();

    //     // call user code to transform the response to a Vec<u8>

    //     Ok("a message!".into())
    // }

    // fn get_event_receiver(
    //     &self,
    //     event: &Event,
    // ) -> Result<tokio::sync::mpsc::UnboundedReceiver<Vec<u8>>, String> {
    //     todo!()
    // }
}
