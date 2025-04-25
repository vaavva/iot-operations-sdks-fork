// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

// use std::collections::HashMap;

// use crate::{file_mount_azure_device_registry::adr_client::Dataset, destination_endpoint::Forwarder};

pub mod tinykube;

// one per connector
// pub trait DataTransformer {
//   fn new(
//     dataset: Dataset,
//     forwarder: Forwarder
//   ) -> Self;
//   async fn add_sampled_data(
//     &self,
//     dataset: Dataset,
//     headers: HashMap<Vec<u8>, Vec<u8>>,
//     data: Vec<u8>,
//   ) -> Result<CompletionToken, String>;

// }

// pub struct JsonDataTransformer {
//   pub dataset: Dataset,
//   forwarder: Forwarder
// }

// impl DataTransformer<Vec<u8>> for JsonDataTransformer {
//     fn new(
//         dataset: Dataset,
//         forwarder: Forwarder
//       ) -> Self {
//         todo!()
//     }

//     async fn add_sampled_data(
//         &self,
//         data: Vec<u8>,
//       ) -> Result<CompletionToken, String> {
//         todo!()
//     }
// }

// would this be per dataset?
// generic JSON transformer
// on new, gets the asset definition (or the message schema?) so it knows it's mapping.
// takes in bytes, deserializes into serde json, filters on fields and creates new json and serializes to SerializedPayload
//
// REST transformer trait
// on new, gets the asset definition (or the message schema?) so it knows it's mapping
// takes in REST response (headers + payload?)
