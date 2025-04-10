// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Connector framework for Azure IoT Operations

// #![warn(missing_docs)]
#![allow(clippy::missing_errors_doc)]

use azure_iot_operations_services::schema_registry::PutRequest;

pub type MessageSchema = PutRequest;
#[allow(clippy::missing_panics_doc)]
pub mod base_connector;
pub mod data_transformer;
pub mod destination_endpoint;
pub mod file_mount_azure_device_registry;
pub mod source_endpoint; // TODO: protocol_transformer
