// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

// Wrappers for incompatible types.

use std::{error::Error, time::Duration};

use azure_iot_operations_mqtt::{
    MqttConnectionSettings,
    session::{Session, SessionManagedClient, SessionOptionsBuilder},
};
use azure_iot_operations_protocol::{
    application::ApplicationContext, common::aio_protocol_error::AIOProtocolErrorKind,
};
use azure_iot_operations_services::state_store;
use tinykube_hostlib::common::dssclient::{StateStoreError, state_store::StateStore};
use tokio::runtime::Handle;

use crate::data_transformer::tinykube::leak;

const STATE_STORE_TIMEOUT: Duration = Duration::from_secs(10);

pub struct StateStoreClientWrapper(state_store::Client<SessionManagedClient>);

impl StateStoreClientWrapper {
    pub fn build(
        application_context: ApplicationContext,
        mqtt_connection_settings: MqttConnectionSettings,
    ) -> Result<Self, Box<dyn Error>> {
        let session = Session::new(
            SessionOptionsBuilder::default()
                .connection_settings(mqtt_connection_settings)
                .build()?,
        )?;
        Ok(Self(state_store::Client::new(
            application_context,
            session.create_managed_client(),
            session.create_connection_monitor(),
            state_store::ClientOptionsBuilder::default().build()?,
        )?))
    }
}

impl StateStore for StateStoreClientWrapper {
    fn get(&self, key_name: leak::Bytes) -> Result<Option<leak::Bytes>, StateStoreError> {
        match Handle::current().block_on(self.0.get(key_name.to_vec(), STATE_STORE_TIMEOUT)) {
            Ok(res) => Ok(res.response.map(|r| r.into())),
            Err(err) => Err(map_state_store_error(err)),
        }
    }

    fn set(&self, key_name: leak::Bytes, value: leak::Bytes) -> Result<(), StateStoreError> {
        match Handle::current().block_on(self.0.set(
            key_name.to_vec(),
            value.to_vec(),
            STATE_STORE_TIMEOUT,
            None,
            Default::default(),
        )) {
            Ok(_) => Ok(()),
            Err(err) => Err(map_state_store_error(err)),
        }
    }
}

fn map_state_store_error(error: state_store::Error) -> StateStoreError {
    match error.kind() {
        state_store::ErrorKind::ServiceError(err) => StateStoreError::RequestError(err.to_string()),
        state_store::ErrorKind::AIOProtocolError(err) => match err.kind {
            AIOProtocolErrorKind::Timeout => StateStoreError::Timeout,
            _ => StateStoreError::Protocol,
        },
        _ => StateStoreError::Internal,
    }
}
