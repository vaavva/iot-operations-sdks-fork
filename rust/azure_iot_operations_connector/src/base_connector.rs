// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Types for Azure IoT Operations Connectors.

use std::{sync::Arc, time::Duration};

use azure_iot_operations_mqtt::session::SessionManagedClient;
use azure_iot_operations_services::{leased_lock, state_store};

pub mod base_connector;

pub struct ActivePassiveLock {
    leased_lock_client: azure_iot_operations_services::leased_lock::Client<SessionManagedClient>,
    client_id: Vec<u8>,
    lease_duration: Duration,
    default_timeout: Duration,
}

pub enum ReplicaManager {
    ActiveActive,
    ActivePassive(ActivePassiveLock),
}
impl ReplicaManager {
    pub fn new_active_passive(
        client_id: Vec<u8>,
        connector_id: Vec<u8>,
        lease_duration: Option<Duration>,
        default_timeout: Duration,
        state_store_client: Arc<state_store::Client<SessionManagedClient>>,
    ) -> Self {
        // can only fail if either name is empty, which should be validated before this
        let ap_lock = ActivePassiveLock {
            leased_lock_client: leased_lock::Client::new(
                state_store_client.clone(),
                connector_id,
                client_id.clone(),
            )
            .expect("Pre-validated fields should not fail."),
            client_id,
            lease_duration: lease_duration.unwrap_or(Duration::from_secs(60)),
            default_timeout,
        };
        Self::ActivePassive(ap_lock)
    }
    pub fn new_active_active() -> Self {
        Self::ActiveActive
    }

    /// Block until this replica is in the active state.
    /// For active-active, this is a no-op.
    /// For active-passive, this will block until the lock is acquired. Retries/error reporting are handled within this function.
    pub async fn become_active(&self) {
        match &self {
            Self::ActiveActive => {
                return;
            }
            Self::ActivePassive(lock) => {
                // TODO: figure out how to handle this error. retry loop?
                lock.leased_lock_client
                    .acquire_lock(lock.lease_duration, lock.default_timeout)
                    .await
                    .unwrap();
            }
        }
    }

    pub async fn observe_becomes_passive(&self) -> PassiveObservation {
        match &self {
            Self::ActiveActive => {
                return PassiveObservation::ActiveActive;
            }
            Self::ActivePassive(lock) => {
                return PassiveObservation::ActivePassive(
                    lock.leased_lock_client
                        .observe_lock(lock.default_timeout)
                        .await
                        .unwrap()
                        .response,
                    lock.client_id.clone(),
                );
            }
        }
    }

    // pub fn notify_on_lock_lost();
}

pub enum PassiveObservation {
    ActivePassive(leased_lock::LockObservation, Vec<u8>),
    ActiveActive,
}
impl PassiveObservation {
    pub async fn recv_notification(&mut self) -> Option<()> {
        match self {
            PassiveObservation::ActivePassive(observation, client_id) => {
                loop {
                    match observation.recv_notification().await {
                        Some((notification, ack_token)) => {
                            match notification.operation {
                                state_store::Operation::Set(new_lock_holder) => {
                                    // if the notification is about the current lock holder renewing the lease, ignore it
                                    if new_lock_holder == *client_id {
                                        continue;
                                    }
                                    Some(());
                                }
                                // if the notification is Del, it means the lock has been released and we should try to re-acquire.
                                state_store::Operation::Del => {
                                    return Some(());
                                }
                            }
                        }
                        None => return None,
                    }
                }
            }
            PassiveObservation::ActiveActive => {
                // never return because it's not possible to move to the passive state
                loop {}
            }
        }
    }
}
