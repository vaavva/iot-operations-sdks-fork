// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

// TODO: remove unwraps() and replace with proper error handling

use std::{collections::HashMap, sync::Arc, time::Duration};

use azure_iot_operations_mqtt::{
    MqttConnectionSettingsBuilder,
    session::{
        Session, SessionManagedClient, SessionOptionsBuilder,
        reconnect_policy::{ExponentialBackoffWithJitter, ReconnectPolicy},
    },
};
use azure_iot_operations_protocol::application::ApplicationContext;
use azure_iot_operations_services::{schema_registry, state_store};
use derive_builder::Builder;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::source_endpoint::SourceEndpoint;
use crate::{
    destination_endpoint::{self, DestinationEndpoint},
    file_mount_azure_device_registry::adr_client::{
        ADRClient, ADRError, AssetDefinition, AssetEndpointProfile, AssetEndpointProfileStatus,
        AssetStatus, DatasetsSchema, EventsSchema, MessageSchemaReference,
    },
    source_endpoint::SourceEndpointFactory,
};

pub struct Connector<SEF>
where
    SEF: SourceEndpointFactory + Send + Sync + 'static,
{
    session: Session,
    connector_context: ConnectorContext,
    source_endpoint_factory: SEF,
    state_store_client: Arc<state_store::Client<SessionManagedClient>>,
}

#[derive(Clone)]
struct ConnectorContext {
    // name that would be in asset definition to destination endpoint
    destination_endpoints: Arc<HashMap<String, Box<dyn DestinationEndpoint>>>,
    asset_monitor: Arc<ADRClient>,
    schema_registry_client: schema_registry::Client<SessionManagedClient>,
    default_timeout: Duration,
}

#[derive(Builder)]
#[builder(pattern = "owned")]
pub struct ConnectorOptions {
    /// Reconnect Policy to by used by the `Session`
    #[builder(default = "Box::new(ExponentialBackoffWithJitter::default())")]
    pub reconnect_policy: Box<dyn ReconnectPolicy>, // TODO: re-export

    /// Maximum number of queued outgoing messages not yet accepted by the MQTT Session
    #[builder(default = "100")]
    pub outgoing_max: usize,

    /// Default timeout used for network and command operations
    #[builder(default = "Duration::from_secs(10)")]
    pub default_timeout: Duration,

    /// Default message expiry for telemetry messages or default key expiry for State Store keys
    #[builder(default = "Duration::from_secs(10)")]
    pub default_expiry: Duration,
    // /// Custom forwarders to be used by the connector
    // /// MQTT Telemetry and State Store will always be included in addition to these
    // #[builder(default)]
    // pub custom_forwarder_managers: Hashmap<String, Box<dyn ForwarderManager>>,
}

impl<SEF> Connector<SEF>
where
    SEF: SourceEndpointFactory + Send + Sync + 'static,
{
    // should return error for connection settings and connector_options errors
    pub fn new(
        source_endpoint_factory: SEF,
        application_context: ApplicationContext,
        connector_options: ConnectorOptions,
    ) -> Self {
        // create session
        // TODO: switch to from_file_mount
        let connection_settings = MqttConnectionSettingsBuilder::from_environment()
            .unwrap()
            .build()
            .unwrap(); // both unwraps can fail
        let session_options = SessionOptionsBuilder::default()
            .connection_settings(connection_settings)
            .reconnect_policy(connector_options.reconnect_policy)
            .outgoing_max(connector_options.outgoing_max)
            .build()
            .unwrap(); // can't fail
        let session = Session::new(session_options).unwrap(); // can fail if bad outgoing max
        let connection_monitor = session.create_connection_monitor();
        let schema_registry_client = schema_registry::Client::new(
            application_context.clone(),
            &session.create_managed_client(),
        );
        let state_store_client = state_store::Client::new(
            application_context.clone(),
            session.create_managed_client(),
            connection_monitor,
            state_store::ClientOptionsBuilder::default()
                .build()
                .unwrap(),
        )
        .unwrap(); // errors for this should not be possible
        let arc_state_store_client = Arc::new(state_store_client);

        // maybe create ADR client here too? Not sure why dotnet has it passed in instead
        let asset_monitor = ADRClient::new();

        // initialize destination endpoints
        let mut destination_endpoints: HashMap<String, Box<dyn DestinationEndpoint>> =
            HashMap::new();
        let telemetry_destination_endpoint =
            destination_endpoint::mqtt_telemetry::TelemetryDestinationEndpoint {
                default_expiry: connector_options.default_expiry,
                managed_client: session.create_managed_client(),
                application_context,
            };
        destination_endpoints.insert(
            "Mqtt".to_string(),
            Box::new(telemetry_destination_endpoint) as Box<dyn DestinationEndpoint>,
        );
        let state_store_destination_endpoint =
            destination_endpoint::state_store::StateStoreDestinationEndpoint {
                default_expiry: connector_options.default_expiry,
                default_timeout: connector_options.default_timeout,
                state_store_client: arc_state_store_client.clone(),
            };
        destination_endpoints.insert(
            "Dss".to_string(),
            Box::new(state_store_destination_endpoint) as Box<dyn DestinationEndpoint>,
        );

        Self {
            session,
            connector_context: ConnectorContext {
                destination_endpoints: Arc::new(destination_endpoints),
                asset_monitor: Arc::new(asset_monitor),
                schema_registry_client,
                default_timeout: connector_options.default_timeout,
            },
            source_endpoint_factory,
            state_store_client: arc_state_store_client,
        }
    }

    pub async fn run(self) {
        // start session
        let session = self.session;
        tokio::try_join!(
            // does this task failing always mean that the session needs to be restarted, or can sometimes just this task be restarted?
            // can call session.exit in connector_tasks to stop the session nicely potentially
            async move {
                Self::connector_tasks(
                    self.connector_context,
                    self.source_endpoint_factory,
                    self.state_store_client,
                )
                .await
                .map_err(|e| e.to_string())
            },
            async move { session.run().await.map_err(|e| { e.to_string() }) }
        )
        .unwrap();
    }

    async fn connector_tasks(
        connector_context: ConnectorContext,
        source_endpoint_factory: SEF,
        state_store_client: Arc<state_store::Client<SessionManagedClient>>,
    ) -> Result<(), String> {
        // TODO: make sure to shutdown if cancellation token is called up here too - maybe split everything that isn't shutdown out to another function to capture any errors/cancellation

        // read AEP (ADR client)
        let mut aep_creates_observation = connector_context
            .asset_monitor
            .observe_asset_endpoint_profile_creates()
            .await
            .unwrap(); // metric log error and return after retries
        let aep_context = AssetEndpointProfileContext {
            connector_context: connector_context.clone(),
            root_cancellation_token: CancellationToken::new(),
        };

        let aep_names = connector_context
            .asset_monitor
            .get_asset_endpoint_profiles()
            .await
            .unwrap(); // metric log error and return after retries
        let mut aeps_join_set = JoinSet::new();
        for name in aep_names {
            if let Ok(aep) = connector_context
                .asset_monitor
                .get_asset_endpoint_profile(name)
                .await
            {
                match aep_tasks(aep_context.clone(), &source_endpoint_factory, aep).await {
                    Ok(join_handle) => {
                        aeps_join_set.spawn(join_handle);
                    }
                    Err(e) => {
                        log::error!("Error creating source endpoint: {e}");
                    }
                }
            }
            // if error response, metric log and continue
        }

        loop {
            tokio::select!(
                // TODO: may need branch for monitoring connector config too?
                recv_result = aep_creates_observation.recv() => {
                    if let Some(aep) = recv_result {
                        match aep_tasks(aep_context.clone(), &source_endpoint_factory, aep).await
                        {
                            Ok(join_handle) => {
                                aeps_join_set.spawn(join_handle);
                            },
                            Err(e) => {
                                log::error!("Error creating source endpoint: {e}");
                            }
                        }
                    } else {
                        // TODO: no more notifications will be received, fatal?
                        // metric log error
                        break;
                    }
                },
                next_result = aeps_join_set.join_next() => {
                    match next_result {
                        Some(Ok(Ok(result))) => {
                            // if the aep finished, it will have reported it's status already. We just need to remove it from our tracking and continue
                            match result {
                                EndReason::Deleted => todo!(),
                                EndReason::Updated(aep) => {
                                    match aep_tasks(aep_context.clone(), &source_endpoint_factory, aep).await
                                    {
                                        Ok(join_handle) => {
                                            aeps_join_set.spawn(join_handle);
                                        },
                                        Err(e) => {
                                            log::error!("Error creating source endpoint: {e}");
                                        }
                                    }
                                },
                                EndReason::Error => todo!(),
                                EndReason::Finished => todo!(),
                            }
                        },
                        Some(Ok(Err(e)) | Err(e)) => {
                            // TODO: handle join_set errors
                            log::error!("AEP task was cancelled or panic'd: {e}");
                        },
                        None => {
                            // might need logic for this to not continously win the select loop
                            // Connector has no more aeps running. Continue in case more are discovered
                        },
                    }
                }
            );
        }

        // shutdown all clients
        // fields.asset_monitor.shutdown().await.unwrap();
        connector_context
            .schema_registry_client
            .shutdown()
            .await
            .unwrap();
        state_store_client.shutdown().await.unwrap();

        Ok(())
    }

    // do we want to give a shutdown method that breaks out of the loop or have them provide a cancellationToken/Notify?
}

#[derive(Clone)]
struct AssetEndpointProfileContext {
    connector_context: ConnectorContext,
    root_cancellation_token: CancellationToken,
}

async fn aep_tasks<SEF: SourceEndpointFactory + Send + Sync + 'static>(
    aep_context: AssetEndpointProfileContext,
    source_endpoint_factory: &SEF,
    mut aep: AssetEndpointProfile,
) -> Result<tokio::task::JoinHandle<EndReason<AssetEndpointProfile>>, String> {
    let ct = aep_context.root_cancellation_token.child_token();

    let source_endpoint =
        match source_endpoint_factory.create_asset_endpoint_profile_source_endpoint(aep.clone()) {
            Ok(se) => {
                aep = aep_context
                    .connector_context
                    .asset_monitor
                    .update_aep_status("aep.name".to_string(), None)
                    .await
                    .unwrap();
                se
            }
            // TODO: change create aep se to return a vector of errors so we don't need to do as much translation
            Err(e) => {
                aep_context
                    .connector_context
                    .asset_monitor
                    .update_aep_status(
                        "aep.name".to_string(),
                        Some(AssetEndpointProfileStatus {
                            errors: vec![ADRError {
                                code: 1,
                                message: e.clone(),
                            }],
                        }),
                    )
                    .await
                    .unwrap();
                return Err(e);
            }
        };

    Ok(tokio::task::spawn({
        async move {
            let mut aep_delete_observation = aep_context
                .connector_context
                .asset_monitor
                .observe_asset_endpoint_profile_deletes(aep.name.clone())
                .await
                .unwrap();
            let mut aep_update_observation = aep_context
                .connector_context
                .asset_monitor
                .observe_asset_endpoint_profile_updates(aep.name.clone())
                .await
                .unwrap();

            if let Err(e) = source_endpoint.start().await {
                log::error!("Error starting source endpoint: {e}");
                return EndReason::Error;
            }

            // asset_observation from asset_monitor
            let mut asset_definition_creates_observation = aep_context
                .connector_context
                .asset_monitor
                .observe_asset_definition_creates()
                .await
                .map_err(|e| ADRError {
                    code: 2,
                    message: e,
                })
                .unwrap();

            let source_endpoint = Arc::new(source_endpoint);
            let asset_definition_context = AssetDefinitionContext {
                source_endpoint,
                connector_context: aep_context.connector_context.clone(),
                root_cancellation_token: aep_context.root_cancellation_token.child_token(),
            };

            let asset_names = aep_context
                .connector_context
                .asset_monitor
                .get_asset_names()
                .await
                .map_err(|e| ADRError {
                    code: 2,
                    message: e,
                })
                .unwrap(); // todo: aep.asset_names
            let mut join_set = JoinSet::new();
            for asset_name in asset_names {
                match aep_context
                    .connector_context
                    .asset_monitor
                    .get_asset_definition(asset_name.clone())
                    .await
                {
                    Ok(asset_definition) => {
                        if let Some(join_handle) =
                            asset_tasks(asset_definition_context.clone(), asset_definition).await
                        {
                            join_set.spawn(join_handle);
                        } else {
                            log::error!(
                                "No datasets or events were able to be created for asset '{}'",
                                asset_name
                            );
                        }
                    }
                    Err(e) => {
                        log::error!("Error getting asset definition for '{}': {e}", asset_name);
                    }
                }
            }

            loop {
                tokio::select!(
                    () = ct.cancelled() => {
                        return EndReason::Finished;
                    },
                    recv_result = asset_definition_creates_observation.recv() => {
                        if let Some(asset_definition) = recv_result {
                            // if let Ok(asset_definition_update_observation) = asset_monitor.observe_asset_definition_updates(asset_definition.name).await {
                                if let Some(join_handle) = asset_tasks(asset_definition_context.clone(), asset_definition).await {
                                    join_set.spawn(join_handle);
                                }
                            // }
                        } else {
                            // no more notifications will be received, fatal? Or keep waiting because there can be other notifications? Or restart it?
                            return EndReason::Error;
                       }
                    },
                    recv_result = aep_delete_observation.recv() => {
                        match recv_result {
                            Some(_) => {
                                // aep has been deleted, return
                                ct.cancel();
                                // TODO: call unobserve for this AEP
                                return EndReason::Deleted;
                            },
                            None => {
                                // no more notifications will be received, fatal?
                                // metric log error
                                return EndReason::Error;
                            }
                        }
                    }
                    recv_result = aep_update_observation.recv() => {
                        match recv_result {
                            Some(new_aep) => {
                                // aep has been updated, end it and return the new one
                                ct.cancel();
                                return EndReason::Updated(new_aep);
                            },
                            None => {
                                // no more notifications will be received, fatal?
                                // metric log error
                                return EndReason::Error;
                            }
                        }
                    }
                    next_result = join_set.join_next() => {
                        match next_result {
                            Some(Ok(Ok(result))) => {
                                // if the asset finished, it will have reported it's status already. We just need to remove it from our tracking and continue
                                match result {
                                    EndReason::Deleted => todo!(),
                                    EndReason::Updated(asset_definition) => {
                                        if let Some(join_handle) = asset_tasks(asset_definition_context.clone(), asset_definition).await {
                                            join_set.spawn(join_handle);
                                        }
                                    },
                                    EndReason::Error => todo!(),
                                    EndReason::Finished => todo!(),
                                }
                            },
                            Some(Ok(Err(e)) | Err(e)) => {
                                log::error!("Asset task was cancelled or panic'd: {e}");
                            },
                            None => {

                                // AEP has no more assets running. Continue in case more are discovered
                                // TODO: do we need to add a sleep here to keep this from looping super fast?
                            }
                        }
                    }
                );
            }

            // any cleanup needed?
        }
    }))
}

enum EndReason<T> {
    Deleted,
    Updated(T),
    Error,
    Finished,
}

struct AssetDefinitionContext<SE: SourceEndpoint + Send + Sync + 'static> {
    source_endpoint: Arc<SE>,
    connector_context: ConnectorContext,
    root_cancellation_token: CancellationToken,
}
impl<SE> Clone for AssetDefinitionContext<SE>
where
    SE: SourceEndpoint + Send + Sync + 'static,
{
    fn clone(&self) -> Self {
        Self {
            source_endpoint: self.source_endpoint.clone(),
            connector_context: self.connector_context.clone(),
            root_cancellation_token: self.root_cancellation_token.clone(),
        }
    }

    fn clone_from(&mut self, source: &Self) {
        *self = source.clone();
    }
}
async fn asset_tasks<SE: SourceEndpoint + Send + Sync + 'static>(
    asset_context: AssetDefinitionContext<SE>,
    mut asset_definition: AssetDefinition,
) -> Option<tokio::task::JoinHandle<EndReason<AssetDefinition>>> {
    let ct = asset_context.root_cancellation_token.child_token();
    let (asset_status, mut join_set) = init_asset(
        &asset_definition,
        &asset_context.source_endpoint,
        &asset_context.connector_context.destination_endpoints,
        &asset_context.connector_context.schema_registry_client,
        asset_context.connector_context.default_timeout,
        ct.clone(),
    )
    .await;

    asset_definition = asset_context
        .connector_context
        .asset_monitor
        .update_asset_status(asset_definition.name.clone(), asset_status.clone())
        .await
        .unwrap(); // who knows what to do with this error lol

    if join_set.is_empty() {
        return None;
    }

    Some(tokio::task::spawn({
        let ct_clone = ct.clone();
        let mut asset_delete_observation = asset_context
            .connector_context
            .asset_monitor
            .observe_asset_definition_deletes(asset_definition.name.clone())
            .await
            .unwrap();
        let mut asset_update_observation = asset_context
            .connector_context
            .asset_monitor
            .observe_asset_definition_updates(asset_definition.name.clone())
            .await
            .unwrap();
        async move {
            loop {
                tokio::select! {
                    join_result = join_set.join_next() => {
                        match join_result {
                            Some(Ok(res)) => {
                                // asset/event ended. Log on error? Might already be logged within task
                                log::info!("Asset dataset or event task ended: {res:?}");
                            },
                            Some(Err(e)) => {
                                // TODO: This is when the task has been cancelled or has panic'd.
                                // I don't think this should really happen because of how we use it,
                                // but let's treat it as if it ended and send a metric log
                                log::error!("Asset dataset or event task was cancelled or panic'd: {e}");
                            }
                            None => {
                                // no more datasets/events running, return
                                return EndReason::Finished;
                            }
                        }
                    },
                    delete_notification = asset_delete_observation.recv() => {
                        match delete_notification {
                            Some(_) => {
                                // asset has been deleted, return
                                ct_clone.cancel();
                                // TODO: call unobserve for this asset
                                return EndReason::Deleted;
                            },
                            None => {
                                // no more notifications will be received, fatal?
                                // metric log error
                                return EndReason::Error;
                            }
                        }
                    },
                    update_notification = asset_update_observation.recv() => {
                        match update_notification {
                            Some(new_asset_definition) => {

                                ct_clone.cancel();
                                return EndReason::Updated(new_asset_definition);
                            },
                            None => {
                                // no more notifications will be received, fatal?
                                // metric log error
                                return EndReason::Error;
                            }
                        }
                    }
                }
            }
            // if they all end, that's fine I guess
            // return EndReason::Finished;
        }
    }))
}

async fn init_asset<SE: SourceEndpoint + Send + Sync + 'static>(
    asset_definition: &AssetDefinition,
    source_endpoint: &Arc<SE>,
    // name that would be in asset definition to destination endpoint
    destination_endpoints: &Arc<HashMap<String, Box<dyn DestinationEndpoint>>>,
    schema_registry_client: &schema_registry::Client<SessionManagedClient>,
    default_timeout: Duration,
    cancellation_token: CancellationToken,
) -> (AssetStatus, JoinSet<Result<(), ADRError>>) {
    let mut join_set = JoinSet::new();
    let mut asset_status = AssetStatus {
        datasets_schema: None,
        events_schema: None,
        errors: None,
        version: Some(1), // asset_definition.asset_specification_schema.version
    };
    // DSS vs telemetry set at the asset definition level.
    let Some(destination_endpoint) = destination_endpoints.get(&asset_definition.target) else {
        asset_status.add_error(ADRError {
            code: 1,
            message: "Destination endpoint not found".to_string(),
        });
        return (asset_status, join_set);
    };
    let asset_forwarder_factory =
        match destination_endpoint.create_asset_forwarder_factory(asset_definition) {
            Ok(aff) => aff,
            Err(e) => {
                asset_status.add_error(ADRError {
                    code: 1,
                    message: e,
                });
                return (asset_status, join_set);
            }
        };

    for dataset in asset_definition.datasets.clone() {
        // TODO: check if asset status is present and has a message schema already for each dataset/event
        let message_schema_uri = {
            // get message schema and send to SR
            if let Some(message_schema) = source_endpoint.get_dataset_message_schema(
                asset_definition,
                "dataset_name".to_string(),
                &dataset,
            ) {
                match schema_registry_client
                    .put(message_schema, default_timeout)
                    .await
                {
                    Ok(schema) => {
                        asset_status.add_dataset_schema(DatasetsSchema {
                            name: dataset.name.clone(),
                            message_schema_reference: MessageSchemaReference {
                                name: schema.name,
                                version: schema.version,
                                namespace: schema.namespace,
                            },
                        });
                        schema.hash
                    }
                    Err(e) => {
                        match e.kind() {
                            schema_registry::ErrorKind::InvalidArgument(_)
                            | schema_registry::ErrorKind::SerializationError(_) => {
                                log::error!("Error getting message schema to SR: {}", e);
                                asset_status.add_error(ADRError {
                                    code: 1,
                                    message: e.to_string(),
                                });
                                // continue without message_schema_uri
                                None
                            }
                            _ => {
                                // might need to retry network op
                                // continue without message_schema_uri. Should this be reported to ADR?
                                log::error!("Error getting message schema to SR: {}", e);
                                None
                            }
                        }
                    }
                }
            } else {
                None
            }
        };

        // get Forwarder
        let forwarder = match asset_forwarder_factory
            .create_forwarder(dataset.name.clone(), message_schema_uri)
        {
            Ok(f) => f,
            Err(e) => {
                // shouldn't fail entire asset, just this dataset. Stop running this dataset and log error
                log::error!("Error creating forwarder: {}", e);
                asset_status.add_error(ADRError {
                    code: 1,
                    message: e,
                });
                // TODO: remove from dataset_schemas? remove from SR?
                continue;
            }
        };
        source_endpoint.dataset_created_notification(
            asset_definition.name.clone(),
            &dataset,
            forwarder,
            cancellation_token.child_token(),
        );

        // // spawn task per dataset? make sure this can exit as well.
        // join_set.spawn({
        //     let source_endpoint_clone = source_endpoint.clone();
        //     let ct = cancellation_token.clone();
        //     let frequency = asset_definition.frequency;
        //     // TODO: might have to update this to be per datapoint instead of per dataset
        //     async move {
        //         loop {
        //             tokio::select! {
        //               () = ct.cancelled() => {
        //                 return Ok(());
        //               },
        //               () = tokio::time::sleep(frequency) => { // TODO: dataset.frequency
        //                 let message_payload = source_endpoint_clone
        //                   .sample_dataset(&dataset)
        //                   .await // log error and continue if not fatal (?)
        //                   .unwrap(); // need to have it tell us if the component fatal error or fatal error

        //                   // TRANSFORM - should be option on connector to use no transformation, json transformation, avro transformation, or wasm transformer?
        //                 // have this send a message on a channel to the asset forwarder manager
        //                 // instead of sending the message itself
        //                 forwarder.send(
        //                     message_payload
        //                 ).unwrap(); // fatal // log error and end because it's fatal? Maybe try to recreate forwarder?
        //               }
        //             }
        //         }
        //     }
        // });
    }
    for event in asset_definition.events.clone() {
        // get message schema and send to SR
        let message_schema_uri = {
            if let Some(message_schema) = source_endpoint.get_event_message_schema(
                asset_definition,
                "event_name".to_string(),
                &event,
            ) {
                match schema_registry_client
                    .put(message_schema, default_timeout)
                    .await
                {
                    Ok(schema) => {
                        asset_status.add_events_schema(EventsSchema {
                            name: event.name.clone(),
                            message_schema_reference: MessageSchemaReference {
                                name: schema.name,
                                version: schema.version,
                                namespace: schema.namespace,
                            },
                        });
                        schema.hash
                    }
                    Err(e) => {
                        match e.kind() {
                            schema_registry::ErrorKind::InvalidArgument(_)
                            | schema_registry::ErrorKind::SerializationError(_) => {
                                log::error!("Error getting message schema to SR: {}", e);
                                asset_status.add_error(ADRError {
                                    code: 1,
                                    message: e.to_string(),
                                });
                                // continue without message_schema_uri
                                None
                            }
                            _ => {
                                // might need to retry network op
                                // continue without message_schema_uri. Should this be reported to ADR?
                                log::error!("Error getting message schema to SR: {}", e);
                                None
                            }
                        }
                    }
                }
            } else {
                None
            }
        };

        // get Forwarder
        let forwarder = match asset_forwarder_factory
            .create_forwarder(event.name.clone(), message_schema_uri)
        {
            Ok(f) => f,
            Err(e) => {
                // shouldn't fail entire asset, just this event. Stop running this event and log error
                log::error!("Error creating forwarder: {}", e);
                asset_status.add_error(ADRError {
                    code: 1,
                    message: e,
                });
                // TODO: remove from events_schemas? remove from SR?
                continue;
            }
        };
        source_endpoint.event_created_notification(
            asset_definition.name.clone(),
            &event,
            forwarder,
            cancellation_token.clone(),
        );

        // let mut event_receiver_result = source_endpoint_clone.get_event_receiver(&event);
        // match source_endpoint.get_event_receiver(&event) {
        //     Ok(mut event_receiver) => {
        //         // spawn task per event? make sure this can exit as well.
        //         join_set.spawn({
        //             let ct = cancellation_token.clone();
        //             async move {
        //                 // this will be event driven
        //                 loop {
        //                     tokio::select! {
        //                         () = ct.cancelled() => {
        //                             log::error!("'{}' event receiver has been cancelled", event.name);
        //                             return Ok(());
        //                         },
        //                         msg = event_receiver.recv() => {
        //                             if let Some(message_payload) = msg {
        //                                 forwarder.send(
        //                                     message_payload
        //                                 ).unwrap(); // retry and log errors
        //                             } else {
        //                                 // no more events will be sent, return adr error
        //                                 log::error!("No more '{}' events will be received from source endpoint ", event.name);
        //                                 return Err(ADRError { code: 1, message: "Event receiver closed".to_string() });
        //                             }
        //                         }
        //                     }
        //                 }
        //             }
        //         });
        //     }
        //     Err(e) => {
        //         log::error!("Error getting event receiver: {}", e);
        //         asset_status.add_error(ADRError {
        //             code: 1,
        //             message: e,
        //         });
        //         // TODO: remove from events_schemas? remove from SR?
        //         continue;
        //     }
        // }
    }
    (asset_status, join_set)
}
