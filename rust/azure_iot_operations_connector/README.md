Big open questions:
1. When connection to the endpoint happens (once per AEP or per asset or per dataset or options for any)
1. What are setpoints
1. What is Process Control
1. Does the connection need access to the MQTT Session to be able to listen to things from the broker? It could create it's own session though if needed?
1. More details on how polling frequency per datapoint can work
1. What happens when I lost leadership (pause or terminate)

Missing in PoC/proposal
1. Errors need to be thought through

API Surface
```rust
pub struct BaseConnector<TCon>
where
    TCon: Connection + Send + Sync + 'static,
{
    // private
}

pub struct ConnectorOptions {
    /// Reconnect Policy to by used by the `Session`
    #[builder(default = "Box::new(ExponentialBackoffWithJitter::default())")]
    pub reconnect_policy: Box<dyn ReconnectPolicy>,

    /// Maximum number of queued outgoing messages not yet accepted by the MQTT Session
    #[builder(default = "100")]
    pub outgoing_max: usize,

    /// Default timeout used for network and command operations
    #[builder(default = "Duration::from_secs(10)")]
    pub default_timeout: Duration,

    /// Default message expiry for telemetry messages or default key expiry for State Store keys
    #[builder(default = "Duration::from_secs(10)")]
    pub default_expiry: Duration,
    /// Custom forwarders to be used by the connector
    /// MQTT Telemetry and State Store will always be included in addition to these
    #[builder(default)]
    pub custom_forwarder_managers: Hashmap<String, Box<dyn ForwarderManager>>,
}

impl BaseConnector<TCon>
where
    TCon: Connection + Send + Sync + 'static,
{
  // may return an error
  pub fn new(
      connection: TCon, // this is the endpoint connection, so a RestConnection or SQLConnection
      application_context: ApplicationContext,
      connector_options: ConnectorOptions,
  ) -> Self;

  // will return an error
  pub async fn run(self);
}

pub trait Connection {
    /// establishes connection for a given aep
    fn connect_to_external_endpoint(
        &self,
        aep: String,
    ) -> impl std::future::Future<Output = Result<(), String>> + std::marker::Send;

    /// Given an aep, asset_definition, and dataset, generates a MessageSchema to send to the Schema Registry Client. TODO: should it be an option for the Connection to return None and have this not be sent to SR?
    fn get_dataset_message_schema(
        &self,
        aep: String,
        asset_definition: String,
        dataset_name: String,
        dataset: String,
    ) -> Result<MessageSchema, String>;

    /// Given an aep, asset_definition, and event, generates a MessageSchema to send to the Schema Registry Client. TODO: should it be an option for the Connection to return None and have this not be sent to SR?
    fn get_event_message_schema(
        &self,
        aep: String,
        asset_definition: String,
        event_name: String,
        event: String,
    ) -> Result<MessageSchema, String>;
    
    /// Requests information specified from the dataset from the endpoint
    /// and transforms it to a raw byte format to be sent either as telemetry
    /// or a DSS key value (or others in the future)
    fn sample_dataset(
        &self,
        dataset: String,
    ) -> impl std::future::Future<Output = Result<Vec<u8>, String>> + std::marker::Send;

    /// Returns a receiver that will get transformed data in a raw byte format
    /// to be sent either as telemetry or a DSS key value (or others in the future)
    /// whenever an event has been sent from the endpoint
    fn get_event_receiver(
        &self,
        event: String,
    ) -> impl std::future::Future<Output = Result<UnboundedReceiver<Vec<u8>>, String>> + std::marker::Send;
}

```


Internal Logic pseudo code
```rust
new() {
  get connection settings from file mount
  create session
  create connection monitor for dss
  create Arc'd DSS client
  create SR client
  create Arc'd ADR client
  create destination endpoints (DSS and Mqtt for now)
}
run() {
  tokio::try_join!(session.run(), connector_tasks())
}
connector_tasks() {
  if connector.additional_configuration.contains("leadershipPositionId") {
      le_client = Some(LeaderElection::Client::new(dss_client.clone()));
      // campaign for leader
  }

  let aep_join_set = JoinSet::new();
  aep_creates_observation = observe for AEP creates

  aep_names = adr.get_aep_names()
  for name in aep_names {
    let aep = adr.get_aep(name)
    aep_join_set.spawn(aep_tasks(aep))
  }
  loop {
    tokio::select!(
      recv_result = aep_creates_observation.recv() => {
        if let Some(aep_name) = recv_result {
          let aep = adr.get_aep(aep_name)
          aep_join_set.spawn(aep_tasks(aep))
        }
      },
      result = aep_join_set.join_next() => {
        match result {
          Some(Ok(Ok(endReason))) => {
            match endReason {
              Deleted:
              Updated(aep):
              Error:
              Finished:
            }
          },
          Some(Ok(Err(e)) | Err(e)) => { task was cancelled or panic'd log.},
          None => { all aeps ended, continue? }
        }
      }
     // some LE monitoring
    )
  }
  
  shutdown tasks
}

aep_tasks(aep) -> Result<EndReason, ADRError> {
  let ct = root_cancellation_token.child_token()
    
  let source_endpoint = match self.source_endpoint_factory.create_asset_endpoint_profile_source_endpoint(aep.name) {
    Ok(se) {
      adr.update_aep_status(aep.name, None)
      if let Err(e) = source_endpoint.start().await {
        log(e)
        None
      }
      Some(se)
    },
    Err(e) {
      adr.update_aep_status(aep, error)
      log error
      None // monitor for update/delete notifications, but don't parse assets and all
    }
  };
  
    let aep_delete_observation = adr.observe_aep_delete(aep.name)
    let aep_update_observation = adr.observe_aep_update(aep.name)
    let asset_creates_observation = adr.observe_asset_creates(aep.name)
    
    let assets_join_set = JoinSet::new()
    if source_endpoint.is_some() {
      asset_definition_names = adr.get_asset_names() // on (retried) error, metric log error, return ADRError
      for name in asset_definition_names {
        let asset_definition = adr.get_asset(name) // on (retried) error, metric log error, continue
        assets_join_set.spawn(asset_tasks(asset_definition))
      }
    }
    
    loop {
      tokio::select!(
        () = ct.cancelled() -> return Ok(()); // needed as a way to stop listening for new assets?
        recv_res = asset_creates_observation.recv() {
          if Some(result) = recv_res {
            let asset_definition = adr.get_asset(name) // on (retried) error, metric log error, continue
            assets_join_set.spawn(asset_tasks(asset_definition))
          } else {
            // no more notifications will be received, fatal? return ADRError
            break;
          }
        },
        delete_notification = aep_delete_observation.recv() {
          match delete_notification {
            Some(notification) => {
              ct.cancel()
              adr.unobserve_aep_delete(aep.name)
              adr.unobserve_aep_update(aep.name)
              return EndReason::Deleted()
            },
            None => {
              return EndReason::Failure(Err(()))  // no more observations will be received, fatal error?
            }
          }
        }.
        update_notification = aep_update_observation.recv() {
          match update_notification {
            Some(new_aep) => {
              ct.cancel()
              // unobserves here?
              adr.unobserve_aep_delete(aep.name)
              adr.unobserve_aep_update(aep.name)
              return EndReason::Updated(new_aep)
            },
            None => {
              return EndReason::Failure(Err(())) // no more observations will be received, fatal error?
            }
          }
        }
        result = assets_join_set.join_next() {
          match result {
            Some(Ok(Ok(endReason))) => {
              match endReason {
                Deleted:
                Updated(asset_definition):
                Error:
                Finished:
              }
            },
            Some(Ok(Err(e)) | Err(e)) => { task was cancelled or panic'd log.},
            None => { all assets ended, continue? }
          }
        }
      )
    }
  }

  async asset_tasks(asset_definition) -> EndReason {
    let ct = root_cancellation_token.child_token()
    let asset_delete_observation = adr.observe_asset_delete(asset_definition.name)
    let asset_update_observation = adr.observe_asset_update(asset_definition.name)
    asset_status = init_asset()
    asset_definition = adr.update_asset_status(asset_status)
    loop {
      tokio::select! {
        delete_notification = asset_delete_observation.recv() {
          match delete_notification {
            Some(notification) => {
              ct.cancel()
              adr.unobserve_asset_delete(asset_definition.name)
              adr.unobserve_asset_update(asset_definition.name)
              return EndReason::Deleted()
            },
            None => {
              return EndReason::Failure(Err(()))  // no more observations will be received, fatal error?
            }
          }
        }
        update_notification = asset_update_observation.recv() {
          match update_notification {
            Some(new_asset_definition) => {
              ct.cancel()
              // unobserves here?
              adr.unobserve_asset_delete(asset_definition.name)
              adr.unobserve_asset_update(asset_definition.name)
              return EndReason::Updated(new_asset_definition)
            },
            None => {
              return EndReason::Failure(Err(())) // no more observations will be received, fatal error?
            }
          }
        }
      }
    }
  }
init_asset() -> AssetStatus {
  let asset_status = new()
  let destination_endpoint = destination_endpoints.get(&asset_definition.target) // if error, add to AssetStatus and return
  let asset_forwarder_factory = destination_endpoint.create_asset_forwarder_factory(asset) // if error, add to AssetStatus and return

  for dataset in asset_definition.datasets  {
    // get message schema and send to SR
    if let Some(message_schema) = source_endpoint.get_dataset_message_schema() {
      // if fails on schema, add to AssetStatus errors and continue (to next dataset?)
      // if fails because of network, just metric log and continue as if no message schema was provided by source endpoint
      message_schema_uri = schema_registry_client.put(message_schema)
      asset_status.datasets_schema.push(message_schema)
    }

    let forwarder = asset_forwarder_factory.create_forwarder(dataset) // if error, add to AssetStatus errors and continue to next dataset. Remove from asset_status.datasets_schema?
    source_endpoint.dataset_created_notification(asset_definition.name, &dataset, forwarder, asset_cancellation_token.child_token());
  }
  for event in asset_definition.events  {
    // get message schema and send to SR
    if let Some(message_schema) = source_endpoint.get_event_message_schema() {
      // if fails on schema, add to AssetStatus errors and continue (to next event?)
      // if fails because of network, just metric log and continue as if no message schema was provided by source endpoint
      message_schema_uri = schema_registry_client.put(message_schema)
      asset_status.events_schema.push(message_schema)
    }

    let forwarder = asset_forwarder_factory.create_forwarder(event) // if error, add to AssetStatus errors and continue to next event. Remove from asset_status.events_schema?
    source_endpoint.event_created_notification(asset_definition.name, &event, forwarder, asset_cancellation_token.child_token());
  }
  return asset_status
}
```


<!-- DSS use:
  - used in LE clients at AEP level. Up to one LE per AE, must use same dss client
  - used in forwarders defined at asset level. Will use the shared dss client everywhere, but not known whether needed until asset definition found
### forwarders
## defined in asset
MQ
DSS
Storage
maybe custom in the future

So either the forwarder manager takes in a dss client, which de-generics the inputs, or the forwarders are somehow created at the beginning

// holds all types of forwarders
ForwarderManager:
  - telemetry: probably doesn't contain anything
  - dss: contains arc of dss client
  - storage: probably doesn't contain anything
  - custom: whatever needed

// manages creating "forwarders" for each dataset
AssetForwarderManager:
  - telemetry: contains telemetry sender with default topic. On create_forwarder, either clones default sender or creates a new one
  - dss: contains arc of dss client.  On create_forwarder, clones arc<dss client> and sets key name
  - storage: contains default path.  On create_forwarder, creates one with default path or new path
  - custom: implements new and create_forwarder

// the "unique" forwarder to use for that dataset
Forwarder:
  - telemetry: 1 telemetry sender that sends the telemetry message (has specific topic)
  - dss: arc of dss client that does SET operation (has specific key name)
  - storage: storage file writer (has specific file path)
  - custom: implements forward_message -->


  ```rust
  // Application
  let source_endpoint_factory = SourceEndpointFactory::new();
  let base_connector = BaseConnector::new(source_endpoint_factory);
  base_connector.run().await...

  // Source Endpoint Factory calls
  // on a new AEP
  let source_endpoint = source_endpoint_factory.create_aep_source_endpoint(aep);
  
  // Base Connector to Source Endpoint calls
  // touchpoint to start the endpoint and report access issues
  let result = source_endpoint.start();
    [se] http_client.connect() // and report any errors
  source_endpoint.new_asset_definition(asset_definition, asset);
    [se] // maybe save this information
  source_endpoint.get_dataset_message_schema(dataset_definition); // or source_endpoint.update_dataset_message_schema(dataset_definition, current_message_schema);
    [se] // return message schema
  source_endpoint.new_dataset_definition(dataset);
  source_endpoint.new_event_definition(event);

  // called at their polling frequency. Nothing returned, this is just a notification
  source_endpoint.sample_dataset_notification(dataset);
    [se] // triggers an HTTP request to endpoint. Once that returns, call
    [se] data_transformer.transform(data).await // get puback back
  source_endpoint.sample_datapoint_notification(dataset);

  // Source Endpoint calls



//  struct Asset {
//   name: String,
//   asset_definition: AssetDefinition // maybe
//  }
//  impl Asset {
//   pub fn async forward_dataset_data(payload: SerializedPayload, dataset: Dataset) -> Result<CompletionToken, Err>;
//   pub fn async forward_event_data(payload: SerializedPayload, dataset: Event) -> Result<CompletionToken, Err>;
// }

 struct Dataset {
  name: String,
  dataset_definition: DatasetDefinition // maybe
 }
 impl Dataset {
  pub fn async forward_data(payload: SerializedPayload) -> Result<CompletionToken, Err>;
}

  ```

