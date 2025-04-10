use std::time::Duration;

use tokio::sync::mpsc::UnboundedReceiver;

pub struct ADRClient {}
impl Default for ADRClient {
    fn default() -> Self {
        Self::new()
    }
}

#[allow(clippy::unused_async)]
impl ADRClient {
    #[must_use]
    pub fn new() -> Self {
        unimplemented!()
    }
    pub async fn observe_asset_endpoint_profile_creates(
        &self,
    ) -> Result<UnboundedReceiver<AssetEndpointProfile>, String> {
        unimplemented!()
    }

    pub async fn observe_asset_endpoint_profile_updates(
        &self,
        aep_name: String,
    ) -> Result<UnboundedReceiver<AssetEndpointProfile>, String> {
        unimplemented!()
    }

    pub async fn observe_asset_endpoint_profile_deletes(
        &self,
        aep_name: String,
    ) -> Result<UnboundedReceiver<String>, String> {
        unimplemented!()
    }

    pub async fn get_asset_endpoint_profiles(&self) -> Result<Vec<String>, String> {
        unimplemented!()
    }

    pub async fn get_asset_endpoint_profile(
        &self,
        _name: String,
    ) -> Result<AssetEndpointProfile, String> {
        unimplemented!()
    }

    pub async fn observe_asset_definition_creates(
        &self,
    ) -> Result<UnboundedReceiver<AssetDefinition>, String> {
        unimplemented!()
    }

    pub async fn observe_asset_definition_updates(
        &self,
        _name: String,
    ) -> Result<UnboundedReceiver<AssetDefinition>, String> {
        unimplemented!()
    }

    pub async fn observe_asset_definition_deletes(
        &self,
        _name: String,
    ) -> Result<UnboundedReceiver<String>, String> {
        unimplemented!()
    }

    pub async fn get_asset_names(&self) -> Result<Vec<String>, String> {
        unimplemented!()
    }

    pub async fn get_asset_definition(&self, _name: String) -> Result<AssetDefinition, String> {
        unimplemented!()
    }

    pub async fn update_aep_status(
        &self,
        _name: String,
        _status: Option<AssetEndpointProfileStatus>,
    ) -> Result<AssetEndpointProfile, String> {
        unimplemented!()
    }

    pub async fn update_asset_status(
        &self,
        _name: String,
        _status: AssetStatus,
    ) -> Result<AssetDefinition, String> {
        unimplemented!()
    }
}

#[derive(Clone)]
pub struct AssetEndpointProfile {
    pub name: String,
    pub additional_configuration: Vec<String>,
}

#[derive(Clone)]
pub struct AssetDefinition {
    pub name: String,
    pub target: String,
    pub frequency: Duration,
    pub datasets: Vec<Dataset>,
    pub events: Vec<Event>,
}

#[derive(Clone)]
pub struct Dataset {
    pub name: String,
}

#[derive(Clone)]
pub struct Event {
    pub name: String,
}

pub enum Notification {
    Update(String),
    Delete(String),
}

pub struct AssetEndpointProfileStatus {
    pub errors: Vec<ADRError>,
}

#[derive(Clone)]
pub struct AssetStatus {
    pub datasets_schema: Option<Vec<DatasetsSchema>>,
    pub events_schema: Option<Vec<EventsSchema>>,
    pub errors: Option<Vec<ADRError>>,
    pub version: Option<i32>,
}

impl AssetStatus {
    pub fn add_dataset_schema(&mut self, dataset_schema: DatasetsSchema) {
        if let Some(datasets_schema) = &mut self.datasets_schema {
            datasets_schema.push(dataset_schema);
        } else {
            self.datasets_schema = Some(vec![dataset_schema]);
        }
    }
    pub fn remove_dataset_schema(&mut self, dataset_name: String) {
        if let Some(datasets_schema) = &mut self.datasets_schema {
            datasets_schema.retain(|ds| ds.name != dataset_name);
        }
    }
    pub fn add_events_schema(&mut self, events_schema: EventsSchema) {
        if let Some(events_schemas) = &mut self.events_schema {
            events_schemas.push(events_schema);
        } else {
            self.events_schema = Some(vec![events_schema]);
        }
    }
    pub fn remove_events_schema(&mut self, event_name: String) {
        if let Some(events_schemas) = &mut self.events_schema {
            events_schemas.retain(|es| es.name != event_name);
        }
    }
    pub fn add_error(&mut self, error: ADRError) {
        if let Some(errors) = &mut self.errors {
            errors.push(error);
        } else {
            self.errors = Some(vec![error]);
        }
    }
}

#[derive(Clone)]
pub struct DatasetsSchema {
    pub name: String,
    pub message_schema_reference: MessageSchemaReference,
}

#[derive(Clone)]
pub struct EventsSchema {
    pub name: String,
    pub message_schema_reference: MessageSchemaReference,
}

#[derive(Clone)]
pub struct MessageSchemaReference {
    pub name: Option<String>,
    pub namespace: Option<String>,
    pub version: Option<String>,
}

#[derive(Clone, Debug)]
pub struct ADRError {
    pub code: u16,
    pub message: String,
}
