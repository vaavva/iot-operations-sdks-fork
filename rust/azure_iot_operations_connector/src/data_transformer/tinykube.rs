// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use std::{
    cell::RefCell,
    collections::BTreeMap,
    env,
    error::Error,
    rc::Rc,
    sync::{Arc, Mutex},
};

use azure_iot_operations_mqtt::MqttConnectionSettings;
use azure_iot_operations_protocol::{
    application::ApplicationContext,
    common::payload_serialize::{FormatIndicator, SerializedPayload},
};
use tinykube_hostlib::{
    AppConfig, WasmComponentInfo, WasmConfig, WasmDeviceManager,
    common::mqtt_connection::MqttConnectionConfig,
    graph::{
        Dataflow, GraphBuilder, GraphMessage, InputHandle, MessageMetadata, ProbeHandle,
        ProcessedAck, WasmOperatorProvider, Worker,
    },
    host_type::{BufferOrBytes, DataModel, HybridLogicalClock, ObservabilityContext},
};
use tokio::{
    sync::{mpsc, oneshot},
    task,
};

use crate::destination_endpoint::Forwarder;

mod copy;
mod leak;
mod wrap;

mod ack {
    use std::result;
    use tokio::sync::mpsc;

    pub type Result = result::Result<(), String>;
    pub type Sender = mpsc::Sender<Result>;
    pub type Receiver = mpsc::Receiver<Result>;

    pub fn new() -> (Sender, Receiver) {
        mpsc::channel(1)
    }
}

pub struct TinyKubeDataTransformer {
    application_context: ApplicationContext,
    mqtt_connection_settings: MqttConnectionSettings,
    source_sender: mpsc::UnboundedSender<(Vec<u8>, ack::Sender)>,
    source_receiver: Mutex<mpsc::UnboundedReceiver<(Vec<u8>, ack::Sender)>>,
    forwarder: Forwarder,
}

impl TinyKubeDataTransformer {
    pub fn new(
        application_context: ApplicationContext,
        mqtt_connection_settings: MqttConnectionSettings,
        forwarder: Forwarder,
    ) -> Self {
        let (source_sender, source_receiver) = mpsc::unbounded_channel();
        Self {
            application_context,
            mqtt_connection_settings,
            source_sender,
            source_receiver: Mutex::new(source_receiver),
            forwarder,
        }
    }

    pub async fn start(&self) -> Result<(), Box<dyn Error>> {
        let wasm_modules_path = env::var("TK_WASM_MODULES_PATH")?;

        let device_manager = Arc::new(leak::Mutex::new(WasmDeviceManager::new(AppConfig {
            wasm_config: WasmConfig {
                // Where does this come from?
                wasm_controller_id: "30657190-2dea-42dd-a438-0b77483446ae".to_owned(),
                rpc_request_timeout_s: 10,

                // I don't think any of these values are actually meaningful, but the TK registration requires them.
                wasm_runtime_type: "wasmtime".to_owned(),
                wasm_runtime_version: "unknown".to_owned(),
                install_type: "onhost".to_owned(),
                entity_type: "pod".to_owned(),

                wasm_modules_path: wasm_modules_path.clone(),
                modules_to_subscribe: copy::modules_to_subscribe(cfg.graph),

                ..Default::default()
            },

            mqtt_config_for_rpc: MqttConnectionConfig {
                ..Default::default()
            },

            mqtt_config_for_telemetry: None,
        })?));

        let component_info = Arc::new(leak::Mutex::new(WasmComponentInfo::new(
            leak::Config::default().wasm_component_model(true),
            Box::new(wrap::StateStoreClientWrapper::build(
                self.application_context,
                self.mqtt_connection_settings,
            )?),
            ObservabilityContext::default(),
            None,
        )));

        let (sink_sender, sink_receiver) = leak::unbounded_channel();

        let (module_names, graph_config) = copy::make_graph_config(cfg.graph);
        GraphBuilder::new(WasmOperatorProvider::new(
            component_info.clone(),
            sink_sender,
            module_names,
        ))
        .spawn_thread(graph_config, *self);

        tokio::join!(
            WasmDeviceManager::start(device_manager, component_info),
            self.forward_result(sink_receiver),
        );

        Ok(())
    }

    pub fn add_sampled_data(
        &self,
        data: Vec<u8>,
    ) -> Result<impl Future<Output = ack::Result>, Box<dyn Error>> {
        let (send, mut recv) = ack::new();
        self.source_sender.send((data, send))?;
        Ok(async move { recv.recv().await.unwrap_or(Ok(())) })
    }

    async fn forward_result(
        &self,
        mut sink_receiver: leak::UnboundedReceiver<ProcessedAck<Metadata>>,
    ) {
        while let Some(result) = sink_receiver.recv().await {
            match result {
                ProcessedAck::SinkMessage(msg) => {
                    let (send, recv) = oneshot::channel();
                    self.forwarder.send_data(
                        SerializedPayload {
                            payload: msg.payload.into(),
                            content_type: msg.content_type,
                            format_indicator: FormatIndicator::UnspecifiedBytes,
                        },
                        send,
                    );
                    task::spawn(async {
                        msg.metadata
                            .send(recv.await.unwrap_or_else(|err| Err(err.to_string())))
                    });
                }
                ProcessedAck::DropSinkMessage(metadata, _) => metadata.send(Ok(())),
            }
        }
    }
}

// timely::execute can spin up multiple workers, but GraphBuilder::spawn_thread only configures one, so the immutability
// of self in drive_graph is potentially a leaky abstraction. Given this, it is safe to lock the source_receiver and use
// tokio channels, since there will only ever be one instance.
impl Dataflow<Rc<GraphMessage<Metadata>>, HybridLogicalClock> for TinyKubeDataTransformer {
    fn drive_graph(
        &self,
        _: &mut impl Worker,
        input_handles: BTreeMap<
            String,
            impl InputHandle<HybridLogicalClock, Rc<GraphMessage<Metadata>>>,
        >,
        _: impl ProbeHandle<HybridLogicalClock>,
    ) {
        if let Some(mut input_handle) = input_handles.into_values().next() {
            let mut source_receiver = self.source_receiver.lock().unwrap();
            while let Some(data) = source_receiver.blocking_recv() {
                input_handle.send(Rc::new(GraphMessage {
                    metadata: Metadata::new(data.1),
                    inner: DataModel::BufferOrBytes(BufferOrBytes::Bytes(data.0)),
                    error: RefCell::new(None),
                }));
            }
        }
    }
}

#[derive(Clone, Default)]
struct Metadata(Vec<ack::Sender>);

impl Metadata {
    fn new(ack: ack::Sender) -> Self {
        Metadata(vec![ack])
    }

    fn send(self, result: ack::Result) {
        for ack in self.0 {
            ack.send(result.clone());
        }
    }
}

impl MessageMetadata for Metadata {
    fn extend(&mut self, one: Self) {
        self.0.append(&mut one.0.clone());
    }
}
