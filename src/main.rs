use anyhow::{anyhow, bail, Context};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use futures::future::try_join_all;
use futures::stream::BoxStream;
use futures::{FutureExt, StreamExt, TryStreamExt};
use k8s_openapi::serde_json;
use kube::api::{DynamicObject, ListParams};
use kube::core::discovery;
use kube::discovery::verbs;
use kube::runtime::watcher;
use kube::{Api, Client, Discovery, ResourceExt};

use rdkafka::message::OwnedHeaders;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use rdkafka::ClientConfig;

use tokio::{main, select};
use tracing::{error, info, trace, Level};

use crate::discovery::ApiResource;
use crate::watcher::Event;

#[derive(Debug, clap::Parser)]
pub struct Args {
    /// The Kafka host
    #[clap(long, default_value = "localhost")]
    pub kafka_host: String,

    /// The Kafka port
    #[clap(long, default_value = "58979")]
    pub kafka_port: u16,

    /// The Kafka topic to publish Kube events into
    #[clap(long, default_value = "kubestream")]
    pub kafka_topic: String,

    #[clap(long)]
    pub verbose: bool,
}

#[main]
pub async fn main() -> anyhow::Result<()> {
    let config: Arc<Args> = Arc::new(Args::parse());

    tracing_subscriber::fmt()
        .with_max_level(if config.verbose {
            Level::TRACE
        } else {
            Level::INFO
        })
        .with_level(true)
        .init();

    let kube_client = Client::try_default().await?;

    let kafka_producer: FutureProducer = ClientConfig::new()
        .set(
            "bootstrap.servers",
            format!("{}:{}", config.kafka_host, config.kafka_port),
        )
        .set("linger.ms", "100")
        .set("message.timeout.ms", "5000")
        .set("delivery.timeout.ms", "15000")
        .create()
        .expect("kafka producer creation error");

    let start_and_refresh_watchers = async {
        let mut interval = tokio::time::interval(Duration::from_secs(10));
        let mut watchers_map = HashMap::new();

        loop {
            let discovery = Discovery::new(kube_client.clone()).run().await?;

            let existing_api_resources: HashSet<ApiResource> =
                watchers_map.keys().cloned().collect();
            let mut latest_api_resources = HashSet::new();

            for api_group in discovery.groups() {
                for (api_resource, capabilities) in api_group.recommended_resources() {
                    if !(capabilities.supports_operation(verbs::WATCH)
                        && capabilities.supports_operation(verbs::LIST))
                    {
                        trace!(
                            "Cannot Watch/List {:?}-{:?}. Does not support WATCH && LIST",
                            api_group.name(),
                            api_resource
                        );
                        continue;
                    }

                    latest_api_resources.insert(api_resource.clone());

                    if watchers_map.contains_key(&api_resource) {
                        continue;
                    }

                    let api: Api<DynamicObject> = Api::all_with(kube_client.clone(), &api_resource);
                    let watcher = watcher(api, ListParams::default()).boxed();
                    let error_message = format!("watch failure for resource: {:?}", api_resource);

                    let join_handle = tokio::spawn(watcher_to_kafka(
                        Arc::clone(&config),
                        kafka_producer.clone(),
                        api_resource.clone(),
                        watcher,
                    ))
                    .map(move |a| a.map(|e| e.context(error_message)))
                    .fuse();

                    watchers_map.insert(api_resource, join_handle);
                }
            }

            for deleted_api_resource in existing_api_resources.difference(&latest_api_resources) {
                match watchers_map.remove(deleted_api_resource) {
                    None => {
                        trace!(
                            "Tried to remove non-existent task for API resource {:?}",
                            deleted_api_resource
                        );
                    }
                    Some(join_handle) => {
                        trace!(
                            "{:?} has been removed, dropping watch task",
                            deleted_api_resource
                        );
                        drop(join_handle);
                    }
                }
            }

            select! {
                 results = try_join_all(watchers_map.values_mut()) => {
                    match results {
                        Ok(results) => {
                            for result in results {
                                if let Err(e) = result {
                                    error!("Watcher error: {}", e);
                                    return Err(anyhow!(e));
                                }
                            }
                        },
                        Err(e) => {
                            return Err(anyhow!(e));
                        }
                    }
                }
                 _ = interval.tick() => {
                   trace!("recomputing watchers...");
                }
            }
        }

        #[allow(unreachable_code)]
        Ok::<(), anyhow::Error>(())
    };

    start_and_refresh_watchers.await?;

    Ok(())
}

async fn watcher_to_kafka(
    config: Arc<Args>,
    kafka_producer: FutureProducer,
    resource: ApiResource,
    mut stream: BoxStream<'_, watcher::Result<Event<DynamicObject>>>,
) -> Result<(), anyhow::Error> {
    let kafka_topic = &config.kafka_topic;
    info!("Starting watcher to {:?}", resource);

    loop {
        trace!("Awaiting next on {:?}", resource);
        let kube_event = stream.try_next().await;

        match kube_event {
            Ok(Some(Event::Applied(event))) => {
                let json = serde_json::to_string(&event).unwrap_or_else(|e| {
                    panic!(
                        "unable to deserialize Kube event: {} for event {:?}",
                        e, event
                    )
                });
                let key = get_kafka_key(&event, &resource);
                trace!("Sending {:?} of type {:?}", event, resource);
                let future = kafka_producer.send(
                    FutureRecord::to(kafka_topic)
                        .key(&key)
                        .headers(get_kafka_headers(&resource))
                        .payload(&json),
                    Timeout::After(Duration::from_secs(10)),
                );
                match future.await {
                    Ok(delivery) => trace!("Sent applied resource: {:?}", delivery),
                    Err((e, _)) => return Err(e.into()),
                }
            }
            Ok(Some(Event::Deleted(event))) => {
                let key = get_kafka_key(&event, &resource);
                let record: FutureRecord<String, String> = FutureRecord::to(kafka_topic).key(&key);

                let future = kafka_producer.send(record, Timeout::After(Duration::from_secs(10)));
                match future.await {
                    Ok(delivery) => trace!("Sent deletion: {:?}", delivery),
                    Err((e, _)) => return Err(e.into()),
                }
            }
            Ok(Some(Event::Restarted(events))) => {
                trace!(
                    "Restarted {:?}. Has {} known resources",
                    &resource,
                    events.len()
                );

                let events: Vec<_> = events
                    .iter()
                    .map(|event| {
                        let key = get_kafka_key(&event, &resource);
                        let json = serde_json::to_string(&event).unwrap_or_else(|e| {
                            panic!(
                                "unable to deserialize Kube event: {} for event {:?}",
                                e, event
                            )
                        });
                        (key, json)
                    })
                    .collect();

                let mut futures = Vec::with_capacity(events.len());
                for (key, json) in &events {
                    futures.push(
                        kafka_producer.send(
                            FutureRecord::to(kafka_topic)
                                .key(key)
                                .headers(get_kafka_headers(&resource))
                                .payload(json),
                            Timeout::After(Duration::from_secs(10)),
                        ),
                    );
                }

                match try_join_all(futures).await {
                    Ok(delivery) => {
                        trace!("Sent refresh update: {:?} for {:?}", delivery, &resource)
                    }
                    Err((e, _)) => return Err(e.into()),
                }
            }
            Err(err) => {
                bail!(err)
            }
            _ => {
                panic!("unreachable");
            }
        }
    }
}

fn get_kafka_key(object: &DynamicObject, resource: &ApiResource) -> String {
    format!(
        "{}-{}-{}-{}-{}",
        resource.api_version,
        resource.kind,
        resource.plural,
        object.namespace().unwrap_or_else(|| "".to_string()),
        object.name()
    )
}

// really lazy way to pass in group-version-kind information without modifying the Kube payload.
// this should prolly be in some outer envelope, rather than passed separately as headers
fn get_kafka_headers(resource: &ApiResource) -> OwnedHeaders {
    OwnedHeaders::new()
        .add("kube_kind", &resource.kind)
        .add("kube_group", &resource.group)
        .add("kube_version", &resource.version)
        .add("kube_plural", &resource.plural)
}
