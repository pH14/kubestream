use std::time::Duration;

use futures::{stream, StreamExt, TryStreamExt};
use k8s_openapi::serde_json;
use kube::{Api, Client, Discovery, ResourceExt};
use kube::api::{DynamicObject, ListParams};
use kube::core::discovery;
use kube::discovery::verbs;
use kube::runtime::watcher;
use rdkafka::ClientConfig;
use rdkafka::message::OwnedHeaders;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use rdkafka::util::Timeout;
use tokio::main;

use crate::discovery::ApiResource;
use crate::watcher::Event;

#[main]
async fn main() -> anyhow::Result<()> {
    let client = Client::try_default().await?;
    let discovery = Discovery::new(client.clone()).run().await?;

    // TODO: a watcher for any new Kinds that get added in after this starts
    let mut watchers = vec![];
    for api_group in discovery.groups() {
        for (api_resource, capabilities) in api_group.recommended_resources() {
            if capabilities.supports_operation(verbs::WATCH)
                && capabilities.supports_operation(verbs::LIST)
            {
                println!(
                    "Watching: {:?}-{:?}",
                    api_group.name(),
                    api_resource
                );
            } else {
                println!(
                    "Cannot watch/list {:?}-{:?}",
                    api_group.name(),
                    api_resource
                );
                continue;
            }

            let api: Api<DynamicObject> = Api::all_with(client.clone(), &api_resource);

            // Kube events don't always contain the full identifying group-version-kind, so we zip
            // the events with them to ensure the reader always knows what resources it's handling
            watchers.push(watcher(api, ListParams::default())
                .zip(stream::repeat(api_resource))
                .map(|(w, ar)| w.map(|inner| (inner, ar))).boxed());
        }
    }

    // TODO: command line args for Kafka connection
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", "127.0.0.1:61053")
        .create()
        .expect("kafka producer creation error");

    let mut all_resources = stream::select_all(watchers);

    loop {
        let kube_resource = all_resources.try_next().await;

        let kafka_producer = producer.clone();
        match kube_resource {
            Ok(Some((Event::Applied(result), resource))) => {
                if let Ok(json) = serde_json::to_string(&result) {
                    println!("Applied: {} - {:?}", json, resource);
                    let key = get_kafka_key(&result, &resource);
                    // do not take this async code for inspiration. don't know what I'm doing
                    tokio::spawn(async move {
                        let future = kafka_producer.send(
                            // TODO: command line args for Kafka topic
                            FutureRecord::to("kubecdc")
                                .key(&key)
                                .headers(get_kafka_headers(&resource))
                                .payload(&json),
                            Timeout::Never,
                        );
                        match future.await {
                            Ok(delivery) => println!("Sent: {:?}", delivery),
                            Err((e, _)) => println!("Error: {:?}", e),
                        }
                    });
                } else {
                    println!(
                        "Unable to deserialize {:?} of type {:?}",
                        result.metadata.name, resource
                    );
                }
            }
            Ok(Some((Event::Deleted(result), resource))) => {
                let key = get_kafka_key(&result, &resource);
                println!("Deleted: {}", key);
                tokio::spawn(async move {
                    let record: FutureRecord<String, String> =
                        FutureRecord::to("kubecdc").key(&key);
                    let future = kafka_producer.send(record, Timeout::Never);
                    match future.await {
                        Ok(delivery) => println!("Sent deletion: {:?}", delivery),
                        Err((e, _)) => println!("Error deleting: {:?}", e),
                    }
                });
            }
            Ok(Some((Event::Restarted(replacements), resource))) => {
                println!("Restarted {:?}", resource);
                // use a Kafka transaction to atomically write in the whole updated batch of records
                tokio::spawn(async move {
                    // TODO: error handling the transaction BEGIN/COMMIT points
                    kafka_producer.begin_transaction();
                    for result in replacements {
                        let key = get_kafka_key(&result, &resource);
                        if let Ok(json) = serde_json::to_string(&result) {
                            let future = kafka_producer.send(
                                FutureRecord::to("kubecdc")
                                    .key(&key)
                                    .headers(get_kafka_headers(&resource))
                                    .payload(&json),
                                Timeout::Never,
                            );
                            match future.await {
                                Ok(delivery) => println!("Sent: {:?}", delivery),
                                Err((e, _)) => println!("Error: {:?}", e),
                            }
                        }
                    }
                    kafka_producer.commit_transaction(Duration::from_secs(30));
                });
            }
            Err(err) => {
                println!("Error! {:?}", err);
            }
            _ => {
                panic!("unexpected return");
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
        object.namespace().unwrap_or("".to_string()),
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
