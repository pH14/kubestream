use crate::discovery::ApiResource;
use crate::watcher::{Error, Event};
use futures::{stream, StreamExt, TryStreamExt};
use k8s_openapi::api::core::v1::Node;
use k8s_openapi::{api_version, serde_json};
use kube::api::{DynamicObject, GroupVersionKind, ListParams};
use kube::core::discovery;
use kube::discovery::{verbs, Scope};
use kube::runtime::utils::try_flatten_applied;
use kube::runtime::{reflector, watcher};
use kube::{Api, Client, Discovery, ResourceExt};
use rdkafka::message::OwnedHeaders;
use rdkafka::producer::future_producer::OwnedDeliveryResult;
use rdkafka::producer::{BaseRecord, FutureProducer, FutureRecord, Producer};
use rdkafka::util::Timeout;
use rdkafka::ClientConfig;
use std::future::Future;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::main;

#[main]
async fn main() -> anyhow::Result<()> {
    let client = Client::try_default().await?;
    let discovery = Discovery::new(client.clone()).run().await?;
    let mut watchers = vec![];

    // TODO: a watcher for any new Kinds that get added in after this starts
    for api_group in discovery.groups() {
        for (api_resource, capabilities) in api_group.recommended_resources() {
            if capabilities.supports_operation(verbs::WATCH)
                && capabilities.supports_operation(verbs::LIST)
            {
                println!(
                    "Setting up watch for: {:?}-{:?}",
                    api_group.name(),
                    api_resource
                );
            } else {
                println!(
                    "cannot watch/list {:?}-{:?}",
                    api_group.name(),
                    api_resource
                );
                continue;
            }

            let api: Api<DynamicObject> = Api::all_with(client.clone(), &api_resource);
            let watcher = watcher(api, ListParams::default());

            let x = watcher
                .zip(stream::repeat(api_resource))
                .map(|(w, ar)| w.map(|inner| (inner, ar)));
            watchers.push(x.boxed());
        }
    }

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", "127.0.0.1:61053")
        .create()
        .expect("kafka producer creation error");

    let mut all_resources = stream::select_all(watchers);
    // TODO: we'd want monotonic time here
    let mut timestamp_epoch = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();

    while let kube_resource = all_resources.try_next().await {
        let inner_produce = producer.clone();
        match kube_resource {
            Ok(Some((Event::Applied(result), resource))) => {
                if let Ok(json) = serde_json::to_string(&result) {
                    println!("Applied: {} - {:?}", json, resource);
                    let key = get_kafka_key(&result, &resource);
                    tokio::spawn(async move {
                        let future = inner_produce.send(
                            FutureRecord::to("kubecdc")
                                .key(&key)
                                .headers(get_kafka_headers(timestamp_epoch, &resource))
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
                    let future = inner_produce.send(record, Timeout::Never);
                    match future.await {
                        Ok(delivery) => println!("Sent deletion: {:?}", delivery),
                        Err((e, _)) => println!("Error deleting: {:?}", e),
                    }
                });
            }
            Ok(Some((Event::Restarted(replacements), resource))) => {
                // Requires atomically repopulating the downstream
                // can use a timestamp as a best effort, and let the
                // materialized views filter on the greatest timestamp
                // TODO: Restarted gets called on initial population so it's not just for errors later...
                timestamp_epoch = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis();
                println!("Restarted {:?}", resource);

                tokio::spawn(async move {
                    // TODO: error handling the transaction BEGIN/COMMIT points
                    inner_produce.begin_transaction();
                    for result in replacements {
                        let key = get_kafka_key(&result, &resource);
                        if let Ok(json) = serde_json::to_string(&result) {
                            let future = inner_produce.send(
                                FutureRecord::to("kubecdc")
                                    .key(&key)
                                    .headers(get_kafka_headers(timestamp_epoch, &resource))
                                    .payload(&json),
                                Timeout::Never,
                            );
                            match future.await {
                                Ok(delivery) => println!("Sent: {:?}", delivery),
                                Err((e, _)) => println!("Error: {:?}", e),
                            }
                        }
                    }
                    inner_produce.commit_transaction(Duration::from_secs(30));
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

    Ok(())
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

fn get_kafka_headers(timestamp_epoch: u128, resource: &ApiResource) -> OwnedHeaders {
    OwnedHeaders::new()
        .add("kube_kind", &resource.kind)
        .add("kube_group", &resource.group)
        .add("kube_version", &resource.version)
        .add("kube_plural", &resource.plural)
        .add("timestamp_epoch", &timestamp_epoch.to_string())
}
