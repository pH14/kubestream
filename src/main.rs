use std::time::Duration;
use kube::{Api, Client, Discovery, ResourceExt};
use kube::api::{DynamicObject, GroupVersionKind, ListParams};
use kube::runtime::watcher;
use tokio::main;
use futures::{stream, StreamExt, TryStreamExt};
use k8s_openapi::api::core::v1::Node;
use k8s_openapi::{api_version, serde_json};
use kube::core::discovery;
use kube::discovery::{Scope, verbs};
use kube::runtime::utils::try_flatten_applied;
use rdkafka::ClientConfig;
use rdkafka::message::OwnedHeaders;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use crate::discovery::ApiResource;
use crate::watcher::Error;

#[main]
async fn main() -> anyhow::Result<()> {
    println!("Hello, world!");
    let client = Client::try_default().await?;
    let discovery = Discovery::new(client.clone()).run().await?;
    let mut watchers = vec![];

    // TODO: a watcher for any new Kinds that get added in after this starts
    for api_group in discovery.groups() {
        for (api_resource, capabilities) in api_group.recommended_resources() {
            if capabilities.supports_operation(verbs::WATCH) && capabilities.supports_operation(verbs::LIST) {
                println!("Setting up watch for: {:?}-{:?}", api_group.name(), api_resource);
            } else {
                println!("cannot watch/list {:?}-{:?}", api_group.name(), api_resource);
                continue;
            }

            let api: Api<DynamicObject> = Api::all_with(client.clone(), &api_resource);

            let watcher = watcher(api, ListParams::default());

            let x = try_flatten_applied(watcher).zip(stream::repeat(api_resource))
                .map(|(w, ar)| w.map(|inner| (inner, ar)));
            watchers.push(x.boxed());
        }
    }

    let producer : FutureProducer= ClientConfig::new()
        .set("bootstrap.servers", "127.0.0.1:61053")
        .create()
        .expect("kafka producer creation error");


    let mut all_resources = stream::select_all(watchers);
    while let kube_resource = all_resources.try_next().await {
        match kube_resource {
            Ok(Some((result, resource))) => {
                if let Ok(json) = serde_json::to_string(&result) {
                    println!("Got: {} - {:?}", json, resource);
                    let inner_produce = producer.clone();
                    let headers = OwnedHeaders::new()
                        .add("kube_kind", &resource.kind)
                        .add("kube_group", &resource.group)
                        .add("kube_version", &resource.version)
                        .add("kube_plural", &resource.plural);
                    tokio::spawn(async move {
                        let key = format!("{}-{}-{}-{}-{}", resource.api_version, resource.kind, resource.plural, result.namespace().unwrap_or("".to_string()), result.name());
                        let future = inner_produce.send(FutureRecord::to("kubecdc")
                                                            .key(&key)
                                                            .headers(headers)
                                                            .payload(&json),
                                                        Timeout::Never);
                        match future.await {
                            Ok(delivery) => println!("Sent: {:?}", delivery),
                            Err((e, _)) => println!("Error: {:?}", e),
                        }
                    });
                } else {
                    println!("Unable to deserialize {:?} of type {:?}", result.metadata.name, resource);
                }
            }
            Err(err) => {
                println!("Error! {}", err);
            }
            _ => {
                panic!("unexpected return");
            }
        }
    }

    Ok(())
}
