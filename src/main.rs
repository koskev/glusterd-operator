use std::{collections::BTreeMap, error::Error, sync::Arc, time::Duration};

use futures::{pin_mut, StreamExt, TryStreamExt};
use k8s_openapi::{
    api::{
        apps::v1::{Deployment, StatefulSet, StatefulSetSpec},
        core::v1::{
            Container, HostPathVolumeSource, Node, Pod, PodSpec, PodTemplateSpec,
            ResourceRequirements, SecurityContext, Service, ServicePort, ServiceSpec, Volume,
            VolumeMount,
        },
    },
    apimachinery::pkg::{api::resource::Quantity, apis::meta::v1::LabelSelector},
};
use kube::{
    api::{Api, AttachParams, DeleteParams, ListParams, Patch, PatchParams, PostParams},
    core::ObjectMeta,
    runtime::{
        controller::Action,
        wait::{await_condition, Condition},
        watcher, Controller, WatchStreamExt,
    },
    Client, ResourceExt,
};

use kube::core::{CustomResourceExt, Resource};
use kube_derive::CustomResource;
use log::LevelFilter;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use simplelog::{ColorChoice, TermLogger, TerminalMode};

use log::info;

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
struct GlusterdStorageNodeSpec {
    name: String,
    path: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
enum GlusterdStorageTypeSpec {
    Dispersed,
    Replica3,
}

#[derive(CustomResource, Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[kube(
    group = "glusterd-operator.storage",
    version = "v1",
    kind = "GlusterdStorage",
    namespaced
)]
struct GlusterdStorageSpec {
    r#type: GlusterdStorageTypeSpec,
    nodes: Vec<GlusterdStorageNodeSpec>,
}

#[derive(thiserror::Error, Debug)]
pub enum MyError {}

pub type Result<T, E = MyError> = std::result::Result<T, E>;

fn is_statefulset_running() -> impl Condition<StatefulSet> {
    |obj: Option<&StatefulSet>| {
        if let Some(dep) = &obj {
            if let Some(status) = &dep.status {
                return status.ready_replicas.unwrap_or(0) == status.replicas;
            }
        }
        false
    }
}

fn handle_glusterdstorage_event(event: GlusterdStorage) {
    info!("New GlusterdStorage with spec {:?}", event.spec)
}

fn get_id(object: &GlusterdStorage, node_name: &str) -> String {
    format!("{}-{}", object.metadata.name.clone().unwrap(), node_name)
}

fn get_label(id: &str) -> String {
    format!("glusterd-{}", id)
}

// This has to be a stateful set to ensure that the data is only ever written by one instance!
fn get_statefulset(namespace: &str, id: &str, node_name: &str) -> StatefulSet {
    let label_str = get_label(&id);
    let label = BTreeMap::from([("app".to_string(), label_str)]);
    StatefulSet {
        metadata: ObjectMeta {
            name: Some(format!("glusterd-{}", id)),
            namespace: Some(namespace.to_string()),
            labels: Some(label.clone()),

            ..Default::default()
        },
        spec: Some(StatefulSetSpec {
            selector: LabelSelector {
                match_labels: Some(label.clone()),
                ..Default::default()
            },
            template: PodTemplateSpec {
                metadata: Some(ObjectMeta {
                    labels: Some(label.clone()),
                    ..Default::default()
                }),
                spec: Some(PodSpec {
                    node_selector: Some(BTreeMap::from([(
                        "kubernetes.io/hostname".to_string(),
                        node_name.to_string(),
                    )])),
                    containers: vec![Container {
                        name: "glusterd".to_string(),
                        // TODO: allow external image and tag for renovate
                        image: Some("ghcr.io/koskev/glusterfs-image:2023.11.25".to_string()),
                        args: Some(vec![
                            "-l".to_string(),
                            "/dev/stdout".to_string(),
                            "-L".to_string(),
                            "DEBUG".to_string(),
                            "-N".to_string(),
                        ]),

                        volume_mounts: Some(vec![VolumeMount {
                            mount_path: "/var/lib/glusterd".to_string(),
                            name: "glusterd-config".to_string(),
                            ..Default::default()
                        }]),
                        resources: Some(ResourceRequirements {
                            requests: Some(BTreeMap::from([(
                                "memory".to_string(),
                                Quantity("256Mi".to_string()),
                            )])),
                            ..Default::default()
                        }),
                        security_context: Some(SecurityContext {
                            privileged: Some(true),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }],
                    volumes: Some(vec![Volume {
                        name: "glusterd-config".to_string(),
                        host_path: Some(HostPathVolumeSource {
                            path: "/var/lib/k8s-glusterd".to_string(),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }]),
                    ..Default::default()
                }),
            },
            ..Default::default()
        }),
        ..Default::default()
    }
}

async fn reconcile(obj: Arc<GlusterdStorage>, ctx: Arc<Client>) -> Result<Action> {
    info!("reconcile request: {}", obj.name_any());
    // TODO: check if file changed
    let client = ctx.as_ref().clone();
    // Start deployments for each node
    // TODO: support changing nodes
    let namespace = obj
        .metadata
        .namespace
        .clone()
        .unwrap_or("default".to_string());

    let mut deployments = vec![];
    let mut services = vec![];
    let statefulset_api = Api::<StatefulSet>::namespaced(client.clone(), &namespace);
    let service_api = Api::<Service>::namespaced(client.clone(), &namespace);

    for node in &obj.spec.nodes {
        let id = get_id(obj.as_ref(), &node.name);
        let label_str = get_label(&id);
        let label = BTreeMap::from([("app".to_string(), label_str)]);

        let dep = get_statefulset(&namespace, &id, &node.name);

        // TODO: just patch instead of deleting?
        //let _ = statefulset_api
        //    .delete(
        //        &dep.metadata.name.clone().unwrap(),
        //        &DeleteParams::default(),
        //    )
        //    .await;

        let patch = Patch::Apply(dep);
        let patch_result = statefulset_api
            .patch(
                &dep.metadata.name.clone().unwrap(),
                &PatchParams::apply("glusterd-operator"),
                &patch,
            )
            .await;
        match patch_result {
            Ok(s) => {
                deployments.push(s);
            }
            Err(e) => {
                info!("Unable to patch: {}. Creating set...", e);
                let d = statefulset_api
                    .create(&PostParams::default(), &dep)
                    .await
                    .unwrap();
                deployments.push(d);
            }
        }

        info!("Deployed {:?}", dep.metadata.name.unwrap());
        // Start service for each node
        let svc = Service {
            metadata: ObjectMeta {
                name: Some(format!("glusterd-service-{}", id)),
                namespace: Some(namespace.clone()),
                labels: Some(label.clone()),
                ..Default::default()
            },
            spec: Some(ServiceSpec {
                selector: Some(label.clone()),
                ports: Some(vec![ServicePort {
                    app_protocol: Some("TCP".to_string()),
                    name: Some("brick".to_string()),
                    port: 24007,
                    ..Default::default()
                }]),
                cluster_ip: Some("None".to_string()),
                ..Default::default()
            }),
            ..Default::default()
        };
        let _ = service_api
            .delete(
                &svc.metadata.name.clone().unwrap(),
                &DeleteParams::default(),
            )
            .await;
        info!("Deployed service {:?}", svc.metadata.name.clone().unwrap());
        let s = service_api.create(&pp, &svc).await.unwrap();
        services.push(s);
    }

    // Wait for all to become ready
    for deployment in deployments.iter() {
        let name = deployment.metadata.name.clone().unwrap();
        info!("Waiting for {}", name);
        await_condition(statefulset_api.clone(), &name, is_statefulset_running())
            .await
            .unwrap();
    }

    // Get Pod with selector

    let node = obj.spec.nodes.first().unwrap();
    let id = get_id(obj.as_ref(), &node.name);
    info!("id: {}", id);
    let label_str = get_label(&id);
    info!("Getting pod with label: {}", label_str);
    let pod_api: Api<Pod> = Api::namespaced(client.clone(), &namespace);

    // Execute peer probe for every node on the first pod
    for node in &obj.spec.nodes {
        let id = get_id(obj.as_ref(), &node.name);
        let service_name = format!("glusterd-service-{}.{}", id, namespace);
        info!("Executing for with service {}", service_name);
        let command = vec!["gluster", "peer", "probe", &service_name];
        glusterd_exec(command, &pod_api, &label_str).await;
    }

    // Create brick
    Ok(Action::requeue(Duration::from_secs(3600)))
}

async fn glusterd_exec(command: Vec<&str>, pod_api: &Api<Pod>, label: &str) {
    info!("Getting pod with label: {}", label);
    let params = ListParams {
        label_selector: Some(format!("app={}", label)),
        ..Default::default()
    };
    let pod_list = pod_api.list(&params).await.unwrap();
    let pod = pod_list.items.first().unwrap();

    info!(
        "Executing \"{:?}\" in {}",
        command,
        pod.metadata.name.clone().unwrap()
    );

    let _ = pod_api
        .exec(
            &pod.metadata.name.clone().unwrap(),
            command,
            &AttachParams::default(),
        )
        .await;
}

fn error_policy(_object: Arc<GlusterdStorage>, _err: &MyError, _ctx: Arc<Client>) -> Action {
    Action::requeue(Duration::from_secs(5))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    TermLogger::init(
        LevelFilter::Trace,
        simplelog::Config::default(),
        TerminalMode::Stdout,
        ColorChoice::Auto,
    )
    .unwrap();
    info!("kind = {}", GlusterdStorage::kind(&())); // impl kube::Resource
    info!(
        "crd: {}",
        serde_yaml::to_string(&GlusterdStorage::crd()).unwrap()
    ); // crd yaml

    let client = Client::try_default().await?;
    let glusterd_storages: Api<GlusterdStorage> = Api::all(client.clone());
    let nodes: Api<Node> = Api::all(client.clone());
    let config = watcher::Config::default();

    ////let (reader, writer) = reflector::store();
    ////let stream = reflector(writer, watcher(nodes, config));

    let stream = watcher(glusterd_storages.clone(), config)
        .default_backoff()
        .applied_objects();

    Controller::new(glusterd_storages.clone(), Default::default())
        .owns(
            Api::<Deployment>::all(client.clone()),
            watcher::Config::default(),
        )
        .run(reconcile, error_policy, Arc::new(client.clone()))
        .for_each(|_| futures::future::ready(()))
        .await;

    //pods.exec(
    //    "deployment-0",
    //    ["gluster probe <new_peer"],
    //    &AttachParams::default(),
    //)
    //.await
    //.unwrap();
    pin_mut!(stream);
    while let Some(event) = stream.try_next().await? {
        handle_glusterdstorage_event(event);
    }

    Ok(())
}
