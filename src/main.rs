use std::{
    collections::{BTreeMap, HashMap, HashSet},
    error::Error,
    sync::Arc,
    time::Duration,
};

use futures::{pin_mut, StreamExt, TryStreamExt};
use k8s_openapi::{
    api::{
        apps::v1::{Deployment, StatefulSet, StatefulSetSpec},
        core::v1::{
            Container, HostPathVolumeSource, Pod, PodSpec, PodTemplateSpec, ResourceRequirements,
            SecurityContext, Service, ServicePort, ServiceSpec, Volume, VolumeMount,
        },
    },
    apimachinery::pkg::{api::resource::Quantity, apis::meta::v1::LabelSelector},
};
use kube::{
    api::{
        Api, AttachParams, DeleteParams, ListParams, Patch, PatchParams, PostParams, WatchParams,
    },
    core::{ObjectMeta, WatchEvent},
    runtime::{controller::Action, wait::Condition, watcher, Controller, WatchStreamExt},
    Client, ResourceExt,
};

use kube::core::{CustomResourceExt, Resource};
use kube_derive::CustomResource;
use log::LevelFilter;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use simplelog::{ColorChoice, TermLogger, TerminalMode};

use log::{error, info};
use tokio::sync::RwLock;

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
struct GlusterdStorageNodeSpec {
    name: String,
    path: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
enum GlusterdStorageTypeSpec {
    Dispersed,
    Replica,
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

fn create_volume(name: &str, mount_path: &str, host_path: &str) -> (Volume, VolumeMount) {
    let volume_mount = VolumeMount {
        mount_path: mount_path.to_string(),
        name: name.to_string(),
        ..Default::default()
    };
    let volume = Volume {
        name: name.to_string(),
        host_path: Some(HostPathVolumeSource {
            path: host_path.to_string(),
            ..Default::default()
        }),
        ..Default::default()
    };
    (volume, volume_mount)
}

struct GlusterdOperator {
    client: Client,
    namespace: String,
    storage: Vec<GlusterdStorage>,
    nodes: Vec<GlusterdNode>,
}

impl GlusterdOperator {
    fn new(client: Client, namespace: &str) -> Self {
        Self {
            client,
            namespace: namespace.to_string(),
            storage: vec![],
            nodes: vec![],
        }
    }

    fn add_storage(&mut self, storage: GlusterdStorage) {
        self.storage.push(storage);
    }

    async fn update_nodes(&mut self) {
        for storage in self.storage.iter() {
            for node_spec in storage.spec.nodes.iter() {
                let node_opt = self.nodes.iter_mut().find(|n| n.name == node_spec.name);
                match node_opt {
                    Some(node) => {
                        // We already have a node
                        node.add_storage(storage.clone());
                    }
                    None => {
                        // First time seeing this node
                        let mut node = GlusterdNode::new(&node_spec.name);
                        node.add_storage(storage.clone());
                        self.nodes.push(node);
                    }
                }
            }
        }
    }

    pub async fn update(&mut self) {
        self.update_nodes().await;
        // TODO: support changing nodes

        let mut deployments = vec![];
        let mut services = vec![];
        let statefulset_api = Api::<StatefulSet>::namespaced(self.client.clone(), &self.namespace);
        let service_api = Api::<Service>::namespaced(self.client.clone(), &self.namespace);

        for node in self.nodes.iter() {
            let id = node.name.clone();
            let label_str = get_label(&id);
            let label = BTreeMap::from([("app".to_string(), label_str.clone())]);

            let stateful_set = node.get_statefulset(&self.namespace);
            let patch = Patch::Apply(stateful_set.clone());
            let patch_result = statefulset_api
                .patch(
                    &stateful_set.metadata.name.clone().unwrap(),
                    &PatchParams::apply("glusterd-operator"),
                    &patch,
                )
                .await;
            match patch_result {
                Ok(s) => {
                    deployments.push(s);
                }
                Err(e) => {
                    error!("Unable to patch: {}", e);
                    error!("Patch: {:#?}", patch);
                    // TODO: fix error handling
                    return;
                }
            }
            info!("Deployed {:?}", stateful_set.metadata.name.unwrap());
            // --- DEPLOYMENT END ---

            // Start service for each node
            let svc = Service {
                metadata: ObjectMeta {
                    name: Some(format!("glusterd-service-{}", id)),
                    namespace: Some(self.namespace.clone()),
                    labels: Some(label.clone()),
                    ..Default::default()
                },
                spec: Some(ServiceSpec {
                    selector: Some(label.clone()),
                    ports: Some(vec![
                        ServicePort {
                            app_protocol: Some("TCP".to_string()),
                            name: Some("brick".to_string()),
                            port: 24007,
                            ..Default::default()
                        },
                        ServicePort {
                            name: Some("brick2".to_string()),
                            port: 24008,
                            app_protocol: Some("TCP".to_string()),
                            ..Default::default()
                        },
                    ]),
                    cluster_ip: Some("None".to_string()),
                    ..Default::default()
                }),
                ..Default::default()
            };
            // TODO: patch to prevent connection loss
            let _ = service_api
                .delete(
                    &svc.metadata.name.clone().unwrap(),
                    &DeleteParams::default(),
                )
                .await;
            info!("Deployed service {:?}", svc.metadata.name.clone().unwrap());
            let s = service_api
                .create(&PostParams::default(), &svc)
                .await
                .unwrap();
            services.push(s);

            // Wait for all to become ready
            for deployment in deployments.iter() {
                let name = deployment.metadata.name.clone().unwrap();
                // TODO: this works because of the timeout. Otherwise it hangs forever
                // But unless we wait: We get a 500
                let wp = WatchParams::default()
                    .labels(&format!("app={}", label_str.clone()))
                    .timeout(10);
                let pod_api: Api<Pod> = Api::namespaced(self.client.clone(), &self.namespace);
                let mut stream = pod_api.watch(&wp, "0").await.unwrap().boxed();
                info!("Waiting for {} with label {}", name, label_str);
                while let Some(status) = stream.try_next().await.unwrap() {
                    match status {
                        WatchEvent::Added(o) => {
                            info!("Added wp {}", o.name_any());
                        }
                        WatchEvent::Modified(o) => {
                            let s = o.status.as_ref().expect("status exists on pod");
                            if s.phase.clone().unwrap_or_default() == "Running" {
                                info!("Ready to attach to {}", o.name_any());
                                break;
                            }
                        }
                        _ => {}
                    }
                }
            }
            info!("Waiting done!");
        }

        // Get Pod with selector
        // TODO: 1. probe every node to every node
        // 2. delete hostname1 if there is a hostname2
        // otherwise we have weird hostnames tied to the service ip due to ptr
        // just use a regex
        // make a "wait_for_pod" function

        info!("Iterating storage to exec commands");
        // For every storage find one pod to probe and create brick
        for storage in self.storage.iter() {
            // Find node for current storage
            let nodes: Vec<&GlusterdNode> = self
                .nodes
                .iter()
                .filter(|node| {
                    storage
                        .spec
                        .nodes
                        .iter()
                        .map(|sn| sn.name.clone())
                        .find(|name| node.name == *name)
                        .is_some()
                })
                .collect();
            // TODO: error handling
            for node in nodes {
                let id = node.name.clone();
                info!("id: {}", id);
                let label_str = get_label(&id);
                info!("Getting pod with label: {}", label_str);
                let pod_api: Api<Pod> = Api::namespaced(self.client.clone(), &self.namespace);

                // Execute peer probe for every node on the first pod
                for s_node in &storage.spec.nodes {
                    let service_name =
                        format!("glusterd-service-{}.{}", s_node.name, self.namespace);
                    info!("Executing for with service {}", service_name);
                    let command = vec!["gluster", "peer", "probe", &service_name];
                    node.exec_pod(command, &pod_api).await;
                    // TODO: if new node -> Mark for kill and wait for state == 3
                }
            }
            // Now every node has probed every other node

            // Create brick
            //let bricks: Vec<String> = storage
            //    .spec
            //    .nodes
            //    .iter()
            //    .map(|node| {
            //        let service_name = format!("glusterd-service-{}.{}", node.name, self.namespace);
            //        info!("Executing for with service {}", service_name);
            //        format!("{}:{}", service_name, storage.get_brick_path())
            //    })
            //    .collect();

            //let brick_len_str = bricks.len().to_string();
            //let volume_name = storage.get_name();
            //let mut command = vec![
            //    "gluster",
            //    "volume",
            //    "create",
            //    &volume_name,
            //    "replica",
            //    &brick_len_str,
            //];
            //let mut brick_ref: Vec<&str> = bricks.iter().map(|brick| brick.as_str()).collect();
            //command.append(&mut brick_ref);
            //command.push("force");
            //glusterd_exec(command, &pod_api, &label_str).await;
            //let command = vec!["gluster", "volume", "start", &volume_name];
            //glusterd_exec(command, &pod_api, &label_str).await;
        }
    }
}

struct GlusterdNode {
    name: String,
    storages: HashMap<String, GlusterdStorage>,
}

struct Context {
    client: Client,
    operators: Arc<RwLock<HashMap<String, Arc<RwLock<GlusterdOperator>>>>>,
}

impl GlusterdNode {
    fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            storages: HashMap::new(),
        }
    }
    fn add_storage(&mut self, storage: GlusterdStorage) {
        info!("Adding storage {:?}", storage);
        self.storages.insert(storage.get_name(), storage);
    }

    fn get_brick_mounts(&self) -> Vec<(Volume, VolumeMount)> {
        info!(
            "Getting brick mounts with {} number of storages",
            self.storages.len()
        );
        let volumes: Vec<(Volume, VolumeMount)> = self
            .storages
            .iter()
            .filter_map(|(_name, storage)| {
                let my_spec = storage.spec.nodes.iter().find(|n| self.name == n.name);
                match my_spec {
                    Some(my_spec) => {
                        let host_path = &my_spec.path;
                        // XXX: With this we won't support two bricks of the same storage on the same
                        // node
                        let (volume, volume_mount) = create_volume(
                            &storage.get_name(),
                            &storage.get_brick_path(),
                            &host_path,
                        );
                        Some((volume, volume_mount))
                    }
                    None => None,
                }
            })
            .collect();
        volumes
    }
    // This has to be a stateful set to ensure that the data is only ever written by one instance!
    fn get_statefulset(&self, namespace: &str) -> StatefulSet {
        let label_str = get_label(&self.name);
        let label = BTreeMap::from([("app".to_string(), label_str)]);

        let host_path = format!("/var/lib/k8s-glusterd-{}", namespace);
        let (config_volume, config_volume_mount) =
            create_volume("glusterd-config", "/var/lib/glusterd", &host_path);

        let brick_volumes = self.get_brick_mounts();
        let mut volumes = vec![config_volume];
        let mut volume_mounts = vec![config_volume_mount];

        brick_volumes.iter().for_each(|(volume, volume_mount)| {
            volumes.push(volume.clone());
            volume_mounts.push(volume_mount.clone());
        });

        StatefulSet {
            metadata: ObjectMeta {
                name: Some(format!("glusterd-{}", self.name)),
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
                            self.name.clone(),
                        )])),
                        containers: vec![Container {
                            name: "glusterd".to_string(),
                            // TODO: allow external image and tag for renovate
                            image: Some("ghcr.io/koskev/glusterfs-image:2023.11.26".to_string()),
                            args: Some(vec!["-L".to_string(), "DEBUG".to_string()]),

                            volume_mounts: Some(volume_mounts),
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
                        volumes: Some(volumes),
                        ..Default::default()
                    }),
                },
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    async fn exec_pod(&self, command: Vec<&str>, pod_api: &Api<Pod>) {
        let label = get_label(&self.name);
        info!("Getting pod with label: {}", label);
        let params = ListParams {
            label_selector: Some(format!("app={}", label)),
            ..Default::default()
        };
        let pod_list = pod_api.list(&params).await.unwrap();
        // XXX: Assumes we only have one pod
        let pod = pod_list.items.first().unwrap();

        info!(
            "Executing \"{:?}\" in {}",
            command,
            pod.metadata.name.clone().unwrap()
        );

        let res = pod_api
            .exec(
                &pod.metadata.name.clone().unwrap(),
                command,
                &AttachParams::default(),
            )
            .await;

        match res {
            Ok(mut p) => {
                // TODO: blocks if no output
                //let stdout = tokio_util::io::ReaderStream::new(p.stdout().unwrap())
                //    .filter_map(|r| async {
                //        r.ok().and_then(|v| String::from_utf8(v.to_vec()).ok())
                //    })
                //    .collect::<Vec<_>>()
                //    .await
                //    .join("");
                //let stderr = tokio_util::io::ReaderStream::new(p.stderr().unwrap())
                //    .filter_map(|r| async {
                //        r.ok().and_then(|v| String::from_utf8(v.to_vec()).ok())
                //    })
                //    .collect::<Vec<_>>()
                //    .await
                //    .join("");
                //info!("Stdout: {}", stdout);
                //error!("Stderr: {}", stderr);
                //let _ = p.join().await;
            }
            Err(e) => error!("Error executing command: {}", e),
        }
    }
}

impl GlusterdStorage {
    fn get_namespace(&self) -> String {
        self.namespace().unwrap_or("default".to_string())
    }

    fn get_name(&self) -> String {
        format!(
            "{}-{}",
            self.metadata.name.clone().unwrap(),
            self.get_namespace()
        )
    }

    fn get_brick_path(&self) -> String {
        format!("/bricks/{}", self.get_name())
    }

    fn get_id(&self, node_name: &str) -> String {
        format!("{}", node_name)
    }

    fn is_valid(&self) -> bool {
        let names: HashSet<String> = self.spec.nodes.iter().map(|n| n.name.clone()).collect();

        self.metadata.name.is_some() && names.len() == self.spec.nodes.len()
    }
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

fn get_label(id: &str) -> String {
    format!("glusterd-{}", id)
}

async fn reconcile(obj: Arc<GlusterdStorage>, ctx: Arc<Context>) -> Result<Action> {
    info!("reconcile request: {}", obj.name_any());
    if !obj.is_valid() {
        error!("Invalid Storage! Ignoring");
        return Ok(Action::requeue(Duration::from_secs(3600)));
    }
    let namespace = obj.get_namespace();
    let operator;
    let mut operator_lock = ctx.operators.write().await;
    let operator_opt = operator_lock.get(&namespace);
    match operator_opt {
        Some(o) => operator = o,
        None => {
            // TODO: race condition with hashmap?
            operator_lock.insert(
                namespace.clone(),
                Arc::new(RwLock::new(GlusterdOperator::new(
                    ctx.client.clone(),
                    &namespace,
                ))),
            );

            operator = operator_lock.get(&namespace).unwrap();
        }
    }

    operator.write().await.add_storage(obj.as_ref().clone());
    operator.write().await.update().await;

    Ok(Action::requeue(Duration::from_secs(3600)))
}

fn error_policy(_object: Arc<GlusterdStorage>, _err: &MyError, _ctx: Arc<Context>) -> Action {
    Action::requeue(Duration::from_secs(5))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    TermLogger::init(
        LevelFilter::Info,
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
    let config = watcher::Config::default();

    let stream = watcher(glusterd_storages.clone(), config)
        .default_backoff()
        .applied_objects();

    let context = Context {
        client: client.clone(),
        operators: Arc::new(RwLock::new(HashMap::new())),
    };

    Controller::new(glusterd_storages.clone(), Default::default())
        .owns(
            Api::<Deployment>::all(client.clone()),
            watcher::Config::default(),
        )
        .run(reconcile, error_policy, Arc::new(context))
        .for_each(|_| futures::future::ready(()))
        .await;

    pin_mut!(stream);
    while let Some(event) = stream.try_next().await? {
        handle_glusterdstorage_event(event);
    }

    Ok(())
}
