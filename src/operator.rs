use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use k8s_openapi::api::apps::v1::{DaemonSet, DaemonSetSpec, StatefulSet};
use k8s_openapi::api::core::v1::{
    Capabilities, Container, HostPathVolumeSource, Pod, PodSecurityContext, PodSpec,
    PodTemplateSpec, SecurityContext, Service, Volume, VolumeMount,
};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::LabelSelector;
use kube::api::{DeleteParams, Patch, PatchParams, PostParams};
use kube::core::ObjectMeta;
use kube::{Api, Client};

use crate::node::{ExecPod, GlusterdNode};
use crate::storage::{GlusterdStorage, GlusterdStorageTypeSpec};

use log::{error, info};

pub struct GlusterdOperator {
    client: Client,
    namespace: String,
    nodes: Vec<GlusterdNode>,
}

impl GlusterdOperator {
    pub fn new(client: Client, namespace: &str) -> Self {
        Self {
            client,
            namespace: namespace.to_string(),
            nodes: vec![],
        }
    }

    fn get_storages(&self) -> HashMap<String, Arc<GlusterdStorage>> {
        let mut storages: HashMap<String, Arc<GlusterdStorage>> = HashMap::new();
        for node in self.nodes.iter() {
            for (name, storage) in node.storages.iter() {
                storages.insert(name.clone(), storage.clone());
            }
        }
        storages
    }

    fn get_client_mount_daemonset(&self) -> Vec<DaemonSet> {
        let mut daemonsets = vec![];
        for (_, storage) in self.get_storages() {
            let server = format!(
                "glusterd-service-{}.{}",
                storage.spec.nodes.first().unwrap().name.clone(),
                storage.get_namespace()
            );
            let volume_name = storage.metadata.name.clone().unwrap();
            let mount_point = format!("/mnt/glusterfs/{}/{}", self.namespace, volume_name);
            let name = format!("glusterfs-mount-{}", storage.get_name());
            let label = BTreeMap::from([("app".to_string(), name.clone())]);
            let ds = DaemonSet {
                metadata: ObjectMeta {
                    name: Some(name),
                    namespace: Some(self.namespace.clone()),
                    labels: Some(label.clone()),
                    ..Default::default()
                },
                spec: Some(DaemonSetSpec {
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
                            containers: vec![Container {
                                name: "glusterfs-client".to_string(),
                                image: Some(
                                    "ghcr.io/koskev/glusterfs-image:2023.12.20".to_string(),
                                ),
                                image_pull_policy: Some("Always".to_string()),
                                args: Some(vec![server, volume_name, mount_point]),
                                security_context: Some(SecurityContext {
                                    // Needed for fuse
                                    privileged: Some(true),
                                    capabilities: Some(Capabilities {
                                        add: Some(vec!["SYS_ADMIN".to_string()]),
                                        ..Default::default()
                                    }),

                                    ..Default::default()
                                }),
                                volume_mounts: Some(vec![
                                    VolumeMount {
                                        name: "fuse".to_string(),
                                        mount_path: "/dev/fuse".to_string(),
                                        ..Default::default()
                                    },
                                    VolumeMount {
                                        name: "gluster".to_string(),
                                        mount_path: "/mnt/glusterfs".to_string(),
                                        mount_propagation: Some("Bidirectional".to_string()),
                                        ..Default::default()
                                    },
                                ]),
                                ..Default::default()
                            }],
                            volumes: Some(vec![
                                Volume {
                                    name: "fuse".to_string(),
                                    host_path: Some(HostPathVolumeSource {
                                        path: "/dev/fuse".to_string(),
                                        ..Default::default()
                                    }),
                                    ..Default::default()
                                },
                                Volume {
                                    name: "gluster".to_string(),
                                    host_path: Some(HostPathVolumeSource {
                                        path: "/mnt/glusterfs".to_string(),
                                        ..Default::default()
                                    }),
                                    ..Default::default()
                                },
                            ]),
                            ..Default::default()
                        }),
                    },
                    ..Default::default()
                }),
                ..Default::default()
            };
            daemonsets.push(ds);
        }
        daemonsets
    }

    pub fn add_storage(&mut self, storage: GlusterdStorage) {
        let storage_rc = Arc::new(storage);
        for node_spec in storage_rc.spec.nodes.iter() {
            let node_opt = self.nodes.iter_mut().find(|n| n.name == node_spec.name);
            match node_opt {
                Some(node) => {
                    // We already have a node
                    node.add_storage(storage_rc.clone());
                }
                None => {
                    // First time seeing this node
                    let mut node = GlusterdNode::new(&node_spec.name, &self.namespace);
                    node.add_storage(storage_rc.clone());
                    self.nodes.push(node);
                }
            }
        }
    }

    // Take first node and probe it with every other node
    async fn probe_nodes(&self, pod_api: &Api<Pod>) {
        let mut cluster_node: Option<&GlusterdNode> = None;
        for node in &self.nodes {
            if cluster_node.is_none() {
                // Set to a value in case we don't have a cluster yet
                cluster_node = Some(node);
            }
            // Use the first node part of the cluster.
            // XXX: We assume we have no split cluster
            if node.get_peer_num(pod_api).await > 0 {
                cluster_node = Some(node);
                break;
            }
        }
        for node in &self.nodes {
            if node.get_name() != cluster_node.unwrap().get_name() {
                cluster_node
                    .unwrap()
                    .probe(&node.get_name(), &pod_api)
                    .await;
            }
        }
    }

    async fn create_volumes(&self, pod_api: &Api<Pod>) {
        // Create dirs on all nodes as gluster won't do that
        for node in &self.nodes {
            for (_name, storage) in &node.storages {
                let brick_path = storage.get_brick_path();
                let mkdir_cmd = vec!["mkdir", "-p", &brick_path];
                node.exec_pod(mkdir_cmd, pod_api).await;
            }
        }
        let mut existing_volumes = HashSet::new();
        for node in &self.nodes {
            let command = vec!["gluster", "volume", "list"];
            let (output, _) = node.exec_pod(command, &pod_api).await;
            if let Some(output) = output {
                output.split("\n").for_each(|v| {
                    existing_volumes.insert(v.to_string());
                });
            }
            for (name, storage) in &node.storages {
                // Volume does not exist yet
                if !existing_volumes.contains(name) {
                    // Create volume
                    let bricks: Vec<String> = storage
                        .spec
                        .nodes
                        .iter()
                        .map(|n| {
                            let service_name =
                                format!("glusterd-service-{}.{}", n.name, self.namespace);
                            format!("{}:{}", service_name, storage.get_brick_path())
                        })
                        .collect();
                    let volume_name = storage.get_name();
                    let type_cmd = match storage.spec.r#type {
                        GlusterdStorageTypeSpec::Replica => {
                            vec!["replica".to_string(), bricks.len().to_string()]
                        }
                        GlusterdStorageTypeSpec::Disperse => {
                            vec!["disperse".to_string(), bricks.len().to_string()]
                        }
                        GlusterdStorageTypeSpec::Arbiter => {
                            vec![
                                "replica".to_string(),
                                (bricks.len() - 1).to_string(),
                                "arbiter".to_string(),
                                "1".to_string(),
                            ]
                        }
                        GlusterdStorageTypeSpec::Distribute => {
                            vec![]
                        }
                    };
                    let mut command = vec!["gluster", "volume", "create", &volume_name];
                    let type_cmd_str: Vec<&str> = type_cmd.iter().map(|c| c.as_str()).collect();
                    command.extend(type_cmd_str);
                    let mut brick_ref: Vec<&str> =
                        bricks.iter().map(|brick| brick.as_str()).collect();
                    command.append(&mut brick_ref);
                    command.push("force");
                    node.exec_pod(command, &pod_api).await;

                    let command = vec!["gluster", "volume", "start", &volume_name];
                    node.exec_pod(command, &pod_api).await;

                    // Add to existing volumes
                    existing_volumes.insert(name.clone());
                }
            }
        }
    }

    async fn patch_nodes(
        &self,
        pod_api: &Api<Pod>,
        statefulset_api: &Api<StatefulSet>,
        service_api: &Api<Service>,
    ) {
        for node in self.nodes.iter() {
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
                Ok(_s) => {}
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
            let svc = node.get_service(&self.namespace);
            // TODO: patch to prevent connection loss
            let _ = service_api
                .delete(
                    &svc.metadata.name.clone().unwrap(),
                    &DeleteParams::default(),
                )
                .await;
            info!("Deployed service {:?}", svc.metadata.name.clone().unwrap());
            let _s = service_api
                .create(&PostParams::default(), &svc)
                .await
                .unwrap();

            // Wait for all to become ready
            node.wait_for_pod(&pod_api).await;
        }
    }

    async fn deploy_mount_clients(&self, daemonset_api: &Api<DaemonSet>) {
        let sets = self.get_client_mount_daemonset();
        for set in sets.iter() {
            let patch = Patch::Apply(set.clone());
            let patch_result = daemonset_api
                .patch(
                    &set.metadata.name.clone().unwrap(),
                    &PatchParams::apply("glusterd-operator"),
                    &patch,
                )
                .await;
            match patch_result {
                Ok(_s) => {}
                Err(e) => {
                    error!("Unable to patch: {}", e);
                    error!("Patch: {:#?}", patch);
                    // TODO: fix error handling
                    return;
                }
            }
        }
    }

    pub async fn update(&mut self) {
        // TODO: support changing nodes

        let statefulset_api = Api::<StatefulSet>::namespaced(self.client.clone(), &self.namespace);
        let service_api = Api::<Service>::namespaced(self.client.clone(), &self.namespace);
        let pod_api: Api<Pod> = Api::namespaced(self.client.clone(), &self.namespace);
        let daemonset_api: Api<DaemonSet> = Api::namespaced(self.client.clone(), &self.namespace);

        self.patch_nodes(&pod_api, &statefulset_api, &service_api)
            .await;
        self.probe_nodes(&pod_api).await;
        self.deploy_mount_clients(&daemonset_api).await;

        // Every node is probed now.
        // It might be possible that some nodes have the weird peer info

        // Check all nodes for weird peers and restart them if needed
        // The actual correction is done by the container to avoid possible errors with a running
        // instance
        for node in &self.nodes {
            if node.has_wrong_peer(&pod_api).await {
                node.kill_pod(&pod_api).await;
                // Wait for pod to go online again
                // Once it is online we can kill the next node
                node.wait_for_pod(&pod_api).await;
            }
        }

        // Now every node has probed every other node
        info!("Done probing nodes");
        // TODO: Even if A -> B Probe is successfull. B -> A Might be pending!
        self.create_volumes(&pod_api).await;
    }
}

#[cfg(test)]
mod test {

    use k8s_openapi::api::core::v1::Pod;
    use kube::{Api, Client};

    use crate::{
        node::test::{get_last_cmd, PEER_CMD_LIST, PEER_STDOUT_LIST},
        storage::{
            GlusterdStorage, GlusterdStorageNodeSpec, GlusterdStorageSpec, GlusterdStorageTypeSpec,
        },
    };

    use super::GlusterdOperator;
    use serial_test::serial;

    async fn test_operator(storage_spec: GlusterdStorageSpec) {
        unsafe {
            PEER_CMD_LIST.clear();
            PEER_STDOUT_LIST.clear();
        };
        let namespace = "ns";
        let mut operator = GlusterdOperator::new(Client::try_default().await.unwrap(), namespace);
        let pod_api = Api::<Pod>::all(operator.client.clone());

        let storage =
            GlusterdStorage::new_namespaced("test_storage", namespace, storage_spec.clone());
        assert_eq!(operator.nodes.len(), 0);
        operator.add_storage(storage.clone());

        assert_eq!(operator.nodes.len(), storage_spec.nodes.len());
        assert_eq!(operator.namespace, "ns");

        operator.create_volumes(&pod_api).await;

        let expected_command_num = 3 + // list, create, start
            storage_spec.nodes.len() * 2 // len * mkdir + len - 1 * list
            - 1;

        //Remove gluster list cmd
        unsafe {
            // list, create, start, list for every extra node
            assert_eq!(PEER_CMD_LIST.len(), expected_command_num);
            for _ in 0..storage_spec.nodes.len() {
                let cmd = get_last_cmd().unwrap();
                assert_eq!(
                    cmd,
                    format!("mkdir -p /bricks/{}/{}", namespace, "test_storage")
                );
            }
            let cmd = get_last_cmd().unwrap();
            assert_eq!(cmd, "gluster volume list");
        }
        let cmd = get_last_cmd().unwrap();
        let bricks: Vec<String> = storage_spec
            .nodes
            .iter()
            .map(|n| {
                let service_name = format!("glusterd-service-{}.{}", n.name, namespace);
                format!("{}:{}", service_name, storage.get_brick_path())
            })
            .collect();
        let storage_type_string = match storage_spec.r#type {
            GlusterdStorageTypeSpec::Replica => format!("replica {}", storage_spec.nodes.len()),
            GlusterdStorageTypeSpec::Arbiter => {
                format!("replica {} arbiter 1", storage_spec.nodes.len() - 1)
            }
            GlusterdStorageTypeSpec::Disperse => format!("disperse {}", storage_spec.nodes.len()),
            GlusterdStorageTypeSpec::Distribute => "".to_string(),
        };

        let expected_cmd = format!(
            "gluster volume create test_storage {} {} force",
            storage_type_string,
            bricks.join(" ")
        );
        assert_eq!(cmd, expected_cmd);
        let cmd = get_last_cmd().unwrap();
        assert_eq!(cmd, "gluster volume start test_storage");
        unsafe {
            assert_eq!(PEER_CMD_LIST.len(), storage_spec.nodes.len() - 1);
            PEER_CMD_LIST.clear();
        }

        unsafe {
            for _ in storage_spec.nodes.iter() {
                // For the mkdir command
                PEER_STDOUT_LIST.push_back("".to_string());
            }
            PEER_STDOUT_LIST.push_back("test_storage".to_string());
        }

        // Create the volumes again. We should only have volume list commands
        operator.create_volumes(&pod_api).await;

        // One mkdir per node
        for _ in storage_spec.nodes.iter() {
            let cmd = get_last_cmd().unwrap();
            assert_eq!(
                cmd,
                format!("mkdir -p /bricks/{}/{}", namespace, "test_storage")
            );
        }

        // One list for every storage node
        for _ in storage_spec.nodes.iter() {
            let cmd = get_last_cmd().unwrap();
            assert_eq!(cmd, "gluster volume list");
        }
        let cmd = get_last_cmd();
        assert_eq!(cmd, None);
    }

    #[tokio::test]
    #[serial]
    async fn test_replica2() {
        let storage_spec = GlusterdStorageSpec {
            r#type: GlusterdStorageTypeSpec::Replica,
            options: vec![],
            nodes: vec![
                GlusterdStorageNodeSpec {
                    name: "test_node".to_string(),
                    path: "/data/brick".to_string(),
                },
                GlusterdStorageNodeSpec {
                    name: "test_node2".to_string(),
                    path: "/data/brick".to_string(),
                },
            ],
        };

        test_operator(storage_spec).await;
    }

    #[tokio::test]
    #[serial]
    async fn test_replica3() {
        let storage_spec = GlusterdStorageSpec {
            r#type: GlusterdStorageTypeSpec::Replica,
            options: vec![],
            nodes: vec![
                GlusterdStorageNodeSpec {
                    name: "test_node".to_string(),
                    path: "/data/brick".to_string(),
                },
                GlusterdStorageNodeSpec {
                    name: "test_node2".to_string(),
                    path: "/data/brick".to_string(),
                },
                GlusterdStorageNodeSpec {
                    name: "test_node3".to_string(),
                    path: "/data/brick".to_string(),
                },
            ],
        };
        test_operator(storage_spec).await;
    }

    #[tokio::test]
    #[serial]
    async fn test_arbiter() {
        let storage_spec = GlusterdStorageSpec {
            r#type: GlusterdStorageTypeSpec::Arbiter,
            options: vec![],
            nodes: vec![
                GlusterdStorageNodeSpec {
                    name: "test_node".to_string(),
                    path: "/data/brick".to_string(),
                },
                GlusterdStorageNodeSpec {
                    name: "test_node2".to_string(),
                    path: "/data/brick".to_string(),
                },
                GlusterdStorageNodeSpec {
                    name: "test_node3".to_string(),
                    path: "/data/brick".to_string(),
                },
            ],
        };
        test_operator(storage_spec).await;
    }

    #[tokio::test]
    #[serial]
    async fn test_dispersed() {
        let storage_spec = GlusterdStorageSpec {
            r#type: GlusterdStorageTypeSpec::Disperse,
            options: vec![],
            nodes: vec![
                GlusterdStorageNodeSpec {
                    name: "test_node".to_string(),
                    path: "/data/brick".to_string(),
                },
                GlusterdStorageNodeSpec {
                    name: "test_node2".to_string(),
                    path: "/data/brick".to_string(),
                },
                GlusterdStorageNodeSpec {
                    name: "test_node3".to_string(),
                    path: "/data/brick".to_string(),
                },
            ],
        };
        test_operator(storage_spec).await;
    }
}
