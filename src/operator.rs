use std::collections::HashSet;
use std::sync::Arc;

use k8s_openapi::api::apps::v1::StatefulSet;
use k8s_openapi::api::core::v1::{Pod, Service};
use kube::api::{DeleteParams, Patch, PatchParams, PostParams};
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

    pub async fn update(&mut self) {
        // TODO: support changing nodes

        let statefulset_api = Api::<StatefulSet>::namespaced(self.client.clone(), &self.namespace);
        let service_api = Api::<Service>::namespaced(self.client.clone(), &self.namespace);
        let pod_api: Api<Pod> = Api::namespaced(self.client.clone(), &self.namespace);

        self.patch_nodes(&pod_api, &statefulset_api, &service_api)
            .await;
        self.probe_nodes(&pod_api).await;

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
        let mut operator = GlusterdOperator::new(Client::try_default().await.unwrap(), "ns");
        let pod_api = Api::<Pod>::all(operator.client.clone());

        let storage = GlusterdStorage::new("test_storage", storage_spec.clone());
        assert_eq!(operator.nodes.len(), 0);
        operator.add_storage(storage.clone());

        assert_eq!(operator.nodes.len(), storage_spec.nodes.len());
        assert_eq!(operator.namespace, "ns");

        operator.create_volumes(&pod_api).await;

        //Remove gluster list cmd
        unsafe {
            // list, create, start, list for every extra node
            assert_eq!(PEER_CMD_LIST.len(), 3 + storage_spec.nodes.len() - 1);
            PEER_CMD_LIST.pop_front();
        }
        let cmd = get_last_cmd().unwrap();
        let bricks: Vec<String> = storage_spec
            .nodes
            .iter()
            .map(|n| {
                let service_name = format!("glusterd-service-{}.ns", n.name);
                format!("{}:{}", service_name, storage.get_brick_path())
            })
            .collect();
        let storage_type_string = match storage_spec.r#type {
            GlusterdStorageTypeSpec::Replica => format!("replica {}", storage_spec.nodes.len()),
            GlusterdStorageTypeSpec::Arbiter => {
                format!("replica {} arbiter 1", storage_spec.nodes.len() - 1)
            }
            GlusterdStorageTypeSpec::Disperse => format!("disperse {}", storage_spec.nodes.len()),
        };

        let expected_cmd = format!(
            "gluster volume create test_storage-default {} {} force",
            storage_type_string,
            bricks.join(" ")
        );
        assert_eq!(cmd, expected_cmd);
        let cmd = get_last_cmd().unwrap();
        assert_eq!(cmd, "gluster volume start test_storage-default");
        unsafe {
            assert_eq!(PEER_CMD_LIST.len(), storage_spec.nodes.len() - 1);
            PEER_CMD_LIST.clear();
        }

        unsafe {
            PEER_STDOUT_LIST.push_back("test_storage-default".to_string());
        }
        operator.create_volumes(&pod_api).await;

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
