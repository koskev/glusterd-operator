use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use k8s_openapi::api::apps::v1::StatefulSet;
use k8s_openapi::api::core::v1::{Pod, Service, ServicePort, ServiceSpec};
use kube::api::{DeleteParams, ListParams, Patch, PatchParams, PostParams};
use kube::core::ObjectMeta;
use kube::runtime::conditions;
use kube::runtime::wait::await_condition;
use kube::{Api, Client};
use regex::Regex;

use crate::node::GlusterdNode;
use crate::storage::GlusterdStorage;
use crate::utils::get_label;

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

    pub async fn update(&mut self) {
        // TODO: support changing nodes

        let mut deployments = vec![];
        let mut services = vec![];
        let statefulset_api = Api::<StatefulSet>::namespaced(self.client.clone(), &self.namespace);
        let service_api = Api::<Service>::namespaced(self.client.clone(), &self.namespace);
        let pod_api: Api<Pod> = Api::namespaced(self.client.clone(), &self.namespace);

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
            let svc = node.get_service(&self.namespace);
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
                let label_str = get_label(&node.name);
                let name = deployment.metadata.name.clone().unwrap();
                // Unless we wait: We get a 500 error
                // TODO: make a "wait_for_pod" function
                let pod_api: Api<Pod> = Api::namespaced(self.client.clone(), &self.namespace);

                let params = ListParams {
                    label_selector: Some(format!("app={}", label_str.clone())),
                    ..Default::default()
                };
                let pod_list = pod_api.list(&params).await.unwrap();

                for pod in pod_list {
                    let pod_name = &pod.metadata.name.unwrap();
                    info!("Awaiting {}", pod_name);
                    await_condition(pod_api.clone(), &pod_name, conditions::is_pod_running())
                        .await
                        .unwrap();
                    info!("Done awaiting {}", pod_name);
                }

                info!("Waiting done for {}!", name);
            }
        }

        // Take first node and probe it with every other node

        let mut first_node: Option<&GlusterdNode> = None;
        for node in &self.nodes {
            match first_node {
                Some(first_node) => {
                    first_node.probe(&node.name, &pod_api).await;
                }
                None => first_node = Some(node),
            }
        }

        // Every node is probed now.
        // It might be possible that some nodes have the weird peer info

        // Check all nodes for weird peers and restart them if needed
        // The actual correction is done by the container to avoid possible errors with a running
        // instance
        for node in &self.nodes {
            if node.has_wrong_peer(&pod_api).await {
                node.kill_pod(&pod_api).await;
            }
        }

        // Kill nodes to apply correct dns names
        // Only apply to nodes that have a wrong dns name in their config
        // only kill one at a time to prevent downtimes

        // Now every node has probed every other node
        info!("Done probing nodes");

        for node in &self.nodes {
            let command = vec!["gluster", "volume", "list"];
            let (output, _) = node.exec_pod(command, &pod_api).await;
            let mut existing_volumes = HashSet::new();
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
                    let brick_len_str = bricks.len().to_string();
                    let volume_name = storage.get_name();
                    let mut command = vec![
                        "gluster",
                        "volume",
                        "create",
                        &volume_name,
                        // TODO: support more than replica
                        "replica",
                        &brick_len_str,
                    ];
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
