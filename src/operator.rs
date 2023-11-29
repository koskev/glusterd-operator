use std::collections::BTreeMap;

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
    storage: Vec<GlusterdStorage>,
    nodes: Vec<GlusterdNode>,
}

impl GlusterdOperator {
    pub fn new(client: Client, namespace: &str) -> Self {
        Self {
            client,
            namespace: namespace.to_string(),
            storage: vec![],
            nodes: vec![],
        }
    }

    pub fn add_storage(&mut self, storage: GlusterdStorage) {
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

        // Get Pod with selector

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
                let pod_api: Api<Pod> = Api::namespaced(self.client.clone(), &self.namespace);

                // Execute peer probe for every node on the first pod
                for s_node in &storage.spec.nodes {
                    if node.name == s_node.name {
                        // skip self
                        continue;
                    }
                    let service_name =
                        format!("glusterd-service-{}.{}", s_node.name, self.namespace);
                    info!("Executing for with service {}", service_name);
                    let command = vec!["gluster", "peer", "probe", &service_name];
                    node.exec_pod(command, &pod_api).await;
                    // TODO: wait for state == 3
                    let command = vec!["bash", "-c", "tail -n +1 /var/lib/glusterd/peers/*"];
                    let mut connected = false;
                    info!("Waiting for connection to be established");
                    // Waiting for the correct file to have "state=3"
                    let pattern = r"(?m)^state=(.*)$";
                    let regex = Regex::new(pattern).unwrap();
                    while !connected {
                        let (stdout, _err) = node.exec_pod(command.clone(), &pod_api).await;
                        match stdout {
                            Some(output) => {
                                info!("Checking if line contains {}", service_name);
                                if let Some(found_line) =
                                    output.split("\n\n").find(|s| s.contains(&service_name))
                                {
                                    info!("Got service name, checking regex");
                                    if let Some(c) = regex.captures(found_line) {
                                        info!("Found regex");
                                        if let Some(g) = c.get(1) {
                                            info!("State is {}", g.as_str());
                                            connected = g.as_str() == "3";
                                        }
                                    }
                                }
                            }
                            None => (),
                        }
                    }
                }
            }
            // Now every node has probed every other node
            info!("Done probing nodes");

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
