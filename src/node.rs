use async_trait::async_trait;
use futures::StreamExt;
use k8s_openapi::{
    api::{
        apps::v1::{StatefulSet, StatefulSetSpec},
        core::v1::{
            Container, HostPathVolumeSource, Pod, PodSpec, PodTemplateSpec, ResourceRequirements,
            SecurityContext, Service, ServicePort, ServiceSpec, Volume, VolumeMount,
        },
    },
    apimachinery::pkg::{api::resource::Quantity, apis::meta::v1::LabelSelector},
};
use kube::{
    api::{AttachParams, DeleteParams, ListParams},
    core::ObjectMeta,
    runtime::{conditions, wait::await_condition},
    Api,
};
use log::{error, info};
use regex::Regex;
use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

use crate::{storage::GlusterdStorage, utils::get_label};

#[cfg(test)]
use mockall::{automock, mock, predicate::*};

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

pub struct GlusterdNode {
    pub name: String,
    pub storages: HashMap<String, Arc<GlusterdStorage>>,
    namespace: String,
}

#[async_trait]
pub trait ExecPod {
    async fn exec_pod(
        &self,
        command: Vec<&str>,
        pod_api: &Api<Pod>,
    ) -> (Option<String>, Option<String>);
}

impl GlusterdNode {
    pub fn new(name: &str, namespace: &str) -> Self {
        Self {
            name: name.to_string(),
            storages: HashMap::new(),
            namespace: namespace.to_string(),
        }
    }

    pub fn get_name(&self) -> String {
        self.name.clone()
    }

    pub fn add_storage(&mut self, storage: Arc<GlusterdStorage>) {
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
    pub fn get_statefulset(&self, namespace: &str) -> StatefulSet {
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
                            image: Some("ghcr.io/koskev/glusterd-image:2023.12.20".to_string()),
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

    pub fn get_service(&self, namespace: &str) -> Service {
        let label_str = get_label(&self.name);
        let label = BTreeMap::from([("app".to_string(), label_str)]);
        Service {
            metadata: ObjectMeta {
                name: Some(format!("glusterd-service-{}", self.name)),
                namespace: Some(namespace.to_string()),
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
        }
    }

    pub async fn probe(&self, peer: &str, pod_api: &Api<Pod>) {
        let service_name = format!("glusterd-service-{}.{}", peer, self.namespace);
        info!("Executing for with service {}", service_name);
        let command = vec!["gluster", "peer", "probe", &service_name];
        let (_stdout, stderr) = self.exec_pod(command, &pod_api).await;
        if stderr.is_some() {
            error!("Failed to probe {}: {}", service_name, stderr.unwrap());
            return;
        }
        // TODO: wait for state == 3
        let command = vec!["bash", "-c", "tail -n +1 /var/lib/glusterd/peers/*"];
        let mut connected = false;
        info!("Waiting for connection to be established");
        // Waiting for the correct file to have "state=3"
        let pattern = r"(?m)^state=(.*)$";
        let regex = Regex::new(pattern).unwrap();
        while !connected {
            let (stdout, _err) = self.exec_pod(command.clone(), &pod_api).await;
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

    pub async fn get_peer_num(&self, pod_api: &Api<Pod>) -> usize {
        let cmd = vec!["gluster", "pool", "list"];
        let (stdout, _stderr) = self.exec_pod(cmd, pod_api).await;
        let size;
        match stdout {
            Some(out) => {
                let lines: Vec<&str> = out.split("\n").collect();
                size = lines.len() - 2; // remove header and self
            }
            None => size = 0,
        }
        size
    }

    pub async fn kill_pod(&self, pod_api: &Api<Pod>) {
        let pod_name = self.get_pod_name(pod_api).await.unwrap();
        let _ = pod_api.delete(&pod_name, &DeleteParams::default()).await;
    }

    pub async fn has_wrong_peer(&self, pod_api: &Api<Pod>) -> bool {
        let command = vec!["bash", "-c", "tail -n +1 /var/lib/glusterd/peers/*"];
        let pattern = r"(?m)^hostname1=[0-9]{1,3}-[0-9]{1,3}-[0-9]{1,3}-[0-9]{1,3}\.";
        let regex = Regex::new(pattern).unwrap();

        let (stdout, _err) = self.exec_pod(command.clone(), &pod_api).await;
        match stdout {
            Some(output) => {
                println!("Checking for {} in {}", pattern, output);
                return regex.find(&output).is_some();
            }
            None => return false,
        }
    }

    async fn get_pod_name(&self, pod_api: &Api<Pod>) -> Option<String> {
        let label = get_label(&self.name);
        info!("Getting pod with label: {}", label);
        let params = ListParams {
            label_selector: Some(format!("app={}", label)),
            ..Default::default()
        };
        let pod_list = pod_api.list(&params).await;
        match pod_list {
            Ok(pod_list) => {
                // XXX: Assumes we only have one pod
                let pod = pod_list.items.first().unwrap();

                return pod.metadata.name.clone();
            }
            Err(_e) => return None,
        }
    }

    pub async fn wait_for_pod(&self, pod_api: &Api<Pod>) {
        let pod_name = self.get_pod_name(pod_api).await.unwrap();
        info!("Awaiting {}", pod_name);
        await_condition(pod_api.clone(), &pod_name, conditions::is_pod_running())
            .await
            .unwrap();
        info!("Done awaiting {}", pod_name);
    }
}

#[cfg(not(test))]
#[async_trait]
impl ExecPod for GlusterdNode {
    async fn exec_pod(
        &self,
        command: Vec<&str>,
        pod_api: &Api<Pod>,
    ) -> (Option<String>, Option<String>) {
        let mut retval = (None, None);
        let pod_name = self.get_pod_name(pod_api).await.unwrap();

        info!("Executing \"{:?}\" in {}", command, pod_name);

        let res = pod_api
            .exec(&pod_name, command, &AttachParams::default())
            .await;

        match res {
            Ok(mut p) => {
                let stdout = tokio_util::io::ReaderStream::new(p.stdout().unwrap())
                    .filter_map(|r| async {
                        r.ok().and_then(|v| String::from_utf8(v.to_vec()).ok())
                    })
                    .collect::<Vec<_>>()
                    .await
                    .join("");
                let stderr = tokio_util::io::ReaderStream::new(p.stderr().unwrap())
                    .filter_map(|r| async {
                        r.ok().and_then(|v| String::from_utf8(v.to_vec()).ok())
                    })
                    .collect::<Vec<_>>()
                    .await
                    .join("");
                if stdout.len() > 0 {
                    info!("Stdout: {}", stdout);
                    retval.0 = Some(stdout);
                }
                if stderr.len() > 0 {
                    error!("Stderr: {}", stderr);
                    retval.1 = Some(stderr);
                }
                let _ = p.join().await;
            }
            Err(e) => error!("Error executing command: {}", e),
        }
        retval
    }
}

#[cfg(test)]
pub(crate) mod test {
    use std::{collections::VecDeque, sync::Arc};

    use kube::Client;

    use crate::storage::{
        GlusterdStorage, GlusterdStorageNodeSpec, GlusterdStorageSpec, GlusterdStorageTypeSpec,
    };

    use super::*;
    use serial_test::serial;

    pub(crate) static mut PEER_STDOUT_LIST: VecDeque<String> = VecDeque::new();
    pub(crate) static mut PEER_CMD_LIST: VecDeque<Vec<String>> = VecDeque::new();

    pub(crate) fn get_last_cmd() -> Option<String> {
        let cmd = unsafe { PEER_CMD_LIST.pop_front() };
        match cmd {
            Some(c) => Some(c.join(" ")),
            None => None,
        }
    }

    #[async_trait]
    impl ExecPod for GlusterdNode {
        async fn exec_pod(
            &self,
            command: Vec<&str>,
            _pod_api: &Api<Pod>,
        ) -> (Option<String>, Option<String>) {
            let stdout = unsafe { PEER_STDOUT_LIST.pop_front() };
            let command_string: Vec<String> = command.iter().map(|c| c.to_string()).collect();

            unsafe {
                PEER_CMD_LIST.push_back(command_string);
            }
            (stdout, None)
        }
    }

    #[test]
    fn test_new() {
        let node = GlusterdNode::new("test_node", "ns");

        assert_eq!(node.get_name(), "test_node");
        assert_eq!(node.namespace, "ns");
        assert_eq!(node.storages.len(), 0);
    }

    #[tokio::test]
    #[serial]
    async fn test_storage() {
        unsafe { PEER_CMD_LIST.clear() };
        let mut node = GlusterdNode::new("test_node", "ns");
        let storage_spec = GlusterdStorageSpec {
            r#type: GlusterdStorageTypeSpec::Replica,
            nodes: vec![GlusterdStorageNodeSpec {
                name: "test_node".to_string(),
                path: "/data/brick".to_string(),
            }],
        };
        let storage_spec2 = GlusterdStorageSpec {
            r#type: GlusterdStorageTypeSpec::Disperse,
            nodes: vec![GlusterdStorageNodeSpec {
                name: "test_node".to_string(),
                path: "/data/brick2".to_string(),
            }],
        };
        let storage = Arc::new(GlusterdStorage::new("test_storage", storage_spec.clone()));
        let storage2 = Arc::new(GlusterdStorage::new("test_storage2", storage_spec2.clone()));
        assert_eq!(node.storages.len(), 0);

        node.add_storage(storage.clone());
        assert_eq!(node.storages.len(), 1);

        let bricks = node.get_brick_mounts();
        assert_eq!(bricks[0].0.name, bricks[0].1.name);
        assert_eq!(
            bricks[0].0.host_path.clone().unwrap().path,
            storage_spec.nodes[0].path
        );
        assert_eq!(
            bricks[0].1.mount_path,
            format!("/bricks/{}", bricks[0].1.name)
        );
        node.add_storage(storage2.clone());
        let bricks = node.get_brick_mounts();
        assert_eq!(node.storages.len(), 2);
        assert_eq!(bricks.len(), 2);

        assert_eq!(
            node.storages[&storage.get_name()].get_name(),
            storage.get_name()
        );

        let pod_api = Api::<Pod>::all(Client::try_default().await.unwrap());
        let stdout = r#"
            ==> /var/lib/glusterd/peers/348b125f-aeef-4393-a182-609ade09c8b1 <==
            uuid=348b125f-aeef-4393-a182-609ade09c8b1
            state=3
            hostname1=glusterd-service-test_probe.ns.svc.cluster.local"#
            .lines()
            .map(|line| line.trim_start())
            .collect::<Vec<_>>()
            .join("\n");
        unsafe {
            PEER_STDOUT_LIST.push_back("".to_string());
            PEER_STDOUT_LIST.push_back(stdout);
        }
        node.probe("test_probe", &pod_api).await;
        let cmd = unsafe { PEER_CMD_LIST.pop_front().unwrap() };
        assert_eq!(
            cmd.join(" "),
            "gluster peer probe glusterd-service-test_probe.ns"
        );
        unsafe { PEER_CMD_LIST.clear() };
    }

    #[tokio::test]
    #[serial]
    async fn test_wrong_peer() {
        unsafe {
            PEER_CMD_LIST.clear();
            PEER_STDOUT_LIST.clear();
        };
        let node = GlusterdNode::new("test_node", "ns");
        let pod_api = Api::<Pod>::all(Client::try_default().await.unwrap());

        let stdout = r#"
            ==> /var/lib/glusterd/peers/348b125f-aeef-4393-a182-609ade09c8b1 <==
            uuid=348b125f-aeef-4393-a182-609ade09c8b1
            state=3
            hostname1=glusterd-service-raspberrypi-server2.default.svc.cluster.local

            ==> /var/lib/glusterd/peers/6199aaf4-63a1-4200-aa70-fdc171adb164 <==
            uuid=6199aaf4-63a1-4200-aa70-fdc171adb164
            state=3
            hostname1=glusterd-service-raspberrypi-server.default.svc.cluster.local"#
            .lines()
            .map(|line| line.trim_start())
            .collect::<Vec<_>>()
            .join("\n");
        unsafe {
            PEER_STDOUT_LIST.push_back(stdout);
        }
        assert!(!node.has_wrong_peer(&pod_api).await);

        let stdout = r#"
            ==> /var/lib/glusterd/peers/348b125f-aeef-4393-a182-609ade09c8b1 <==
            uuid=348b125f-aeef-4393-a182-609ade09c8b1
            state=3
            hostname1=glusterd-service-raspberrypi-server2.default.svc.cluster.local

            ==> /var/lib/glusterd/peers/6199aaf4-63a1-4200-aa70-fdc171adb164 <==
            uuid=6199aaf4-63a1-4200-aa70-fdc171adb164
            state=3
            hostname1=10-244-0-137.glusterd-service-raspberrypi-server.default.svc.cluster.local"#
            .lines()
            .map(|line| line.trim_start())
            .collect::<Vec<_>>()
            .join("\n");
        unsafe {
            PEER_STDOUT_LIST.push_back(stdout);
        }
        assert!(node.has_wrong_peer(&pod_api).await);
        unsafe {
            PEER_CMD_LIST.clear();
        }
    }
}
