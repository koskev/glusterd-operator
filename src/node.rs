use futures::StreamExt;
use k8s_openapi::{
    api::{
        apps::v1::{StatefulSet, StatefulSetSpec},
        core::v1::{
            Container, HostPathVolumeSource, Pod, PodSpec, PodTemplateSpec, ResourceRequirements,
            SecurityContext, Volume, VolumeMount,
        },
    },
    apimachinery::pkg::{api::resource::Quantity, apis::meta::v1::LabelSelector},
};
use kube::{
    api::{AttachParams, ListParams},
    core::ObjectMeta,
    Api,
};
use log::{error, info};
use std::collections::{BTreeMap, HashMap};

use crate::{storage::GlusterdStorage, utils::get_label};

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
    pub storages: HashMap<String, GlusterdStorage>,
}

impl GlusterdNode {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            storages: HashMap::new(),
        }
    }

    pub fn add_storage(&mut self, storage: GlusterdStorage) {
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

    pub async fn exec_pod(
        &self,
        command: Vec<&str>,
        pod_api: &Api<Pod>,
    ) -> (Option<String>, Option<String>) {
        let label = get_label(&self.name);
        let mut retval = (None, None);
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
