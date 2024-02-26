use kube::{core::ObjectMeta, ResourceExt};
use std::collections::HashSet;

use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct GlusterdStorageNodeSpec {
    pub name: String,
    pub path: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, Default, PartialEq)]
pub enum GlusterdStorageTypeSpec {
    Disperse,
    Replica,
    Arbiter,
    #[default]
    Distribute,
}

#[derive(CustomResource, Clone, Debug, Deserialize, Serialize, JsonSchema, Default, PartialEq)]
#[kube(
    group = "glusterd-operator.storage",
    version = "v1",
    kind = "GlusterdStorage",
    namespaced
)]
pub struct GlusterdStorageSpec {
    pub r#type: GlusterdStorageTypeSpec,
    pub options: Option<Vec<String>>,
    pub nodes: Vec<GlusterdStorageNodeSpec>,
}

impl GlusterdStorage {
    pub fn new_namespaced(name: &str, namespace: &str, spec: GlusterdStorageSpec) -> Self {
        Self {
            metadata: ObjectMeta {
                name: Some(name.to_string()),
                namespace: Some(namespace.to_string()),
                ..Default::default()
            },
            spec,
        }
    }
    pub fn get_namespace(&self) -> String {
        self.namespace().unwrap_or("default".to_string())
    }

    pub fn get_name(&self) -> String {
        self.metadata
            .name
            .clone()
            .unwrap_or("unknown_name".to_string())
    }

    pub fn get_brick_path(&self) -> String {
        format!("/bricks/{}/{}", self.get_namespace(), self.get_name())
    }

    fn get_id(&self, node_name: &str) -> String {
        node_name.to_string()
    }

    pub fn is_valid(&self) -> bool {
        let names: HashSet<String> = self.spec.nodes.iter().map(|n| n.name.clone()).collect();

        self.metadata.name.is_some() && names.len() == self.spec.nodes.len()
    }
}

impl PartialEq for GlusterdStorage {
    fn eq(&self, other: &Self) -> bool {
        self.metadata == other.metadata && self.spec == other.spec
    }
}
