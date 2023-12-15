use kube::ResourceExt;
use std::collections::HashSet;

use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct GlusterdStorageNodeSpec {
    pub name: String,
    pub path: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub enum GlusterdStorageTypeSpec {
    Dispersed,
    Replica,
    Arbiter,
}

#[derive(CustomResource, Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[kube(
    group = "glusterd-operator.storage",
    version = "v1",
    kind = "GlusterdStorage",
    namespaced
)]
pub struct GlusterdStorageSpec {
    pub r#type: GlusterdStorageTypeSpec,
    pub nodes: Vec<GlusterdStorageNodeSpec>,
}

impl GlusterdStorage {
    pub fn get_namespace(&self) -> String {
        self.namespace().unwrap_or("default".to_string())
    }

    pub fn get_name(&self) -> String {
        format!(
            "{}-{}",
            self.metadata.name.clone().unwrap(),
            self.get_namespace()
        )
    }

    pub fn get_brick_path(&self) -> String {
        format!("/bricks/{}", self.get_name())
    }

    fn get_id(&self, node_name: &str) -> String {
        format!("{}", node_name)
    }

    pub fn is_valid(&self) -> bool {
        let names: HashSet<String> = self.spec.nodes.iter().map(|n| n.name.clone()).collect();

        self.metadata.name.is_some() && names.len() == self.spec.nodes.len()
    }
}
