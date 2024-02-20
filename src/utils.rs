use kube::core::ObjectMeta;

pub fn get_label(id: &str) -> String {
    format!("glusterd-{}", id)
}

pub trait MetaName {
    fn get_name(&self) -> String;
}

impl MetaName for ObjectMeta {
    fn get_name(&self) -> String {
        self.name.clone().unwrap_or("unknown-name".to_string())
    }
}
