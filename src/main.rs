use std::{collections::HashMap, error::Error, sync::Arc, time::Duration};

use clap::Parser;
use futures::StreamExt;
use k8s_openapi::api::apps::v1::Deployment;
use kube::{
    api::{Api, ListParams},
    runtime::{controller::Action, watcher, Controller},
    Client, CustomResourceExt, ResourceExt,
};

use log::LevelFilter;
use operator::GlusterdOperator;
use simplelog::{ColorChoice, TermLogger, TerminalMode};

use log::{error, info};
use storage::GlusterdStorage;
use tokio::sync::RwLock;

mod node;
mod operator;
mod storage;
mod utils;

struct Context {
    client: Client,
    operators: Arc<RwLock<HashMap<String, Arc<RwLock<GlusterdOperator>>>>>,
}

impl Context {
    async fn new() -> Self {
        Self {
            // If we fail here something is really wrong :)
            #[allow(clippy::unwrap_used)]
            client: Client::try_default().await.unwrap(),
            operators: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    async fn get_operator(&self, namespace: &str) -> Arc<RwLock<GlusterdOperator>> {
        let namespace = namespace.to_string();
        let mut operator_lock = self.operators.write().await;
        let operator_opt = operator_lock.get(&namespace);
        match operator_opt {
            Some(o) => o.clone(),
            None => {
                let glusterd_operator = Arc::new(RwLock::new(GlusterdOperator::new(
                    self.client.clone(),
                    &namespace,
                )));
                operator_lock.insert(namespace, glusterd_operator.clone());
                glusterd_operator
            }
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub enum MyError {}

pub type Result<T, E = MyError> = std::result::Result<T, E>;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    /// List crd
    #[arg(short, long)]
    crd: bool,

    #[arg(short, long, default_value = "storage")]
    namespace: String,
}

async fn reconcile(obj: Arc<GlusterdStorage>, ctx: Arc<Context>) -> Result<Action> {
    info!("reconcile request: {}", obj.name_any());
    if !obj.is_valid() {
        error!("Invalid Storage! Ignoring");
        return Ok(Action::requeue(Duration::from_secs(3600)));
    }
    let namespace = obj.get_namespace();
    let operator = ctx.get_operator(&namespace).await;

    if operator.write().await.add_storage(obj.as_ref().clone()) {
        info!("Got changed storage. Updating...");
        operator.write().await.update().await;
    } else {
        info!("Storage unchanged");
    }

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
    .expect("Failed to init termlogger. Exiting!");
    let args = Cli::parse();

    if args.crd {
        println!(
            "{}",
            serde_yaml::to_string(&GlusterdStorage::crd())
                .unwrap_or("Failed to generate crd!".to_string())
        ); // crd yaml
        return Ok(());
    }

    let context = Context::new().await;
    let glusterd_storages: Api<GlusterdStorage> =
        Api::namespaced(context.client.clone(), &args.namespace);
    // Create initial state
    let storage_list = glusterd_storages.list(&ListParams::default()).await;
    match storage_list {
        Ok(storage_list) => {
            for existing_storage in storage_list.iter() {
                let namespace = existing_storage.get_namespace();
                let operator = context.get_operator(&namespace).await;
                operator.write().await.add_storage(existing_storage.clone());
            }
        }
        Err(_e) => (),
    }

    Controller::new(glusterd_storages.clone(), Default::default())
        .owns(
            Api::<Deployment>::namespaced(context.client.clone(), &args.namespace),
            watcher::Config::default(),
        )
        .run(reconcile, error_policy, Arc::new(context))
        .for_each(|_| futures::future::ready(()))
        .await;

    Ok(())
}
