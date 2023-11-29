use std::{collections::HashMap, error::Error, sync::Arc, time::Duration};

use futures::{pin_mut, StreamExt, TryStreamExt};
use k8s_openapi::api::apps::v1::Deployment;
use kube::{
    api::Api,
    runtime::{controller::Action, watcher, Controller, WatchStreamExt},
    Client, CustomResourceExt, Resource, ResourceExt,
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

#[derive(thiserror::Error, Debug)]
pub enum MyError {}

pub type Result<T, E = MyError> = std::result::Result<T, E>;

fn handle_glusterdstorage_event(event: GlusterdStorage) {
    info!("New GlusterdStorage with spec {:?}", event.spec)
}

async fn reconcile(obj: Arc<GlusterdStorage>, ctx: Arc<Context>) -> Result<Action> {
    info!("reconcile request: {}", obj.name_any());
    if !obj.is_valid() {
        error!("Invalid Storage! Ignoring");
        return Ok(Action::requeue(Duration::from_secs(3600)));
    }
    let namespace = obj.get_namespace();
    let operator;
    let mut operator_lock = ctx.operators.write().await;
    let operator_opt = operator_lock.get(&namespace);
    match operator_opt {
        Some(o) => operator = o,
        None => {
            // TODO: race condition with hashmap?
            operator_lock.insert(
                namespace.clone(),
                Arc::new(RwLock::new(GlusterdOperator::new(
                    ctx.client.clone(),
                    &namespace,
                ))),
            );

            operator = operator_lock.get(&namespace).unwrap();
        }
    }

    operator.write().await.add_storage(obj.as_ref().clone());
    operator.write().await.update().await;

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
    .unwrap();
    info!("kind = {}", GlusterdStorage::kind(&())); // impl kube::Resource
    info!(
        "crd: {}",
        serde_yaml::to_string(&GlusterdStorage::crd()).unwrap()
    ); // crd yaml

    let client = Client::try_default().await?;
    let glusterd_storages: Api<GlusterdStorage> = Api::all(client.clone());
    let config = watcher::Config::default();

    let stream = watcher(glusterd_storages.clone(), config)
        .default_backoff()
        .applied_objects();

    let context = Context {
        client: client.clone(),
        operators: Arc::new(RwLock::new(HashMap::new())),
    };

    Controller::new(glusterd_storages.clone(), Default::default())
        .owns(
            Api::<Deployment>::all(client.clone()),
            watcher::Config::default(),
        )
        .run(reconcile, error_policy, Arc::new(context))
        .for_each(|_| futures::future::ready(()))
        .await;

    pin_mut!(stream);
    while let Some(event) = stream.try_next().await? {
        handle_glusterdstorage_event(event);
    }

    Ok(())
}
