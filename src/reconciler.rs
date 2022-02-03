use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Mutex,
};

use futures::{
    future::{join_all, BoxFuture},
    pin_mut, StreamExt,
};
use k8s_openapi::api::apps;
use kube::{api::ListParams, Api, Client, Resource};
use kube_runtime::watcher;
use serde::Deserialize;
use snafu::{ResultExt, Snafu};
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, error, info};

use crate::gcs_watcher::VersionstoreUpdate;

static VERSIONSTORE_ANNOTATION: &'static str = "versionstore-operator.prod.zone/managed";

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("k8s client error: {}", source))]
    K8SClient { source: kube::Error },
}

type Result<T> = std::result::Result<T, Error>;

#[derive(Clone)]
struct SomeResource(apps::v1::Deployment);

impl SomeResource {
    pub fn get_key(&self) -> String {
        format!(
            "{}/{}/{}/{}",
            <apps::v1::Deployment as k8s_openapi::Resource>::API_VERSION,
            <apps::v1::Deployment as k8s_openapi::Resource>::KIND,
            self.0
                .meta()
                .namespace
                .as_ref()
                .unwrap_or(&String::from("<no-namespace>")),
            self.0
                .meta()
                .name
                .as_ref()
                .unwrap_or(&String::from("<no-name>"))
        )
    }
}

impl std::fmt::Debug for SomeResource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("SomeResource")
            .field(&self.get_key())
            .finish()
    }
}

impl From<apps::v1::Deployment> for SomeResource {
    fn from(deployment: apps::v1::Deployment) -> Self {
        SomeResource(deployment)
    }
}

impl PartialEq for SomeResource {
    fn eq(&self, other: &Self) -> bool {
        self.get_key() == other.get_key()
    }
}

impl Eq for SomeResource {}

impl PartialOrd for SomeResource {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.get_key().partial_cmp(&other.get_key())
    }
}

impl Ord for SomeResource {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.get_key().cmp(&other.get_key())
    }
}

#[derive(Debug, Deserialize)]
struct VersionstoreEntry {
    container: String,
    path: String,
}

type VersionstoreEntries = Vec<VersionstoreEntry>;

trait VersionstoreResource {
    fn versionstore_data(&self) -> Option<VersionstoreEntries>;
}

impl VersionstoreResource for apps::v1::Deployment {
    fn versionstore_data(&self) -> Option<VersionstoreEntries> {
        let meta = self.meta();
        let annotations = meta.annotations.as_ref()?;
        let data = annotations.get(VERSIONSTORE_ANNOTATION)?;

        let vs_data = serde_json::from_str(data);

        match vs_data {
            Ok(val) => Some(val),
            Err(err) => {
                error!(
                    "failed parsing json in {}/{}: {}",
                    meta.namespace
                        .as_ref()
                        .unwrap_or(&String::from("<missing>")),
                    meta.name.as_ref().unwrap_or(&String::from("<missing>")),
                    err,
                );
                None
            }
        }
    }
}

#[derive(Debug)]
enum StorageWorkerEvent {
    VersionstoreUpdate(VersionstoreUpdate),
    ApiResourceUpdate(SomeResource),
    ApiResourceRemove(SomeResource),
}

type StorageMap = BTreeMap<String, BTreeSet<SomeResource>>;

pub struct Reconciler {
    client: Client,
    versions_receiver: mpsc::Receiver<VersionstoreUpdate>,
    graceful_shutdown_selector: BoxFuture<'static, ()>,
}

impl Reconciler {
    pub async fn new(
        versions_receiver: mpsc::Receiver<VersionstoreUpdate>,
        graceful_shutdown_selector: BoxFuture<'static, ()>,
    ) -> Result<Self> {
        let client = Client::try_default().await.context(K8SClientSnafu)?;

        Ok(Reconciler {
            client,
            versions_receiver,
            graceful_shutdown_selector,
        })
    }

    fn update_resource(deploys: &mut StorageMap, resource: SomeResource) {
        let data = resource.0.versionstore_data();

        match data {
            None => {
                info!("{} is not using versionstore", resource.get_key());
            }
            Some(entries) => {
                let mut known_paths = BTreeSet::new();
                for entry in entries {
                    let set = match deploys.contains_key(&entry.path) {
                        true => deploys.get_mut(&entry.path),
                        false => {
                            deploys.insert(entry.path.clone(), BTreeSet::new());
                            deploys.get_mut(&entry.path)
                        }
                    }
                    // unwrap() validated: we upsert the key
                    .unwrap();
                    set.insert(resource.clone());
                    known_paths.insert(entry.path.clone());
                    debug!("{:?} is watched for {}", resource, entry.path);
                }
                for (key, value) in deploys.iter_mut() {
                    if known_paths.contains(key) {
                        continue;
                    }
                    if value.remove(&resource) {
                        debug!("removed {:?} watch for {}", resource, key);
                    }
                }
            }
        }
    }

    fn remove_resource(deploys: &mut StorageMap, resource: SomeResource) {
        let data = resource.0.versionstore_data();

        match data {
            None => (),
            Some(entries) => {
                for entry in entries {
                    if let Some(set) = deploys.get_mut(&entry.path) {
                        set.remove(&resource);
                    }
                }
            }
        }
    }

    fn patch_resources(deploys: &StorageMap, v: VersionstoreUpdate) -> Option<()> {
        let resources = deploys.get(&v.path)?;
        for resource in resources {
            info!(
                "will update {} container {} to {}",
                resource.get_key(),
                "<unimplemented>",
                v.value
            );
        }

        None
    }

    fn process_event(deploys: &mut StorageMap, evt: StorageWorkerEvent) {
        use StorageWorkerEvent::*;
        debug!("got event {:?}", evt);
        match evt {
            VersionstoreUpdate(vu) => Self::patch_resources(&deploys, vu).or(Some(())).unwrap(),
            ApiResourceUpdate(r) => Self::update_resource(deploys, r),
            ApiResourceRemove(r) => Self::remove_resource(deploys, r),
        }
    }

    pub async fn run(self) {
        let Reconciler {
            mut versions_receiver,
            graceful_shutdown_selector,
            client,
        } = self;

        let (storage_shutdown_tx, mut storage_shutdown_rx) = mpsc::channel(1);
        let (api_shutdown_tx, api_shutdown_rx) = oneshot::channel();
        let (storage_tx, mut storage_rx) = mpsc::channel::<StorageWorkerEvent>(1);

        // wait until graceful shutdown is called and notify
        tokio::spawn(async move {
            graceful_shutdown_selector.await;
            storage_shutdown_tx.send(()).await.unwrap_or_default();
            api_shutdown_tx.send(()).unwrap_or_default();
        });

        // process storage events until `storage_shutdown_rx` is called
        let storage_handle = tokio::spawn(async move {
            let deploys = Mutex::new(BTreeMap::new());
            loop {
                tokio::select! {
                    evt = storage_rx.recv() => {
                        match evt {
                            Some(evt) => {
                                let deploys = &mut deploys.lock().unwrap();
                                Self::process_event(deploys, evt)
                            },
                            None => { return; }
                        }
                    }
                    _  = storage_shutdown_rx.recv() => {
                        break;
                    }
                }
            }
        });

        // process versionstore channel updates. Will shut down when the pubsub watcher terminates
        let receiver_handle = tokio::spawn((|| {
            let storage_tx = storage_tx.clone();

            async move {
                loop {
                    let version = versions_receiver.recv().await;
                    match version {
                        Some(vu) => {
                            storage_tx
                                .send(StorageWorkerEvent::VersionstoreUpdate(vu))
                                .await
                                .unwrap_or_default();
                        }
                        None => break,
                    }
                }
            }
        })());

        // process k8s api events until `api_shutdown_rx` is called
        let api_watcher_handle = tokio::spawn((|| {
            let storage_tx = storage_tx.clone();

            let deploys: Api<apps::v1::Deployment> = Api::all(client.clone());
            let watch_stream =
                watcher(deploys.clone(), ListParams::default()).take_until(api_shutdown_rx);

            async move {
                pin_mut!(watch_stream);

                while let Some(evt) = watch_stream.next().await {
                    match evt {
                        Ok(evt) => match evt {
                            watcher::Event::Applied(deployment) => {
                                storage_tx
                                    .send(StorageWorkerEvent::ApiResourceUpdate(
                                        SomeResource::from(deployment),
                                    ))
                                    .await
                                    .unwrap_or_default();
                            }
                            watcher::Event::Restarted(deployments) => {
                                for deployment in deployments {
                                    storage_tx
                                        .send(StorageWorkerEvent::ApiResourceUpdate(
                                            SomeResource::from(deployment),
                                        ))
                                        .await
                                        .unwrap_or_default();
                                }
                            }
                            watcher::Event::Deleted(deployment) => {
                                storage_tx
                                    .send(StorageWorkerEvent::ApiResourceRemove(
                                        SomeResource::from(deployment),
                                    ))
                                    .await
                                    .unwrap_or_default();
                            }
                        },
                        Err(er) => {
                            eprintln!("watcher error: {}", er);
                        }
                    }
                }
            }
        })());

        join_all(vec![storage_handle, receiver_handle, api_watcher_handle]).await;
    }
}
