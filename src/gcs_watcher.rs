use std::str;

use awaitgroup::WaitGroup;
use futures::future::BoxFuture;
use google_cloud::{pubsub, storage};
use serde::Deserialize;
use snafu::{ResultExt, Snafu};
use tokio::sync::mpsc;
use tracing::{error, warn};

#[derive(Debug, Snafu)]

pub enum Error {
    #[snafu(display("pubsub setup error: {}", source))]
    PubSubSetup { source: pubsub::Error },
    #[snafu(display("pubsub topic error: {}", source))]
    PubSubTopic { source: pubsub::Error },
    #[snafu(display("pubsub topic '{topic_name}' not found"))]
    TopicNotFound { topic_name: String },
    #[snafu(display("pubsub subscription error: {}", source))]
    PubSubSubscription { source: pubsub::Error },
    #[snafu(display("pubsub create subscription error: {}", source))]
    PubSubCreateSubscription { source: pubsub::Error },
    #[snafu(display("utf8 error: {}", source))]
    Utf8 { source: str::Utf8Error },
    #[snafu(display("utf8 error: {}", source))]
    FromUtf8 { source: std::string::FromUtf8Error },
    #[snafu(display("pubsub ack error: {}", source))]
    Ack { source: pubsub::Error },
    #[snafu(display("json deserialize error: {}", source))]
    Json { source: serde_json::Error },
    #[snafu(display("GCS setup error: {}", source))]
    GCSSetup { source: pubsub::Error },
    #[snafu(display("GCS bucket error: {}", source))]
    GCSBucket { source: pubsub::Error },
    #[snafu(display("GCS object error: {}", source))]
    GCSObject { source: pubsub::Error },
    #[snafu(display("GCS object read error: {}", source))]
    GCSObjectRead { source: pubsub::Error },
    #[snafu(display("PubSub receive returned no message"))]
    PubSubRecv {},
}

type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Deserialize)]
pub struct StorageMessage {
    pub kind: String,
    pub id: String,
    #[serde(rename = "selfLink")]
    pub self_link: String,
    pub name: String,
    pub bucket: String,
    pub generation: String,
    pub metageneration: String,
    #[serde(rename = "contentType")]
    pub content_type: String,
    #[serde(rename = "timeCreated")]
    pub time_created: String,
    pub updated: String,
    #[serde(rename = "storageClass")]
    pub storage_class: String,
    #[serde(rename = "timeStorageClassUpdated")]
    pub time_storage_class_updated: String,
    pub size: String,
    #[serde(rename = "md5Hash")]
    pub md5_hash: String,
    #[serde(rename = "mediaLink")]
    pub media_link: String,
    pub crc32c: String,
    pub etag: String,
}

#[derive(Debug)]
pub struct VersionstoreUpdate {
    pub path: String,
    pub value: String,
}

pub struct GCSWatcher {
    subscription: pubsub::Subscription,
    storage_bucket: storage::Bucket,
    graceful_shutdown_selector: BoxFuture<'static, ()>,
    versions_sender: mpsc::Sender<VersionstoreUpdate>,
}

impl GCSWatcher {
    pub async fn new<S: AsRef<str>, I: Into<String> + Clone>(
        project: I,
        pubsub_topic: S,
        pubsub_subscription: S,
        gcs_bucket: S,
        versions_sender: mpsc::Sender<VersionstoreUpdate>,
        graceful_shutdown_selector: BoxFuture<'static, ()>,
    ) -> Result<Self> {
        let mut pubsub_client = pubsub::Client::new(project.clone())
            .await
            .context(PubSubSetupSnafu)?;

        let mut topic = pubsub_client
            .topic(pubsub_topic.as_ref())
            .await
            .context(PubSubTopicSnafu)?
            .ok_or(Error::TopicNotFound {
                topic_name: String::from(pubsub_topic.as_ref()),
            })?;

        let subscription = pubsub_client
            .subscription(pubsub_subscription.as_ref())
            .await;

        let subscription = if matches!(subscription, Err(google_cloud::error::Error::Status(ref s)) if s.code() == tonic::Code::NotFound)
        {
            let config = pubsub::SubscriptionConfig::default();
            let v = topic
                .create_subscription(pubsub_subscription.as_ref(), config)
                .await
                .context(PubSubCreateSubscriptionSnafu)?;
            v
        } else {
            if let Err(ref e) = subscription {
                eprintln!("{:?}", e);
            }
            // unwrap() validated: the library code will always return Ok(Some(_))
            subscription.context(PubSubSubscriptionSnafu)?.unwrap()
        };

        let mut storage_client = storage::Client::new(project).await.context(GCSSetupSnafu)?;

        let storage_bucket = storage_client
            .bucket(gcs_bucket.as_ref(), true)
            .await
            .context(GCSBucketSnafu)?;

        Ok(GCSWatcher {
            subscription,
            graceful_shutdown_selector,
            storage_bucket,
            versions_sender,
        })
    }

    pub async fn run(self) {
        macro_rules! unwrap_or_err {
            ( $e:expr ) => {
                match $e {
                    Ok(val) => val,
                    Err(err) => {
                        error!("{}", err);
                        return;
                    }
                }
            };
        }

        let GCSWatcher {
            mut subscription,
            graceful_shutdown_selector,
            storage_bucket,
            versions_sender,
        } = self;

        let (tx, mut rx) = mpsc::channel(1);

        tokio::spawn(async move {
            graceful_shutdown_selector.await;
            tx.send(()).await.unwrap_or_default();
        });

        let mut wg = WaitGroup::new();

        loop {
            tokio::select! {
                message = subscription.receive() => {
                    tokio::spawn((|| {
                        // TODO: can we not use spawn so the bucket doesn't need to move?
                        let mut bucket = storage_bucket.clone();
                        let worker = wg.worker();
                        let versions_sender = versions_sender.clone();

                        async move {
                            let mut message = unwrap_or_err!(message.ok_or(Error::PubSubRecv {}));
                            unwrap_or_err!(message.ack().await.context(AckSnafu));
                            let json = unwrap_or_err!(str::from_utf8(message.data()).context(Utf8Snafu));
                            // TODO: dedup messages on bucket/name
                            let storage_message: StorageMessage =
                                unwrap_or_err!(serde_json::from_str(json).context(JsonSnafu));
                            if storage_message.bucket != bucket.name() {
                                warn!("received a notification for bucket {} while expecting bucket {}", bucket.name(), storage_message.bucket);
                                return;
                            }
                            let mut obj = unwrap_or_err!(bucket
                                .object(&storage_message.name)
                                .await
                                .context(GCSObjectSnafu));

                            let data = unwrap_or_err!(obj.get().await.context(GCSObjectReadSnafu));
                            let value = unwrap_or_err!(String::from_utf8(data).context(FromUtf8Snafu));

                            let update = VersionstoreUpdate {
                                path: storage_message.name,
                                value: value,
                            };
                            unwrap_or_err!(versions_sender.send(update).await);

                            worker.done();
                        }
                    })());
                }
                _  = rx.recv() => {
                    break;
                }
            };
        }

        wg.wait().await;
    }
}
