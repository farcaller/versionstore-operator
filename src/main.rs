use std::process;

use clap::Parser;
use flexi_logger::{trc::LogSpecAsFilter, LogSpecification};
use futures::{join, FutureExt};
use snafu::{ResultExt, Whatever};
use tokio::sync::mpsc;
use tracing_subscriber::FmtSubscriber;

mod gcs_watcher;
mod reconciler;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Cli {
    #[clap(long)]
    /// GCP project that contains the GCS bucket and the PubSub subscription.
    gcp_project: String,

    #[clap(long)]
    /// GCS storage bucket that contains the image versions.
    gcp_gcs_bucket: String,

    #[clap(long)]
    /// Cloud PubSub topic that GCS publishes updates to.
    gcp_pubsub_topic: String,

    #[clap(long)]
    /// The existing Cloud PubSub subscription for the topic.
    gcp_pubsub_subscription: String,
}

async fn shutdown_signal() {
    futures::future::select(
        tokio::signal::ctrl_c().map(|_| ()).boxed(),
        #[cfg(unix)]
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .unwrap()
            .recv()
            .map(|_| ())
            .boxed(),
        // Assume that ctrl_c is enough on non-Unix platforms (such as Windows)
        #[cfg(not(unix))]
        futures::future::pending::<()>(),
    )
    .await;
}

fn setup_logging() -> Result<(), Whatever> {
    let initial_logspec = LogSpecification::env().unwrap();

    let subscriber_builder = FmtSubscriber::builder()
        .with_env_filter(LogSpecAsFilter(initial_logspec))
        .with_filter_reloading();
    tracing::subscriber::set_global_default(subscriber_builder.finish())
        .with_whatever_context(|e| format!("error setting logger: {}", e))?;

    Ok(())
}

async fn main_loop() -> Result<(), Whatever> {
    let args = Cli::parse();

    setup_logging()?;

    let (versions_sender, versions_receiver) = mpsc::channel(1);

    let shutdown = shutdown_signal().shared();

    let watcher = gcs_watcher::GCSWatcher::new(
        args.gcp_project,
        args.gcp_pubsub_topic,
        args.gcp_pubsub_subscription,
        args.gcp_gcs_bucket,
        versions_sender,
        shutdown.clone().boxed(),
    )
    .await
    .with_whatever_context(|e| format!("error creating watcher: {}", e))?;

    let reconc = reconciler::Reconciler::new(versions_receiver, shutdown.clone().boxed())
        .await
        .with_whatever_context(|e| format!("error creating reconciler: {}", e))?;

    let watcher_loop = watcher.run();
    let reconciler_loop = reconc.run();
    join!(watcher_loop, reconciler_loop);

    Ok(())
}

#[tokio::main]
async fn main() {
    main_loop()
        .await
        .map_err(|e| {
            eprintln!("{}", e);
            process::exit(1);
        })
        .unwrap();
}
