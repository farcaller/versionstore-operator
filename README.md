<!-- omit in TOC -->

# versionstore-operator

> **A k8s operator that updates deployment pod versions based on an external
> key-value store**

[![Crates.io](https://img.shields.io/crates/v/versionstore-operator?style=flat-square)](https://crates.io/crates/versionstore-operator)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue?style=flat-square)](https://github.com/farcaller/versionstore-operator/blob/master/LICENSE)

1. [About](#about)

## About

versionstore-operator observes the changes in an external key-value store and
updates the k8s Deployment image tags. This allows one to e.g. decouple CI from
the k8s cluster, as the CI can update the tag in the version store and the
operator will then update the deployments within the cluster. 

## Usage

```shell
$ versionstore-operator --gcp-project ${PROJECT} --gcp-gcs-bucket ${BUCKET} --gcp-pubsub-topic ${TOPIC} --gcp-pubsub-subscription ${TOPIC_SUBSCRIPTION}
```

- **PROJECT**: GCP project that contains the GCS bucket and the PubSub
  subscription
- **BUCKET**: GCS storage bucket that contains the image versions
- **TOPIC**: Cloud PubSub topic that GCS publishes updates to
- **TOPIC_SUBSCRIPTION**: The existing Cloud PubSub subscription for the topic
