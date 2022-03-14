use anyhow::anyhow;
use futures::{
    channel::oneshot,
    future::{self, AbortHandle},
    prelude::*,
};
use publisher::Publisher as _;
use std::{
    collections::HashMap,
    env,
    error::Error,
    io,
    net::SocketAddr,
    sync::{Arc, Mutex, RwLock},
};
use subscriber::Subscriber as _;
use tarpc::{
    client, context,
    serde_transport::tcp,
    server::{self, Channel},
};
use tokio::net::ToSocketAddrs;
use tokio_serde::formats::Json;
use tracing::info;
use tracing_subscriber::prelude::*;

pub mod subscriber {
    #[tarpc::service]
    pub trait Subscriber {
        async fn topics() -> Vec<String>;
        async fn receive(topic: String, message: String);
    }
}

pub mod publisher {
    #[tarpc::service]
    pub trait Publisher {
        async fn publish(topic: String, message: String);
    }
}

#[derive(Debug, Clone)]
struct Subscriber {
    local_addr: SocketAddr,
    topics: Vec<String>,
}

#[tarpc::server]
impl subscriber::Subscriber for Subscriber {
    async fn topics(self, _: context::Context) -> Vec<String> {
        self.topics.clone()
    }
    async fn receive(self, _: context::Context, topic: String, message: String) {
        info!(local_addr = %self.local_addr, %topic, %message, "Receive Message")
    }
}
struct SubscribeHandle(AbortHandle);
impl Drop for SubscribeHandle {
    fn drop(&mut self) {
        self.0.abort();
    }
}

impl Subscriber {
    async fn connect(
        publisher_addr: impl ToSocketAddrs,
        topics: Vec<String>,
    ) -> anyhow::Result<SubscribeHandle> {
        let publisher = tcp::connect(publisher_addr, Json::default).await?;
        let local_addr = publisher.local_addr()?;
        let mut handler = server::BaseChannel::with_defaults(publisher).requests();
        let subscriber = Subscriber { local_addr, topics };
        // the first request is for the topics being subscribed to.
        match handler.next().await {
            Some(init_topics) => init_topics?.execute(subscriber.clone().serve()).await,
            None => {
                return Err(anyhow!(
                    "[{}] Server never initialized the subscriber.",
                    local_addr
                ))
            }
        }
        let (handler, abort_handle) = future::abortable(handler.execute(subscriber.serve()));
        tokio::spawn(async move {
            match handler.await {
                Ok(()) | Err(future::Aborted) => info!(?local_addr, "subscriber shutdown"),
            }
        });
        Ok(SubscribeHandle(abort_handle))
    }
}

#[derive(Debug)]
struct Subscription {
    subscriber: subscriber::SubscriberClient,
    topics: Vec<String>,
}

#[derive(Debug, Clone)]
struct Publisher {
    clients: Arc<Mutex<HashMap<SocketAddr, Subscription>>>,
    subscriptions: Arc<RwLock<HashMap<String, HashMap<SocketAddr, subscriber::SubscriberClient>>>>,
}

struct PublisherAddrs {
    publisher: SocketAddr,
    subscriptions: SocketAddr,
}

impl Publisher {
    async fn start(self) -> io::Result<PublisherAddrs> {
        panic!("TODO")
    }
    async fn start_subscription_manager(mut self) -> io::Result<SocketAddr> {
        panic!("TODO")
    }
    async fn initialize_subscription(
        &mut self,
        subscriber_addr: SocketAddr,
        subscriber: subscriber::SubscriberClient,
    ) {
        panic!("TODO")
    }
    fn start_subscriber_gc<E: Error>(
        self,
        subscriber_addr: SocketAddr,
        client_dispatch: impl Future<Output = Result<(), E>> + Send + 'static,
        subscriber_ready: oneshot::Receiver<()>,
    ) {
        panic!("TODO")
    }
}

#[tarpc::server]
impl publisher::Publisher for Publisher {
    async fn publish(self, _: context::Context, topic: String, message: String) {
        panic!("TODO")
    }
}

fn init_tracing(service_name: &str) -> anyhow::Result<()> {
    panic!("TODO")
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // TODO
    Ok(())
}
