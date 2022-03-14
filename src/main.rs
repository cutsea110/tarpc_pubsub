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
        let mut connecting_publishers = tcp::listen("localhost:0", Json::default).await?;

        let publisher_addrs = PublisherAddrs {
            publisher: connecting_publishers.local_addr(),
            subscriptions: self.clone().start_subscription_manager().await?,
        };

        info!(publisher_addr = %publisher_addrs.publisher, "listening for publishers.");
        tokio::spawn(async move {
            // Because this is just an example, we know there will only be one publisher.
            // In more realistic code, this would be a loop to continually accept
            // new publisher connections
            let publisher = connecting_publishers.next().await.unwrap().unwrap();
            info!(publisher.peer_addr = ?publisher.peer_addr(), "publisher connected");

            server::BaseChannel::with_defaults(publisher)
                .execute(self.serve())
                .await
        });

        Ok(publisher_addrs)
    }
    async fn start_subscription_manager(mut self) -> io::Result<SocketAddr> {
        let mut connecting_subscribers = tcp::listen("localhost:0", Json::default)
            .await?
            .filter_map(|r| future::ready(r.ok()));
        let new_subscriber_addr = connecting_subscribers.get_ref().local_addr();
        info!(?new_subscriber_addr, "listening for subscribers.");

        tokio::spawn(async move {
            while let Some(conn) = connecting_subscribers.next().await {
                let subscriber_addr = conn.peer_addr().unwrap();

                let tarpc::client::NewClient {
                    client: subscriber,
                    dispatch,
                } = subscriber::SubscriberClient::new(client::Config::default(), conn);
                let (ready_tx, ready) = oneshot::channel();
                self.clone()
                    .start_subscriber_gc(subscriber_addr, dispatch, ready);

                // Populate the topics
                self.initialize_subscription(subscriber_addr, subscriber)
                    .await;

                // Signal that initialization is done.
                ready_tx.send(()).unwrap();
            }
        });

        Ok(new_subscriber_addr)
    }
    async fn initialize_subscription(
        &mut self,
        subscriber_addr: SocketAddr,
        subscriber: subscriber::SubscriberClient,
    ) {
        // Populate the topics
        if let Ok(topics) = subscriber.topics(context::current()).await {
            self.clients.lock().unwrap().insert(
                subscriber_addr,
                Subscription {
                    subscriber: subscriber.clone(),
                    topics: topics.clone(),
                },
            );

            info!(%subscriber_addr, ?topics, "subscribed to new topics.");
            let mut subscriptions = self.subscriptions.write().unwrap();
            for topic in topics {
                subscriptions
                    .entry(topic)
                    .or_insert_with(HashMap::new)
                    .insert(subscriber_addr, subscriber.clone());
            }
        }
    }
    fn start_subscriber_gc<E: Error>(
        self,
        subscriber_addr: SocketAddr,
        client_dispatch: impl Future<Output = Result<(), E>> + Send + 'static,
        subscriber_ready: oneshot::Receiver<()>,
    ) {
        tokio::spawn(async move {
            if let Err(e) = client_dispatch.await {
                info!(%subscriber_addr, error = %e, "subscriber connection broken.");
            }
            // Don't clean up the subscriber until initialization is done.
            let _ = subscriber_ready.await;
            if let Some(subscription) = self.clients.lock().unwrap().remove(&subscriber_addr) {
                info!(
                    "[{}] unsubscribing from topics: {:?}",
                    subscriber_addr, subscription.topics
                );
                let mut subscriptions = self.subscriptions.write().unwrap();
                for topic in subscription.topics {
                    let subscribers = subscriptions.get_mut(&topic).unwrap();
                    subscribers.remove(&subscriber_addr);
                    if subscribers.is_empty() {
                        subscriptions.remove(&topic);
                    }
                }
            }
        });
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
