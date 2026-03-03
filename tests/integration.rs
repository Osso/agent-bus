use std::path::PathBuf;
use std::time::Duration;

use agent_bus::{Broker, BusClient, Frame, Target};
use tokio::net::UnixListener;
use tokio::task::JoinHandle;

/// Spawn a broker on a unique temp socket. Returns the socket path and task handle.
async fn start_broker() -> (PathBuf, JoinHandle<()>) {
    let socket = std::env::temp_dir().join(format!("agent-bus-test-{}.sock", uuid::Uuid::new_v4()));
    let _ = std::fs::remove_file(&socket);
    let listener = UnixListener::bind(&socket).unwrap();
    let path = socket.clone();
    let handle = tokio::spawn(async move {
        let _ = Broker::new().run(listener).await;
    });
    // Give the broker a moment to start accepting connections
    tokio::time::sleep(Duration::from_millis(50)).await;
    (path, handle)
}

#[tokio::test]
async fn test_register_and_send() {
    let (socket, _handle) = start_broker().await;

    let alice = BusClient::connect(&socket, "alice").await.unwrap();
    let mut bob = BusClient::connect(&socket, "bob").await.unwrap();

    // alice sends to bob
    alice
        .send(Target::Named("bob".into()), "greeting", serde_json::json!({"text": "hello"}))
        .await
        .unwrap();

    // bob receives the message (skip any PeerConnected events first)
    let msg = recv_message(&mut bob).await;
    assert_eq!(msg.from, "alice");
    assert_eq!(msg.kind, "greeting");
    assert_eq!(msg.payload["text"], "hello");

    let _ = std::fs::remove_file(&socket);
}

#[tokio::test]
async fn test_topic_pubsub() {
    let (socket, _handle) = start_broker().await;

    let pub_client = BusClient::connect(&socket, "pub").await.unwrap();
    let mut sub1 = BusClient::connect(&socket, "sub1").await.unwrap();
    let mut sub2 = BusClient::connect(&socket, "sub2").await.unwrap();

    sub1.subscribe("events").await.unwrap();
    sub2.subscribe("events").await.unwrap();

    // Small delay so subscribe frames are processed before publish
    tokio::time::sleep(Duration::from_millis(20)).await;

    pub_client
        .send(Target::Topic("events".into()), "event", serde_json::json!({"n": 1}))
        .await
        .unwrap();

    let m1 = recv_message(&mut sub1).await;
    let m2 = recv_message(&mut sub2).await;

    assert_eq!(m1.from, "pub");
    assert_eq!(m1.kind, "event");
    assert_eq!(m2.from, "pub");
    assert_eq!(m2.kind, "event");

    let _ = std::fs::remove_file(&socket);
}

#[tokio::test]
async fn test_broadcast() {
    let (socket, _handle) = start_broker().await;

    let mut sender = BusClient::connect(&socket, "sender").await.unwrap();
    let mut recv1 = BusClient::connect(&socket, "recv1").await.unwrap();
    let mut recv2 = BusClient::connect(&socket, "recv2").await.unwrap();

    sender
        .send(Target::Broadcast, "shout", serde_json::json!({"msg": "hi all"}))
        .await
        .unwrap();

    let m1 = recv_message(&mut recv1).await;
    let m2 = recv_message(&mut recv2).await;

    assert_eq!(m1.from, "sender");
    assert_eq!(m1.kind, "shout");
    assert_eq!(m2.from, "sender");
    assert_eq!(m2.kind, "shout");

    // sender should NOT receive its own broadcast — verify no message arrives within timeout
    let timeout_result =
        tokio::time::timeout(Duration::from_millis(100), recv_message(&mut sender)).await;
    assert!(timeout_result.is_err(), "sender must not receive its own broadcast");

    let _ = std::fs::remove_file(&socket);
}

#[tokio::test]
async fn test_duplicate_name_rejected() {
    let (socket, _handle) = start_broker().await;

    let _alice = BusClient::connect(&socket, "alice").await.unwrap();

    // Second client trying to register as "alice" should get an error
    let result = BusClient::connect(&socket, "alice").await;
    assert!(result.is_err(), "duplicate name must be rejected");

    let _ = std::fs::remove_file(&socket);
}

#[tokio::test]
async fn test_peer_lifecycle() {
    let (socket, _handle) = start_broker().await;

    let mut watcher = BusClient::connect(&socket, "watcher").await.unwrap();

    // joiner connects — watcher should receive PeerConnected
    let joiner = BusClient::connect(&socket, "joiner").await.unwrap();

    let connected = recv_peer_connected(&mut watcher).await;
    assert_eq!(connected, "joiner");

    // joiner disconnects by dropping
    drop(joiner);

    let disconnected = recv_peer_disconnected(&mut watcher).await;
    assert_eq!(disconnected, "joiner");

    let _ = std::fs::remove_file(&socket);
}

#[tokio::test]
async fn test_named_unknown_target() {
    let (socket, _handle) = start_broker().await;

    let mut alice = BusClient::connect(&socket, "alice").await.unwrap();

    alice
        .send(Target::Named("nobody".into()), "ping", serde_json::json!(null))
        .await
        .unwrap();

    // alice should receive an Error frame back
    let frame = tokio::time::timeout(Duration::from_millis(500), alice.recv())
        .await
        .expect("timed out waiting for error frame")
        .expect("channel closed");

    match frame {
        Frame::Error { message } => {
            assert!(message.contains("nobody"), "error should mention the unknown target");
        }
        other => panic!("expected Error frame, got {:?}", other),
    }

    let _ = std::fs::remove_file(&socket);
}

#[tokio::test]
async fn test_unsubscribe() {
    let (socket, _handle) = start_broker().await;

    let publisher = BusClient::connect(&socket, "pub").await.unwrap();
    let mut sub = BusClient::connect(&socket, "sub").await.unwrap();

    sub.subscribe("events").await.unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;

    // First message should arrive
    publisher
        .send(Target::Topic("events".into()), "ev", serde_json::json!(1))
        .await
        .unwrap();
    let m = recv_message(&mut sub).await;
    assert_eq!(m.payload, serde_json::json!(1));

    // Unsubscribe and send again — should NOT arrive
    sub.unsubscribe("events").await.unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;

    publisher
        .send(Target::Topic("events".into()), "ev", serde_json::json!(2))
        .await
        .unwrap();

    let timeout = tokio::time::timeout(Duration::from_millis(200), recv_message(&mut sub)).await;
    assert!(timeout.is_err(), "unsubscribed client must not receive topic messages");

    let _ = std::fs::remove_file(&socket);
}

#[tokio::test]
async fn test_topic_excludes_sender() {
    let (socket, _handle) = start_broker().await;

    let mut pubsub = BusClient::connect(&socket, "pubsub").await.unwrap();
    let mut other = BusClient::connect(&socket, "other").await.unwrap();

    // Both subscribe to same topic
    pubsub.subscribe("chat").await.unwrap();
    other.subscribe("chat").await.unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;

    // pubsub publishes to the topic it's subscribed to
    pubsub
        .send(Target::Topic("chat".into()), "msg", serde_json::json!("hello"))
        .await
        .unwrap();

    // other receives it
    let m = recv_message(&mut other).await;
    assert_eq!(m.from, "pubsub");

    // pubsub must NOT receive its own message
    let timeout =
        tokio::time::timeout(Duration::from_millis(200), recv_message(&mut pubsub)).await;
    assert!(timeout.is_err(), "sender must not receive its own topic message");

    let _ = std::fs::remove_file(&socket);
}

#[tokio::test]
async fn test_multiple_topics() {
    let (socket, _handle) = start_broker().await;

    let publisher = BusClient::connect(&socket, "pub").await.unwrap();
    let mut sub = BusClient::connect(&socket, "sub").await.unwrap();

    sub.subscribe("alpha").await.unwrap();
    sub.subscribe("beta").await.unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;

    publisher
        .send(Target::Topic("alpha".into()), "a", serde_json::json!("from-alpha"))
        .await
        .unwrap();
    publisher
        .send(Target::Topic("beta".into()), "b", serde_json::json!("from-beta"))
        .await
        .unwrap();

    let m1 = recv_message(&mut sub).await;
    let m2 = recv_message(&mut sub).await;

    let kinds: Vec<&str> = vec![m1.kind.as_str(), m2.kind.as_str()];
    assert!(kinds.contains(&"a"), "should receive from alpha topic");
    assert!(kinds.contains(&"b"), "should receive from beta topic");

    let _ = std::fs::remove_file(&socket);
}

#[tokio::test]
async fn test_disconnect_cleans_topic_subscriptions() {
    let (socket, _handle) = start_broker().await;

    let publisher = BusClient::connect(&socket, "pub").await.unwrap();
    let mut sub1 = BusClient::connect(&socket, "sub1").await.unwrap();
    let sub2 = BusClient::connect(&socket, "sub2").await.unwrap();

    sub1.subscribe("events").await.unwrap();
    sub2.subscribe("events").await.unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;

    // Drop sub2 — broker should remove it from topic subscribers
    drop(sub2);
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Publishing should still work for sub1 without errors
    publisher
        .send(Target::Topic("events".into()), "ev", serde_json::json!("still works"))
        .await
        .unwrap();

    let m = recv_message(&mut sub1).await;
    assert_eq!(m.payload, serde_json::json!("still works"));

    let _ = std::fs::remove_file(&socket);
}

#[tokio::test]
async fn test_client_name() {
    let (socket, _handle) = start_broker().await;
    let client = BusClient::connect(&socket, "my-agent").await.unwrap();
    assert_eq!(client.name(), "my-agent");
    let _ = std::fs::remove_file(&socket);
}

#[tokio::test]
async fn test_bidirectional_named_messages() {
    let (socket, _handle) = start_broker().await;

    let mut alice = BusClient::connect(&socket, "alice").await.unwrap();
    let mut bob = BusClient::connect(&socket, "bob").await.unwrap();

    // alice -> bob
    alice
        .send(Target::Named("bob".into()), "ping", serde_json::json!("from alice"))
        .await
        .unwrap();
    let m = recv_message(&mut bob).await;
    assert_eq!(m.from, "alice");
    assert_eq!(m.kind, "ping");

    // bob -> alice
    bob.send(Target::Named("alice".into()), "pong", serde_json::json!("from bob"))
        .await
        .unwrap();
    let m = recv_message(&mut alice).await;
    assert_eq!(m.from, "bob");
    assert_eq!(m.kind, "pong");

    let _ = std::fs::remove_file(&socket);
}

#[tokio::test]
async fn test_complex_json_payload() {
    let (socket, _handle) = start_broker().await;

    let alice = BusClient::connect(&socket, "alice").await.unwrap();
    let mut bob = BusClient::connect(&socket, "bob").await.unwrap();

    let payload = serde_json::json!({
        "nested": {"deep": {"value": 42}},
        "array": [1, "two", null, true],
        "empty": {},
        "unicode": "caf\u{00e9} \u{1f600}"
    });

    alice
        .send(Target::Named("bob".into()), "complex", payload.clone())
        .await
        .unwrap();

    let m = recv_message(&mut bob).await;
    assert_eq!(m.payload, payload);

    let _ = std::fs::remove_file(&socket);
}

#[tokio::test]
async fn test_topic_no_subscribers() {
    let (socket, _handle) = start_broker().await;

    let mut publisher = BusClient::connect(&socket, "pub").await.unwrap();

    // Publish to a topic with no subscribers — should not error
    publisher
        .send(Target::Topic("ghost".into()), "ev", serde_json::json!(null))
        .await
        .unwrap();

    // No error frame should come back
    let timeout =
        tokio::time::timeout(Duration::from_millis(200), recv_message(&mut publisher)).await;
    assert!(timeout.is_err(), "publishing to empty topic should silently succeed");

    let _ = std::fs::remove_file(&socket);
}

// --- helpers ---

/// Drain frames until we get a Message frame, skipping peer events.
async fn recv_message(client: &mut BusClient) -> agent_bus::BusMessage {
    loop {
        let frame = tokio::time::timeout(Duration::from_millis(500), client.recv())
            .await
            .expect("timed out waiting for message")
            .expect("channel closed");
        match frame {
            Frame::Message(msg) => return msg,
            Frame::PeerConnected { .. } | Frame::PeerDisconnected { .. } => continue,
            other => panic!("unexpected frame: {:?}", other),
        }
    }
}

/// Drain frames until we get a PeerConnected event, return the peer name.
async fn recv_peer_connected(client: &mut BusClient) -> String {
    loop {
        let frame = tokio::time::timeout(Duration::from_millis(500), client.recv())
            .await
            .expect("timed out waiting for PeerConnected")
            .expect("channel closed");
        match frame {
            Frame::PeerConnected { name } => return name,
            Frame::Message(_) | Frame::PeerDisconnected { .. } => continue,
            other => panic!("unexpected frame: {:?}", other),
        }
    }
}

/// Drain frames until we get a PeerDisconnected event, return the peer name.
async fn recv_peer_disconnected(client: &mut BusClient) -> String {
    loop {
        let frame = tokio::time::timeout(Duration::from_millis(500), client.recv())
            .await
            .expect("timed out waiting for PeerDisconnected")
            .expect("channel closed");
        match frame {
            Frame::PeerDisconnected { name } => return name,
            Frame::Message(_) | Frame::PeerConnected { .. } => continue,
            other => panic!("unexpected frame: {:?}", other),
        }
    }
}
