use agent_bus::{Bus, BusError, BusMessage};
use serde_json::json;

#[test]
fn register_and_send() {
    let bus = Bus::new();
    let alice = bus.register("alice").unwrap();
    let mut bob = bus.register("bob").unwrap();

    alice.send("bob", "ping", json!({"n": 1})).unwrap();

    let msg = bob.try_recv().unwrap();
    assert_eq!(msg.from, "alice");
    assert_eq!(msg.to, "bob");
    assert_eq!(msg.kind, "ping");
    assert_eq!(msg.payload, json!({"n": 1}));
}

#[test]
fn duplicate_name_rejected() {
    let bus = Bus::new();
    let _alice = bus.register("alice").unwrap();

    let err = bus.register("alice").unwrap_err();
    assert!(matches!(err, BusError::NameTaken(_)));
}

#[test]
fn unknown_recipient() {
    let bus = Bus::new();
    let alice = bus.register("alice").unwrap();

    let err = alice.send("nobody", "ping", json!(null)).unwrap_err();
    assert!(matches!(err, BusError::UnknownRecipient(_)));
}

#[test]
fn drop_deregisters() {
    let bus = Bus::new();
    let alice = bus.register("alice").unwrap();
    drop(alice);

    // Name is available again after drop
    let _alice2 = bus.register("alice").unwrap();
}

#[tokio::test]
async fn recv_none_when_deregistered() {
    let bus = Bus::new();
    let mut bob = bus.register("bob").unwrap();

    // Deregistering bob (removing sender from registry) closes the channel
    bus.deregister("bob");

    assert!(bob.recv().await.is_none());
}

#[test]
fn bidirectional_messaging() {
    let bus = Bus::new();
    let mut alice = bus.register("alice").unwrap();
    let mut bob = bus.register("bob").unwrap();

    alice.send("bob", "ping", json!(null)).unwrap();
    bob.send("alice", "pong", json!(null)).unwrap();

    let from_bob = alice.try_recv().unwrap();
    assert_eq!(from_bob.kind, "pong");
    assert_eq!(from_bob.from, "bob");

    let from_alice = bob.try_recv().unwrap();
    assert_eq!(from_alice.kind, "ping");
    assert_eq!(from_alice.from, "alice");
}

#[test]
fn multiple_messages_ordered() {
    let bus = Bus::new();
    let alice = bus.register("alice").unwrap();
    let mut bob = bus.register("bob").unwrap();

    for i in 0..5 {
        alice.send("bob", "seq", json!({"n": i})).unwrap();
    }

    for i in 0..5 {
        let msg = bob.try_recv().unwrap();
        assert_eq!(msg.payload["n"], i);
    }

    assert!(bob.try_recv().is_none());
}

#[test]
fn complex_payload() {
    let bus = Bus::new();
    let alice = bus.register("alice").unwrap();
    let mut bob = bus.register("bob").unwrap();

    let payload = json!({
        "action": "deploy",
        "targets": ["web-1", "web-2"],
        "config": {
            "replicas": 3,
            "env": {"RUST_LOG": "debug"}
        }
    });

    alice.send("bob", "deploy", payload.clone()).unwrap();

    let msg = bob.try_recv().unwrap();
    assert_eq!(msg.payload, payload);
}

#[test]
fn message_has_uuid() {
    let bus = Bus::new();
    let alice = bus.register("alice").unwrap();
    let mut bob = bus.register("bob").unwrap();

    let id = alice.send("bob", "ping", json!(null)).unwrap();
    let msg = bob.try_recv().unwrap();
    assert_eq!(msg.id, id);
}

#[test]
fn mailbox_name() {
    let bus = Bus::new();
    let alice = bus.register("alice").unwrap();
    assert_eq!(alice.name(), "alice");
}

#[tokio::test]
async fn async_recv() {
    let bus = Bus::new();
    let alice = bus.register("alice").unwrap();
    let mut bob = bus.register("bob").unwrap();

    alice.send("bob", "hello", json!(null)).unwrap();

    let msg = bob.recv().await.unwrap();
    assert_eq!(msg.kind, "hello");
}

#[test]
fn send_after_recipient_dropped() {
    let bus = Bus::new();
    let alice = bus.register("alice").unwrap();
    let bob = bus.register("bob").unwrap();
    drop(bob);

    let err = alice.send("bob", "ping", json!(null)).unwrap_err();
    assert!(matches!(err, BusError::UnknownRecipient(_)));
}

#[test]
fn bus_clone_shares_state() {
    let bus = Bus::new();
    let bus2 = bus.clone();

    let _alice = bus.register("alice").unwrap();
    let err = bus2.register("alice").unwrap_err();
    assert!(matches!(err, BusError::NameTaken(_)));

    let _bob = bus2.register("bob").unwrap();
    let alice2 = bus.register("alice2").unwrap();
    alice2.send("bob", "hi", json!(null)).unwrap();
}

#[test]
fn list_registered_returns_current_names() {
    let bus = Bus::new();
    let alice = bus.register("alice").unwrap();
    let _bob = bus.register("bob").unwrap();

    let mut names = bus.list_registered();
    names.sort();
    assert_eq!(names, ["alice", "bob"]);

    drop(alice);
    assert_eq!(bus.list_registered(), ["bob"]);
}

#[test]
fn default_bus_accepts_registrations() {
    let bus = Bus::default();
    let alice = bus.register("alice").unwrap();
    let mut bob = bus.register("bob").unwrap();

    alice.send("bob", "ping", json!(null)).unwrap();

    let msg = bob.try_recv().unwrap();
    assert_eq!(msg.from, "alice");
    assert_eq!(msg.kind, "ping");
}

#[test]
fn serialization_roundtrip() {
    let msg = BusMessage {
        id: uuid::Uuid::new_v4(),
        from: "alice".into(),
        to: "bob".into(),
        kind: "test".into(),
        payload: json!({"key": "value"}),
    };

    let serialized = serde_json::to_string(&msg).unwrap();
    let deserialized: BusMessage = serde_json::from_str(&serialized).unwrap();

    assert_eq!(deserialized.from, msg.from);
    assert_eq!(deserialized.to, msg.to);
    assert_eq!(deserialized.kind, msg.kind);
    assert_eq!(deserialized.payload, msg.payload);
}
