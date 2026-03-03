use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::message::Frame;

const MAX_FRAME_SIZE: u32 = 16 * 1024 * 1024; // 16 MB

#[derive(Debug, Error)]
pub enum WireError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("frame too large: {0} bytes")]
    FrameTooLarge(u32),
    #[error("connection closed")]
    ConnectionClosed,
}

pub async fn read_frame<R>(reader: &mut R) -> Result<Frame, WireError>
where
    R: AsyncReadExt + Unpin,
{
    let len = read_length(reader).await?;
    let buf = read_payload(reader, len).await?;
    let frame = serde_json::from_slice(&buf)?;
    Ok(frame)
}

pub async fn write_frame<W>(writer: &mut W, frame: &Frame) -> Result<(), WireError>
where
    W: AsyncWriteExt + Unpin,
{
    let buf = serde_json::to_vec(frame)?;
    let len = buf.len() as u32;
    if len > MAX_FRAME_SIZE {
        return Err(WireError::FrameTooLarge(len));
    }
    writer.write_all(&len.to_be_bytes()).await?;
    writer.write_all(&buf).await?;
    Ok(())
}

async fn read_length<R>(reader: &mut R) -> Result<u32, WireError>
where
    R: AsyncReadExt + Unpin,
{
    let mut len_buf = [0u8; 4];
    match reader.read_exact(&mut len_buf).await {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
            return Err(WireError::ConnectionClosed);
        }
        Err(e) => return Err(WireError::Io(e)),
    }
    let len = u32::from_be_bytes(len_buf);
    if len > MAX_FRAME_SIZE {
        return Err(WireError::FrameTooLarge(len));
    }
    Ok(len)
}

async fn read_payload<R>(reader: &mut R, len: u32) -> Result<Vec<u8>, WireError>
where
    R: AsyncReadExt + Unpin,
{
    let mut buf = vec![0u8; len as usize];
    match reader.read_exact(&mut buf).await {
        Ok(_) => Ok(buf),
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
            Err(WireError::ConnectionClosed)
        }
        Err(e) => Err(WireError::Io(e)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::{BusMessage, Target};
    use std::io::Cursor;
    use uuid::Uuid;

    async fn round_trip(frame: &Frame) -> Frame {
        let mut buf = Vec::new();
        write_frame(&mut buf, frame).await.unwrap();
        let mut cursor = Cursor::new(buf);
        read_frame(&mut cursor).await.unwrap()
    }

    #[tokio::test]
    async fn round_trip_register() {
        let frame = Frame::Register { name: "alice".into() };
        let got = round_trip(&frame).await;
        match got {
            Frame::Register { name } => assert_eq!(name, "alice"),
            other => panic!("expected Register, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn round_trip_registered() {
        let got = round_trip(&Frame::Registered).await;
        assert!(matches!(got, Frame::Registered));
    }

    #[tokio::test]
    async fn round_trip_subscribe() {
        let frame = Frame::Subscribe { topic: "events".into() };
        let got = round_trip(&frame).await;
        match got {
            Frame::Subscribe { topic } => assert_eq!(topic, "events"),
            other => panic!("expected Subscribe, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn round_trip_unsubscribe() {
        let frame = Frame::Unsubscribe { topic: "events".into() };
        let got = round_trip(&frame).await;
        match got {
            Frame::Unsubscribe { topic } => assert_eq!(topic, "events"),
            other => panic!("expected Unsubscribe, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn round_trip_message() {
        let frame = Frame::Message(BusMessage {
            id: Uuid::nil(),
            from: "alice".into(),
            to: Target::Named("bob".into()),
            kind: "ping".into(),
            payload: serde_json::json!({"key": "value"}),
        });
        let got = round_trip(&frame).await;
        match got {
            Frame::Message(msg) => {
                assert_eq!(msg.from, "alice");
                assert_eq!(msg.to, Target::Named("bob".into()));
                assert_eq!(msg.kind, "ping");
                assert_eq!(msg.payload["key"], "value");
            }
            other => panic!("expected Message, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn round_trip_error() {
        let frame = Frame::Error { message: "bad things".into() };
        let got = round_trip(&frame).await;
        match got {
            Frame::Error { message } => assert_eq!(message, "bad things"),
            other => panic!("expected Error, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn round_trip_peer_connected() {
        let frame = Frame::PeerConnected { name: "bob".into() };
        let got = round_trip(&frame).await;
        match got {
            Frame::PeerConnected { name } => assert_eq!(name, "bob"),
            other => panic!("expected PeerConnected, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn round_trip_peer_disconnected() {
        let frame = Frame::PeerDisconnected { name: "bob".into() };
        let got = round_trip(&frame).await;
        match got {
            Frame::PeerDisconnected { name } => assert_eq!(name, "bob"),
            other => panic!("expected PeerDisconnected, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn round_trip_broadcast_target() {
        let frame = Frame::Message(BusMessage {
            id: Uuid::nil(),
            from: "sender".into(),
            to: Target::Broadcast,
            kind: "shout".into(),
            payload: serde_json::json!(null),
        });
        let got = round_trip(&frame).await;
        match got {
            Frame::Message(msg) => assert_eq!(msg.to, Target::Broadcast),
            other => panic!("expected Message, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn round_trip_topic_target() {
        let frame = Frame::Message(BusMessage {
            id: Uuid::nil(),
            from: "pub".into(),
            to: Target::Topic("news".into()),
            kind: "update".into(),
            payload: serde_json::json!(null),
        });
        let got = round_trip(&frame).await;
        match got {
            Frame::Message(msg) => assert_eq!(msg.to, Target::Topic("news".into())),
            other => panic!("expected Message, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn connection_closed_on_empty_input() {
        let mut cursor = Cursor::new(Vec::<u8>::new());
        let err = read_frame(&mut cursor).await.unwrap_err();
        assert!(matches!(err, WireError::ConnectionClosed));
    }

    #[tokio::test]
    async fn connection_closed_on_truncated_header() {
        let mut cursor = Cursor::new(vec![0u8, 1]); // only 2 of 4 header bytes
        let err = read_frame(&mut cursor).await.unwrap_err();
        assert!(matches!(err, WireError::ConnectionClosed));
    }

    #[tokio::test]
    async fn connection_closed_on_truncated_payload() {
        // header says 100 bytes but only 5 bytes of payload
        let mut buf = Vec::new();
        buf.extend_from_slice(&100u32.to_be_bytes());
        buf.extend_from_slice(b"short");
        let mut cursor = Cursor::new(buf);
        let err = read_frame(&mut cursor).await.unwrap_err();
        assert!(matches!(err, WireError::ConnectionClosed));
    }

    #[tokio::test]
    async fn frame_too_large_on_read() {
        let huge_len = MAX_FRAME_SIZE + 1;
        let mut cursor = Cursor::new(huge_len.to_be_bytes().to_vec());
        let err = read_frame(&mut cursor).await.unwrap_err();
        match err {
            WireError::FrameTooLarge(n) => assert_eq!(n, huge_len),
            other => panic!("expected FrameTooLarge, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn invalid_json_payload() {
        let garbage = b"not valid json!!";
        let mut buf = Vec::new();
        buf.extend_from_slice(&(garbage.len() as u32).to_be_bytes());
        buf.extend_from_slice(garbage);
        let mut cursor = Cursor::new(buf);
        let err = read_frame(&mut cursor).await.unwrap_err();
        assert!(matches!(err, WireError::Json(_)));
    }
}
