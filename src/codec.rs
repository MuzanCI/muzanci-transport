use bytes::{Buf, BufMut, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

use crate::mux::Frame;

#[derive(Debug, thiserror::Error)]
pub enum CodecError {
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("Frame too large: {0}")]
    FrameTooLarge(usize),
    #[error("Serialization error: {0}")]
    SerializationError(#[from] postcard::Error),
}

pub struct Codec;

impl Codec {
    pub fn new() -> Self {
        Codec
    }
}

const HEADER_SIZE: usize = 2 + 16; // 2 bytes for payload length, 16 bytes for channel ID
const PAYLOAD_MAX_SIZE: usize = 1 << 16; // 64 kilobytes

impl Encoder<Frame> for Codec {
    type Error = CodecError;

    fn encode(&mut self, item: Frame, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let payload: Vec<u8> = postcard::to_allocvec(&item.message)?;
        if payload.len() > PAYLOAD_MAX_SIZE {
            return Err(CodecError::FrameTooLarge(payload.len()));
        }
        dst.reserve(HEADER_SIZE + payload.len());
        dst.put_u16(payload.len() as u16);
        dst.put_slice(item.channel_id.as_bytes());
        dst.put_slice(&payload);
        Ok(())
    }
}

impl Decoder for Codec {
    type Item = Frame;
    type Error = CodecError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() < HEADER_SIZE {
            return Ok(None);
        }
        let payload_len = u16::from_be_bytes([src[0], src[1]]) as usize;
        if payload_len > PAYLOAD_MAX_SIZE {
            return Err(CodecError::FrameTooLarge(payload_len));
        }
        let channel_id = uuid::Uuid::from_slice(&src[2..18]).unwrap();
        let total = HEADER_SIZE + payload_len;
        if src.len() < total {
            return Ok(None);
        }
        src.advance(HEADER_SIZE);
        let payload = src.split_to(payload_len);
        let message = postcard::from_bytes(&payload)?;
        Ok(Some(Frame {
            channel_id,
            message,
        }))
    }
}
