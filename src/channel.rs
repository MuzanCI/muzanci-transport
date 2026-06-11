use futures::future::BoxFuture;
use tokio::sync::mpsc;

use crate::mux::Command;

use crate::mux::{Frame, MuxError};

pub type ChannelId = u64;
pub type WorkerId = u64;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum RunnerEvent {
    Started { runner_id: u64 },
    StdoutLine { runner_id: u64, line: String },
    StderrLine { runner_id: u64, line: String },
    Exited { runner_id: u64, exit_code: i32 },
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum Message {
    /// A packet of data.
    Data(#[serde(with = "serde_bytes")] Vec<u8>),

    /// A control message to open a channel.
    OpenChannelRequest { channel_id: ChannelId },

    /// A control message to acknowledge successful channel open.
    OpenChannelResponse {
        channel_id: ChannelId,
        result: Result<(), String>,
    },

    /// A control message
    /// A lifecycle event for a Runner.
    RunnerEvent(RunnerEvent),
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum ChannelState {
    Open,
    Closed,
}

pub struct ChannelHandle {
    /// The channel's unique identifier.
    channel_id: ChannelId,

    /// A channel for outbound frames, from a local task to the peer.
    frame_tx: mpsc::Sender<Frame>,

    /// A channel for commands to the mux task, such as closing the channel.
    command_tx: mpsc::Sender<Command>,

    /// A channel for inbound messages from the peer.
    message_rx: mpsc::Receiver<Message>,

    /// The state of the channel.
    state: ChannelState,
}

impl ChannelHandle {
    pub fn new(
        channel_id: ChannelId,
        frame_tx: mpsc::Sender<Frame>,
        command_tx: mpsc::Sender<Command>,
        message_rx: mpsc::Receiver<Message>,
        state: ChannelState,
    ) -> Self {
        ChannelHandle {
            channel_id,
            frame_tx,
            command_tx,
            message_rx,
            state,
        }
    }

    pub async fn send(&self, message: Message) -> Result<(), MuxError> {
        if self.state == ChannelState::Closed {
            return Err(MuxError::ChannelAlreadyClosed(self.channel_id));
        }

        let frame = Frame {
            channel_id: self.channel_id,
            message,
        };

        self.frame_tx
            .send(frame)
            .await
            .map_err(|e| MuxError::MuxTaskTerminated(e.to_string()))
    }

    /// Receives a [`Message`] from the channel.
    /// Returns [`None`] if the channel is closed by peer or the [`Mux`] task has terminated.
    pub async fn recv(&mut self) -> Option<Message> {
        let message = match self.message_rx.recv().await {
            // Message received.
            Some(message) => message,

            // Mux task has terminated.
            None => {
                self.state = ChannelState::Closed;
                return None;
            }
        };

        Some(message)
    }

    pub async fn close(&mut self) -> Result<(), MuxError> {
        if self.state == ChannelState::Closed {
            return Ok(());
        }
        self.state = ChannelState::Closed;

        // Notify channel peer that the channel is closing.
        self.command_tx
            .send(Command::CloseChannel {
                channel_id: self.channel_id,
            })
            .await
            .map_err(|e| MuxError::MuxTaskTerminated(e.to_string()))?;

        // Delete channel from the mux's dispatch table.
        self.command_tx
            .send(Command::CloseChannel {
                channel_id: self.channel_id,
            })
            .await
            .map_err(|e| MuxError::MuxTaskTerminated(e.to_string()))?;

        Ok(())
    }
}

impl Drop for ChannelHandle {
    fn drop(&mut self) {
        if self.state == ChannelState::Closed {
            return;
        }
        self.state = ChannelState::Closed;

        // Delete channel from the mux's dispatch table.
        let _ = self.command_tx.try_send(Command::CloseChannel {
            channel_id: self.channel_id,
        });
    }
}

/// Provides an operation to handle a channel open request from the peer.
pub trait OpenChannelRequestHandler
where
    Self: Clone + Send + 'static,
{
    fn handle(
        &self,
        channel_id: ChannelId,
    ) -> Result<Box<dyn FnOnce(ChannelHandle) -> BoxFuture<'static, ()> + Send>, String>;
}

/// A [`OpenChannelRequestHandler`] that is constructed from a closure.
///
/// Example usage:
///
/// ```
/// let handler = FnOpenChannelRequestHandler::new(move |channel_id| {
///     if !allowed_channels.contains(&channel_id) {
///         return Err(format!("channel {} not permitted", channel_id));
///     }
///     let state = app_state.clone();
///     Ok(accept(move |handle| async move {
///         run_ssh_server(handle, state).await;
///     }))
/// });
/// ```
///
#[derive(Clone)]
pub struct FnOpenChannelRequestHandler<F> {
    f: F,
}

impl<F> FnOpenChannelRequestHandler<F>
where
    F: Fn(
            ChannelId,
        )
            -> Result<Box<dyn FnOnce(ChannelHandle) -> BoxFuture<'static, ()> + Send>, String>
        + Clone
        + Send
        + 'static,
{
    pub fn new(f: F) -> Self {
        Self { f }
    }
}

impl<F> OpenChannelRequestHandler for FnOpenChannelRequestHandler<F>
where
    F: Fn(
            ChannelId,
        )
            -> Result<Box<dyn FnOnce(ChannelHandle) -> BoxFuture<'static, ()> + Send>, String>
        + Clone
        + Send
        + 'static,
{
    fn handle(
        &self,
        channel_id: ChannelId,
    ) -> Result<Box<dyn FnOnce(ChannelHandle) -> BoxFuture<'static, ()> + Send>, String> {
        (self.f)(channel_id)
    }
}

/// Convenience function: converts an async fn(ChannelHandle) into the
/// boxed FnOnce that OpenChannelRequestHandler::handle must return.
///
/// Use this inside your FnOpenChannelRequestHandler closure to avoid writing
/// Box::new and Box::pin at every call site.
pub fn accept<F, Fut>(f: F) -> Box<dyn FnOnce(ChannelHandle) -> BoxFuture<'static, ()> + Send>
where
    F: FnOnce(ChannelHandle) -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    Box::new(move |handle| Box::pin(f(handle)))
}
