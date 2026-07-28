mod wrpc;

pub use wrpc::{compute_covenant_id, NodeHandle};

use crate::model::*;
use crate::Result;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ChainWakeupKind {
    VirtualChainChanged,
    Disconnected,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ChainWakeup {
    pub kind: ChainWakeupKind,
    pub observed_at_ms: u64,
}

#[derive(Clone)]
pub struct ChainWakeupPublisher {
    sender: tokio::sync::watch::Sender<Option<ChainWakeup>>,
}

#[derive(Clone)]
pub struct ChainWakeups {
    receiver: tokio::sync::watch::Receiver<Option<ChainWakeup>>,
}

pub fn chain_wakeup_channel() -> (ChainWakeupPublisher, ChainWakeups) {
    let (sender, receiver) = tokio::sync::watch::channel(None);
    (ChainWakeupPublisher { sender }, ChainWakeups { receiver })
}

impl ChainWakeupPublisher {
    pub fn publish(&self, kind: ChainWakeupKind, observed_at_ms: u64) {
        self.sender.send_replace(Some(ChainWakeup {
            kind,
            observed_at_ms,
        }));
    }
}

impl ChainWakeups {
    pub async fn recv(&mut self) -> Option<ChainWakeup> {
        self.receiver.changed().await.ok()?;
        *self.receiver.borrow_and_update()
    }

    pub fn try_recv(&mut self) -> Option<ChainWakeup> {
        if !self.receiver.has_changed().unwrap_or(false) {
            return None;
        }
        *self.receiver.borrow_and_update()
    }

    pub fn has_pending(&self) -> bool {
        self.receiver.has_changed().unwrap_or(false)
    }
}

/// Read access to the chain, as the sync engine needs it. Implemented by the
/// live wRPC client and by in-memory fakes in tests.
pub trait ChainSource {
    fn dag_info(&self) -> impl std::future::Future<Output = Result<DagInfo>>;
    fn block_with_txs(&self, hash: BlockHash) -> impl std::future::Future<Output = Result<Block>>;
    fn virtual_chain_from(
        &self,
        cursor: BlockHash,
    ) -> impl std::future::Future<Output = Result<ChainStep>>;
    fn mempool_txs(&self) -> impl std::future::Future<Output = Result<Vec<Transaction>>>;
}

impl ChainSource for NodeHandle {
    async fn dag_info(&self) -> Result<DagInfo> {
        NodeHandle::dag_info(self).await
    }
    async fn block_with_txs(&self, hash: BlockHash) -> Result<Block> {
        NodeHandle::block_with_txs(self, hash).await
    }
    async fn virtual_chain_from(&self, cursor: BlockHash) -> Result<ChainStep> {
        NodeHandle::virtual_chain_from(self, cursor).await
    }
    async fn mempool_txs(&self) -> Result<Vec<Transaction>> {
        NodeHandle::mempool_txs(self).await
    }
}
