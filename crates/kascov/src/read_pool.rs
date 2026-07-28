use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use kascov_core::model::Network;
use kascov_core::store::Store;

use crate::performance::ReadPoolMetrics;

pub const DEFAULT_MAX_READERS: u32 = 8;
pub const DEFAULT_CHECKOUT_TIMEOUT: Duration = Duration::from_millis(100);

#[derive(Debug)]
struct StoreManager {
    path: PathBuf,
    network: Network,
}

impl r2d2::ManageConnection for StoreManager {
    type Connection = Store;
    type Error = kascov_core::Error;

    fn connect(&self) -> Result<Self::Connection, Self::Error> {
        Store::open_reader(&self.path, self.network)
    }

    fn is_valid(&self, store: &mut Self::Connection) -> Result<(), Self::Error> {
        store.reader_is_healthy()
    }

    fn has_broken(&self, store: &mut Self::Connection) -> bool {
        store.reader_is_healthy().is_err()
    }
}

#[derive(Clone)]
pub struct ReadPool {
    inner: r2d2::Pool<StoreManager>,
    timeout: Duration,
    closed: Arc<AtomicBool>,
    metrics: Arc<ReadPoolMetrics>,
}

#[derive(Debug)]
pub enum ReadError {
    Closed,
    Unavailable(r2d2::Error),
    Query(anyhow::Error),
}

#[derive(Debug)]
pub enum ReadQueryError<E> {
    Closed,
    Unavailable(r2d2::Error),
    Query(E),
}

impl<E: std::fmt::Display> std::fmt::Display for ReadQueryError<E> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Closed => formatter.write_str("read pool is closed"),
            Self::Unavailable(error) => write!(formatter, "read pool unavailable: {error}"),
            Self::Query(error) => write!(formatter, "read query failed: {error}"),
        }
    }
}

impl std::fmt::Display for ReadError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Closed => formatter.write_str("read pool is closed"),
            Self::Unavailable(error) => write!(formatter, "read pool unavailable: {error}"),
            Self::Query(error) => write!(formatter, "read query failed: {error}"),
        }
    }
}

impl std::error::Error for ReadError {}

impl ReadPool {
    pub fn new(path: &Path, network: Network) -> Self {
        Self::with_limits(path, network, DEFAULT_MAX_READERS, DEFAULT_CHECKOUT_TIMEOUT)
    }

    fn with_limits(path: &Path, network: Network, max_size: u32, timeout: Duration) -> Self {
        let manager = StoreManager {
            path: path.to_owned(),
            network,
        };
        let inner = r2d2::Pool::builder()
            .max_size(max_size)
            .min_idle(Some(0))
            .connection_timeout(timeout)
            .test_on_check_out(true)
            .build_unchecked(manager);
        Self {
            inner,
            timeout,
            closed: Arc::new(AtomicBool::new(false)),
            metrics: Arc::new(ReadPoolMetrics::default()),
        }
    }

    pub fn query<T>(
        &self,
        operation: impl FnOnce(&Store) -> anyhow::Result<T>,
    ) -> Result<T, ReadError> {
        self.query_with(operation).map_err(|error| match error {
            ReadQueryError::Closed => ReadError::Closed,
            ReadQueryError::Unavailable(error) => ReadError::Unavailable(error),
            ReadQueryError::Query(error) => ReadError::Query(error),
        })
    }

    pub fn query_with<T, E>(
        &self,
        operation: impl FnOnce(&Store) -> Result<T, E>,
    ) -> Result<T, ReadQueryError<E>> {
        if self.closed.load(Ordering::Acquire) {
            return Err(ReadQueryError::Closed);
        }
        let checkout_started = Instant::now();
        let store = self.inner.get_timeout(self.timeout).map_err(|error| {
            self.metrics.record_checkout(checkout_started.elapsed());
            ReadQueryError::Unavailable(error)
        })?;
        self.metrics.record_checkout(checkout_started.elapsed());
        if self.closed.load(Ordering::Acquire) {
            return Err(ReadQueryError::Closed);
        }
        let query_started = Instant::now();
        let result = operation(&store).map_err(ReadQueryError::Query);
        self.metrics.record_query(query_started.elapsed());
        result
    }

    pub fn metrics(&self) -> &ReadPoolMetrics {
        &self.metrics
    }

    #[cfg(test)]
    fn shutdown(&self) {
        self.closed.store(true, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn initialized_store() -> (tempfile::TempDir, PathBuf, Network) {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("pool.db");
        let network = Network::Testnet(10);
        Store::open(&path, network).unwrap();
        (directory, path, network)
    }

    #[test]
    fn pool_is_bounded_and_times_out() {
        let (_directory, path, network) = initialized_store();
        let pool = ReadPool::with_limits(&path, network, 1, Duration::from_millis(20));
        let held = pool.inner.get().unwrap();
        let started = Instant::now();
        let error = pool.query(|_| Ok(())).unwrap_err();
        assert!(matches!(error, ReadError::Unavailable(_)));
        assert!(started.elapsed() >= Duration::from_millis(20));
        assert_eq!(1, pool.inner.state().connections);
        drop(held);
    }

    #[test]
    fn unhealthy_connection_is_replaced() {
        let (_directory, path, network) = initialized_store();
        let pool = ReadPool::with_limits(&path, network, 1, Duration::from_millis(100));
        pool.query(|_| Ok(())).unwrap();

        let writer = rusqlite::Connection::open(&path).unwrap();
        writer.execute("DELETE FROM meta WHERE key = 'network'", []).unwrap();
        assert!(matches!(pool.query(|_| Ok(())), Err(ReadError::Unavailable(_))));
        writer
            .execute(
                "INSERT INTO meta(key, value) VALUES ('network', ?1)",
                [network.to_string()],
            )
            .unwrap();
        pool.query(|store| {
            store.reader_is_healthy()?;
            Ok(())
        })
        .unwrap();
        assert_eq!(1, pool.inner.state().connections);
    }

    #[test]
    fn shutdown_refuses_new_queries() {
        let (_directory, path, network) = initialized_store();
        let pool = ReadPool::with_limits(&path, network, 1, Duration::from_millis(20));
        pool.query(|_| Ok(())).unwrap();
        pool.shutdown();
        assert!(matches!(pool.query(|_| Ok(())), Err(ReadError::Closed)));
    }

    #[test]
    fn records_checkout_and_query_separately() {
        let (_directory, path, network) = initialized_store();
        let pool = ReadPool::with_limits(&path, network, 1, Duration::from_millis(20));
        pool.query(|_| Ok(())).unwrap();
        let snapshot = pool.metrics().snapshot_json();
        assert_eq!(1, snapshot["checkout"]["count"]);
        assert_eq!(1, snapshot["query"]["count"]);
    }
}
