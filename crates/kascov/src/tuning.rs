pub const PROFILE_VERSION: u32 = 0;
pub const PROFILE_STATUS: &str = "initial";

pub const FETCH_AHEAD_CANDIDATES: [usize; 4] = [8, 16, 32, 64];
pub const WAL_AUTOCHECKPOINT_CANDIDATES: [u32; 3] = [1_000, 4_000, 16_000];
pub const READ_POOL_CANDIDATES: [u32; 3] = [4, 8, 16];
pub const REPLAY_PAGE_CANDIDATES: [u64; 3] = [256, 512, 1_024];

pub const DEFAULT_FETCH_AHEAD: usize = 16;
pub const DEFAULT_WAL_AUTOCHECKPOINT: u32 = 1_000;
pub const DEFAULT_READ_POOL: u32 = 8;
pub const DEFAULT_REPLAY_PAGE: u64 = 512;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TuningProfile {
    pub fetch_ahead: usize,
    pub wal_autocheckpoint: u32,
    pub read_pool: u32,
    pub replay_page: u64,
}

impl Default for TuningProfile {
    fn default() -> Self {
        Self {
            fetch_ahead: DEFAULT_FETCH_AHEAD,
            wal_autocheckpoint: DEFAULT_WAL_AUTOCHECKPOINT,
            read_pool: DEFAULT_READ_POOL,
            replay_page: DEFAULT_REPLAY_PAGE,
        }
    }
}

impl TuningProfile {
    pub fn validate(self) -> anyhow::Result<Self> {
        validate_candidate("fetch-ahead", self.fetch_ahead, &FETCH_AHEAD_CANDIDATES)?;
        validate_candidate(
            "wal-autocheckpoint",
            self.wal_autocheckpoint,
            &WAL_AUTOCHECKPOINT_CANDIDATES,
        )?;
        validate_candidate("read-pool", self.read_pool, &READ_POOL_CANDIDATES)?;
        validate_candidate(
            "replay-page-size",
            self.replay_page,
            &REPLAY_PAGE_CANDIDATES,
        )?;
        Ok(self)
    }

    pub fn configure_core(self) -> anyhow::Result<()> {
        kascov_core::sync::configure_fetch_ahead(self.fetch_ahead)?;
        kascov_core::store::configure_wal_autocheckpoint(self.wal_autocheckpoint)?;
        Ok(())
    }

    pub fn health_json(self) -> serde_json::Value {
        serde_json::json!({
            "profile_version": PROFILE_VERSION,
            "profile_status": PROFILE_STATUS,
            "fetch_ahead": self.fetch_ahead,
            "wal_autocheckpoint_pages": self.wal_autocheckpoint,
            "read_pool_connections": self.read_pool,
            "replay_page_records": self.replay_page,
        })
    }
}

fn validate_candidate<T>(name: &str, value: T, candidates: &[T]) -> anyhow::Result<()>
where
    T: Copy + PartialEq + std::fmt::Display,
{
    if !candidates.contains(&value) {
        anyhow::bail!("{name} value {value} is outside the fixed Stage 2 candidate set");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn initial_profile_is_valid_and_visible() {
        let profile = TuningProfile::default().validate().unwrap();
        assert_eq!(16, profile.fetch_ahead);
        assert_eq!(1_000, profile.wal_autocheckpoint);
        assert_eq!(8, profile.read_pool);
        assert_eq!(512, profile.replay_page);
        assert_eq!(0, profile.health_json()["profile_version"]);
        assert_eq!(DEFAULT_FETCH_AHEAD, kascov_core::sync::DEFAULT_FETCH_AHEAD);
        assert_eq!(
            DEFAULT_WAL_AUTOCHECKPOINT,
            kascov_core::store::DEFAULT_WAL_AUTOCHECKPOINT
        );
    }

    #[test]
    fn each_value_is_limited_to_its_fixed_candidate_set() {
        for profile in [
            TuningProfile {
                fetch_ahead: 7,
                ..Default::default()
            },
            TuningProfile {
                wal_autocheckpoint: 999,
                ..Default::default()
            },
            TuningProfile {
                read_pool: 3,
                ..Default::default()
            },
            TuningProfile {
                replay_page: 255,
                ..Default::default()
            },
        ] {
            assert!(profile.validate().is_err(), "accepted {profile:?}");
        }
    }

    #[test]
    fn every_declared_candidate_parses() {
        for fetch_ahead in FETCH_AHEAD_CANDIDATES {
            assert!(TuningProfile {
                fetch_ahead,
                ..Default::default()
            }
            .validate()
            .is_ok());
        }
        for wal_autocheckpoint in WAL_AUTOCHECKPOINT_CANDIDATES {
            assert!(TuningProfile {
                wal_autocheckpoint,
                ..Default::default()
            }
            .validate()
            .is_ok());
        }
        for read_pool in READ_POOL_CANDIDATES {
            assert!(TuningProfile {
                read_pool,
                ..Default::default()
            }
            .validate()
            .is_ok());
        }
        for replay_page in REPLAY_PAGE_CANDIDATES {
            assert!(TuningProfile {
                replay_page,
                ..Default::default()
            }
            .validate()
            .is_ok());
        }
    }
}
