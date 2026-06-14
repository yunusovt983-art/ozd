//! ozd-app — use-cases поверх пула шардов: Pool (aggregate root) +
//! RendezvousHrw placement. Часть 1 «sharding» из постановки:
//! key → hash(key) → top-R дисков → записать блок.

pub mod cache;
pub mod car;
pub mod diskslow;
pub mod erasure;
pub mod health;
pub mod latency;
pub mod metrics;
pub mod placement;
pub mod pool;
pub mod throttle;
pub mod verified;

pub use health::{HealthFsm, Observation};
pub use metrics::OpsMetrics;
pub use placement::RendezvousHrw;
pub use pool::{HealPriority, Pool, PoolConfig, ResilverReport, ScrubReport};
