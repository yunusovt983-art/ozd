//! Disk-health FSM (#142, паттерн RustFS): Online → Suspect → Faulted →
//! Returning с ГИСТЕРЕЗИСОМ (N сбоев подряд / N успехов подряд) — одиночный
//! глюк не валит диск, возврат — через probe-подтверждения.

use ozd_domain::ShardStatus;

/// Наблюдение за один цикл монитора (из ZFS-health / disk-slow / IO-ошибок).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Observation {
    /// всё чисто (zpool ONLINE без ошибок)
    Healthy,
    /// деградация (ошибки растут / DEGRADED)
    Degraded,
    /// недоступен (FAULTED/UNAVAIL / команда не отвечает)
    Down,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FsmState {
    Online,
    Suspect,
    Faulted,
    /// вернулся после Faulted — probe-период
    Returning,
}

#[derive(Debug)]
pub struct HealthFsm {
    state: FsmState,
    fails: u32,
    oks: u32,
    /// сбоев подряд до эскалации (Online→Suspect по Degraded; Suspect→Faulted по Down)
    pub suspect_after: u32,
    /// успехов подряд до возврата в Online (из Suspect/Returning)
    pub recover_after: u32,
}

impl HealthFsm {
    pub fn new(suspect_after: u32, recover_after: u32) -> Self {
        Self {
            state: FsmState::Online,
            fails: 0,
            oks: 0,
            suspect_after: suspect_after.max(1),
            recover_after: recover_after.max(1),
        }
    }

    /// Подать наблюдение; вернуть текущий доменный статус.
    pub fn observe(&mut self, obs: Observation) -> ShardStatus {
        use FsmState::*;
        use Observation::*;
        self.state = match (self.state, obs) {
            (Online, Healthy) => {
                self.fails = 0;
                Online
            }
            (Online, Degraded) => {
                self.fails += 1;
                if self.fails >= self.suspect_after {
                    self.reset();
                    Suspect
                } else {
                    Online // гистерезис: одиночный глюк не меняет статус
                }
            }
            (Online, Down) => {
                // жёсткий отказ — сразу Suspect; счёт до Faulted — заново
                self.reset();
                Suspect
            }
            (Suspect, Healthy) => {
                self.fails = 0;
                self.oks += 1;
                if self.oks >= self.recover_after {
                    self.reset();
                    Online
                } else {
                    Suspect
                }
            }
            (Suspect, Degraded) => {
                self.oks = 0;
                Suspect
            }
            (Suspect, Down) => {
                self.oks = 0;
                self.fails += 1;
                if self.fails >= self.suspect_after {
                    self.reset();
                    Faulted
                } else {
                    Suspect
                }
            }
            (Faulted, Healthy) => {
                self.reset();
                self.oks = 1;
                Returning // probe: ещё не доверяем
            }
            (Faulted, _) => Faulted,
            (Returning, Healthy) => {
                self.oks += 1;
                if self.oks >= self.recover_after {
                    self.reset();
                    Online
                } else {
                    Returning
                }
            }
            (Returning, _) => {
                // рецидив на probe — назад в Faulted
                self.reset();
                Faulted
            }
        };
        self.status()
    }

    fn reset(&mut self) {
        self.fails = 0;
        self.oks = 0;
    }

    /// Маппинг в доменный статус: Returning размещение не получает
    /// предпочтения (= Suspect), Faulted исключён из HRW.
    pub fn status(&self) -> ShardStatus {
        match self.state {
            FsmState::Online => ShardStatus::Online,
            FsmState::Suspect | FsmState::Returning => ShardStatus::Suspect,
            FsmState::Faulted => ShardStatus::Faulted,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use Observation::*;

    #[test]
    fn single_glitch_does_not_demote() {
        let mut f = HealthFsm::new(3, 2);
        assert_eq!(f.observe(Degraded), ShardStatus::Online); // 1 глюк — стоим
        assert_eq!(f.observe(Healthy), ShardStatus::Online);  // сброс счётчика
        assert_eq!(f.observe(Degraded), ShardStatus::Online);
        assert_eq!(f.observe(Degraded), ShardStatus::Online);
        assert_eq!(f.observe(Degraded), ShardStatus::Suspect); // 3 подряд
    }

    #[test]
    fn escalates_to_faulted_and_probes_back() {
        let mut f = HealthFsm::new(2, 2);
        assert_eq!(f.observe(Down), ShardStatus::Suspect); // жёсткий → сразу Suspect
        assert_eq!(f.observe(Down), ShardStatus::Suspect); // fails=1 в Suspect
        assert_eq!(f.observe(Down), ShardStatus::Faulted); // fails=2 → Faulted
        // возврат: probe-период, не сразу Online
        assert_eq!(f.observe(Healthy), ShardStatus::Suspect); // Returning
        assert_eq!(f.observe(Healthy), ShardStatus::Online);  // recover_after=2
    }

    #[test]
    fn relapse_on_probe_goes_back_to_faulted() {
        let mut f = HealthFsm::new(1, 3);
        f.observe(Down);            // Suspect
        f.observe(Down);            // Faulted (suspect_after=1)
        f.observe(Healthy);         // Returning
        assert_eq!(f.observe(Down), ShardStatus::Faulted); // рецидив
        // и снова полный probe-цикл
        f.observe(Healthy);
        f.observe(Healthy);
        assert_eq!(f.observe(Healthy), ShardStatus::Online);
    }

    #[test]
    fn suspect_recovers_after_consecutive_healthy() {
        let mut f = HealthFsm::new(2, 3);
        f.observe(Degraded);
        f.observe(Degraded); // Suspect
        assert_eq!(f.status(), ShardStatus::Suspect);
        f.observe(Healthy);
        f.observe(Healthy);
        assert_eq!(f.observe(Degraded), ShardStatus::Suspect); // сброс oks
        f.observe(Healthy);
        f.observe(Healthy);
        assert_eq!(f.observe(Healthy), ShardStatus::Online);
    }
}
