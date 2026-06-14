//! Runner-порт (#146, паттерн go-zfs Manager.Runner): исполнение zfs/zpool
//! через интерфейс → юнит-тесты адаптера БЕЗ zfs-бинаря (FakeRunner),
//! Sudo-вариант для непривилегированного демона.

use std::process::Command;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct CmdOutput {
    pub stdout: String,
    pub stderr: String,
    pub success: bool,
}

pub trait Runner: Send + Sync {
    fn run(&self, program: &str, args: &[&str]) -> std::io::Result<CmdOutput>;
}

/// Локальное исполнение (дефолт).
pub struct LocalRunner;

impl Runner for LocalRunner {
    fn run(&self, program: &str, args: &[&str]) -> std::io::Result<CmdOutput> {
        let out = Command::new(program).args(args).output()?;
        Ok(CmdOutput {
            stdout: String::from_utf8_lossy(&out.stdout).into_owned(),
            stderr: String::from_utf8_lossy(&out.stderr).into_owned(),
            success: out.status.success(),
        })
    }
}

/// Исполнение через sudo (демон не под root).
pub struct SudoRunner;

impl Runner for SudoRunner {
    fn run(&self, program: &str, args: &[&str]) -> std::io::Result<CmdOutput> {
        let mut all = Vec::with_capacity(args.len() + 2);
        all.push("-n"); // не спрашивать пароль (NOPASSWD-правило)
        all.push(program);
        all.extend_from_slice(args);
        LocalRunner.run("sudo", &all)
    }
}

pub fn default_runner() -> Arc<dyn Runner> {
    Arc::new(LocalRunner)
}

/// Фейк для тестов (#146): очередь заготовленных ответов + журнал вызовов.
pub struct FakeRunner {
    pub responses: parking_lot_lite::Mutex<std::collections::VecDeque<CmdOutput>>,
    pub calls: parking_lot_lite::Mutex<Vec<String>>,
}

impl FakeRunner {
    pub fn new(responses: Vec<CmdOutput>) -> Self {
        Self {
            responses: parking_lot_lite::Mutex::new(responses.into()),
            calls: parking_lot_lite::Mutex::new(Vec::new()),
        }
    }
    pub fn ok(stdout: &str) -> CmdOutput {
        CmdOutput { stdout: stdout.to_string(), stderr: String::new(), success: true }
    }
    pub fn fail(stderr: &str) -> CmdOutput {
        CmdOutput { stdout: String::new(), stderr: stderr.to_string(), success: false }
    }
}

impl Runner for FakeRunner {
    fn run(&self, program: &str, args: &[&str]) -> std::io::Result<CmdOutput> {
        self.calls.lock().push(format!("{program} {}", args.join(" ")));
        self.responses.lock().pop_front().ok_or_else(|| {
            std::io::Error::other("FakeRunner: no more queued responses")
        })
    }
}

/// std-Mutex обёртка, чтобы не тянуть parking_lot в этот крейт.
mod parking_lot_lite {
    pub struct Mutex<T>(std::sync::Mutex<T>);
    impl<T> Mutex<T> {
        pub fn new(v: T) -> Self {
            Self(std::sync::Mutex::new(v))
        }
        pub fn lock(&self) -> std::sync::MutexGuard<'_, T> {
            self.0.lock().unwrap_or_else(|e| e.into_inner())
        }
    }
}
