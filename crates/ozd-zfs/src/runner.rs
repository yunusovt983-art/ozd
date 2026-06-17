// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (C) 2025 OpenZFS Daemon contributors

//! Runner-порт (#146, паттерн go-zfs Manager.Runner): исполнение zfs/zpool
//! через интерфейс → юнит-тесты адаптера БЕЗ zfs-бинаря (FakeRunner),
//! Sudo-вариант для непривилегированного демона.

use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

/// W1.2: таймаут на ZFS-команды — зависший zpool на умирающем диске
/// не блокирует мониторный цикл бесконечно.
const CMD_TIMEOUT: Duration = Duration::from_secs(30);

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
        use std::io::Read;
        let mut child = Command::new(program)
            .args(args)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()?;
        // Забираем stdout/stderr в потоки-читатели, чтобы не дедлочить на pipe
        let mut stdout_handle = child.stdout.take().unwrap();
        let mut stderr_handle = child.stderr.take().unwrap();
        let stdout_thread = std::thread::spawn(move || {
            let mut s = String::new();
            let _ = stdout_handle.read_to_string(&mut s);
            s
        });
        let stderr_thread = std::thread::spawn(move || {
            let mut s = String::new();
            let _ = stderr_handle.read_to_string(&mut s);
            s
        });
        // W1.2: таймаут — зависший процесс убиваем через CMD_TIMEOUT
        let deadline = std::time::Instant::now() + CMD_TIMEOUT;
        let status = loop {
            match child.try_wait()? {
                Some(st) => break st,
                None => {
                    if std::time::Instant::now() >= deadline {
                        let _ = child.kill();
                        let _ = child.wait();
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::TimedOut,
                            format!("{program}: timed out after {}s", CMD_TIMEOUT.as_secs()),
                        ));
                    }
                    std::thread::sleep(Duration::from_millis(50));
                }
            }
        };
        let stdout = stdout_thread.join().unwrap_or_default();
        let stderr = stderr_thread.join().unwrap_or_default();
        Ok(CmdOutput { stdout, stderr, success: status.success() })
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
