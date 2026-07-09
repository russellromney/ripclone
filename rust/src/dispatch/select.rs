//! Registry / factory: pick the active [`ComputeProvider`] from `RIPCLONE_DISPATCH`.

use super::ComputeProvider;
use super::exec::ExecProvider;
use super::fly::FlyProvider;
use super::http::HttpProvider;
use super::mock::MockProvider;
use anyhow::{Result, bail};
use std::sync::Arc;

/// Built-in dispatch backends selected by `RIPCLONE_DISPATCH`.
///
/// - **fly** — [`FlyProvider`] (start-stopped pooled machines). Launch target.
/// - **exec** — escape hatch: run a configured command with the env bag.
/// - **http** — escape hatch: POST [`super::WorkerSpec`] to a URL.
/// - **mock** — [`MockProvider`] (tests / local).
/// - **none** — dispatch disabled (default). Enqueue still works; no wake.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DispatchBackend {
    Fly,
    Exec,
    Http,
    Mock,
    None,
}

impl DispatchBackend {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Fly => "fly",
            Self::Exec => "exec",
            Self::Http => "http",
            Self::Mock => "mock",
            Self::None => "none",
        }
    }
}

/// Parse `RIPCLONE_DISPATCH` (or an override). Unknown values fail loudly.
pub fn parse_dispatch_backend(raw: Option<&str>) -> Result<DispatchBackend> {
    let v = raw.unwrap_or("none").trim().to_ascii_lowercase();
    match v.as_str() {
        "" | "none" => Ok(DispatchBackend::None),
        "fly" => Ok(DispatchBackend::Fly),
        "exec" => Ok(DispatchBackend::Exec),
        "http" => Ok(DispatchBackend::Http),
        "mock" => Ok(DispatchBackend::Mock),
        _ => bail!(
            "Unknown RIPCLONE_DISPATCH={}. Expected fly|exec|http|mock|none.",
            raw.unwrap_or("")
        ),
    }
}

/// Options for [`get_compute_provider`]. Tests inject providers / env override.
#[derive(Default)]
pub struct SelectProviderOptions {
    /// Override `RIPCLONE_DISPATCH` env lookup.
    pub dispatch: Option<String>,
    /// Injected mock (when backend is `mock`).
    pub mock: Option<Arc<MockProvider>>,
    /// Injected fly (when backend is `fly`).
    pub fly: Option<Arc<FlyProvider>>,
    /// Injected exec (when backend is `exec`).
    pub exec: Option<Arc<ExecProvider>>,
    /// Injected http (when backend is `http`).
    pub http: Option<Arc<HttpProvider>>,
}

/// Resolve the [`ComputeProvider`] from `RIPCLONE_DISPATCH`.
///
/// Returns `None` when dispatch is off (`none` / unset) — callers treat that as
/// "enqueue only". Outside this function, nothing knows the platform.
pub fn get_compute_provider(
    opts: SelectProviderOptions,
) -> Result<Option<Arc<dyn ComputeProvider>>> {
    let raw = opts
        .dispatch
        .clone()
        .or_else(|| std::env::var("RIPCLONE_DISPATCH").ok());
    let backend = parse_dispatch_backend(raw.as_deref())?;

    match backend {
        DispatchBackend::None => Ok(None),
        DispatchBackend::Mock => {
            let p: Arc<dyn ComputeProvider> = opts
                .mock
                .map(|m| m as Arc<dyn ComputeProvider>)
                .unwrap_or_else(|| Arc::new(MockProvider::new()));
            Ok(Some(p))
        }
        DispatchBackend::Fly => {
            let p: Arc<dyn ComputeProvider> = match opts.fly {
                Some(f) => f,
                None => Arc::new(FlyProvider::from_env()?),
            };
            Ok(Some(p))
        }
        DispatchBackend::Exec => {
            let p: Arc<dyn ComputeProvider> = match opts.exec {
                Some(e) => e,
                None => Arc::new(ExecProvider::from_env()?),
            };
            Ok(Some(p))
        }
        DispatchBackend::Http => {
            let p: Arc<dyn ComputeProvider> = match opts.http {
                Some(h) => h,
                None => Arc::new(HttpProvider::from_env()?),
            };
            Ok(Some(p))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dispatch::WorkerSpec;
    use std::collections::BTreeMap;
    use std::sync::{Mutex, OnceLock};

    /// Serialize env-mutating tests: concurrent suite members share process env.
    fn env_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    #[test]
    fn parse_defaults_to_none() {
        assert_eq!(parse_dispatch_backend(None).unwrap(), DispatchBackend::None);
        assert_eq!(
            parse_dispatch_backend(Some("")).unwrap(),
            DispatchBackend::None
        );
        assert_eq!(
            parse_dispatch_backend(Some("NONE")).unwrap(),
            DispatchBackend::None
        );
    }

    #[test]
    fn parse_known_backends() {
        for (raw, want) in [
            ("fly", DispatchBackend::Fly),
            ("exec", DispatchBackend::Exec),
            ("http", DispatchBackend::Http),
            ("mock", DispatchBackend::Mock),
        ] {
            assert_eq!(parse_dispatch_backend(Some(raw)).unwrap(), want);
        }
    }

    #[test]
    fn parse_rejects_unknown_loudly() {
        let err = parse_dispatch_backend(Some("modal")).unwrap_err();
        assert!(
            err.to_string().contains("Unknown RIPCLONE_DISPATCH"),
            "got: {err}"
        );
    }

    #[test]
    fn selects_none_when_unset() {
        let _g = env_lock().lock().unwrap();
        // SAFETY: serialized by env_lock; we restore after.
        let saved = std::env::var("RIPCLONE_DISPATCH").ok();
        unsafe { std::env::remove_var("RIPCLONE_DISPATCH") };
        let p = get_compute_provider(SelectProviderOptions::default()).unwrap();
        assert!(p.is_none());
        match saved {
            Some(v) => unsafe { std::env::set_var("RIPCLONE_DISPATCH", v) },
            None => unsafe { std::env::remove_var("RIPCLONE_DISPATCH") },
        }
    }

    #[tokio::test]
    async fn selects_mock_by_config() {
        let mock = Arc::new(MockProvider::new());
        let p = get_compute_provider(SelectProviderOptions {
            dispatch: Some("mock".into()),
            mock: Some(mock.clone()),
            ..Default::default()
        })
        .unwrap()
        .expect("provider");
        assert_eq!(p.name(), "mock");
        p.ensure_worker(&WorkerSpec::new("small", BTreeMap::new()))
            .await
            .unwrap();
        assert_eq!(mock.calls().len(), 1);
        assert_eq!(mock.calls()[0].size_class, "small");
    }

    #[test]
    fn selects_fly_injection() {
        use crate::dispatch::fly::{FlyMachinesClient, FlyProviderConfig};
        use async_trait::async_trait;

        struct EmptyClient;
        #[async_trait]
        impl FlyMachinesClient for EmptyClient {
            async fn list_machines(&self, _: &str) -> Result<Vec<crate::dispatch::FlyMachine>> {
                Ok(vec![])
            }
            async fn start_machine(&self, _: &str, _: &str) -> Result<()> {
                Ok(())
            }
        }

        let fly = Arc::new(FlyProvider::new(FlyProviderConfig {
            app: "workers".into(),
            token: "t".into(),
            api_base: None,
            client: Some(Arc::new(EmptyClient)),
            size_class_metadata_key: None,
            process_group: None,
        }));
        let p = get_compute_provider(SelectProviderOptions {
            dispatch: Some("fly".into()),
            fly: Some(fly),
            ..Default::default()
        })
        .unwrap()
        .expect("provider");
        assert_eq!(p.name(), "fly");
    }

    #[test]
    fn selects_exec_and_http_by_name() {
        use crate::dispatch::exec::ExecProviderConfig;
        use crate::dispatch::http::HttpProviderConfig;
        use std::path::PathBuf;

        let exec = Arc::new(ExecProvider::new(ExecProviderConfig {
            program: PathBuf::from("/bin/true"),
            fixed_args: vec![],
        }));
        let p = get_compute_provider(SelectProviderOptions {
            dispatch: Some("exec".into()),
            exec: Some(exec),
            ..Default::default()
        })
        .unwrap()
        .expect("provider");
        assert_eq!(p.name(), "exec");

        let http = Arc::new(HttpProvider::new(HttpProviderConfig {
            url: "http://127.0.0.1:9/wake".into(),
            token: None,
            client: None,
        }));
        let p = get_compute_provider(SelectProviderOptions {
            dispatch: Some("http".into()),
            http: Some(http),
            ..Default::default()
        })
        .unwrap()
        .expect("provider");
        assert_eq!(p.name(), "http");
    }

    #[test]
    fn env_var_selects_mock() {
        let _g = env_lock().lock().unwrap();
        let saved = std::env::var("RIPCLONE_DISPATCH").ok();
        unsafe { std::env::set_var("RIPCLONE_DISPATCH", "mock") };
        let p = get_compute_provider(SelectProviderOptions::default())
            .unwrap()
            .expect("provider");
        assert_eq!(p.name(), "mock");
        match saved {
            Some(v) => unsafe { std::env::set_var("RIPCLONE_DISPATCH", v) },
            None => unsafe { std::env::remove_var("RIPCLONE_DISPATCH") },
        }
    }

    #[test]
    fn fly_from_env_fails_loudly_without_creds() {
        let _g = env_lock().lock().unwrap();
        let saved_dispatch = std::env::var("RIPCLONE_DISPATCH").ok();
        let saved_app = std::env::var("FLY_WORKER_APP").ok();
        let saved_tok = std::env::var("FLY_API_TOKEN").ok();
        unsafe {
            std::env::set_var("RIPCLONE_DISPATCH", "fly");
            std::env::remove_var("FLY_WORKER_APP");
            std::env::remove_var("FLY_API_TOKEN");
        }
        // Avoid unwrap_err: Ok type is Option<Arc<dyn ComputeProvider>> (not Debug).
        let err = match get_compute_provider(SelectProviderOptions::default()) {
            Err(e) => e,
            Ok(_) => panic!("expected missing FLY_WORKER_APP to fail"),
        };
        assert!(err.to_string().contains("FLY_WORKER_APP"), "got: {err}");
        match saved_dispatch {
            Some(v) => unsafe { std::env::set_var("RIPCLONE_DISPATCH", v) },
            None => unsafe { std::env::remove_var("RIPCLONE_DISPATCH") },
        }
        match saved_app {
            Some(v) => unsafe { std::env::set_var("FLY_WORKER_APP", v) },
            None => unsafe { std::env::remove_var("FLY_WORKER_APP") },
        }
        match saved_tok {
            Some(v) => unsafe { std::env::set_var("FLY_API_TOKEN", v) },
            None => unsafe { std::env::remove_var("FLY_API_TOKEN") },
        }
    }

    #[test]
    fn exec_and_http_from_env_fail_loudly_without_config() {
        let _g = env_lock().lock().unwrap();
        let saved_dispatch = std::env::var("RIPCLONE_DISPATCH").ok();
        let saved_cmd = std::env::var("RIPCLONE_DISPATCH_CMD").ok();
        let saved_url = std::env::var("RIPCLONE_DISPATCH_URL").ok();

        unsafe {
            std::env::set_var("RIPCLONE_DISPATCH", "exec");
            std::env::remove_var("RIPCLONE_DISPATCH_CMD");
        }
        let err = match get_compute_provider(SelectProviderOptions::default()) {
            Err(e) => e,
            Ok(_) => panic!("expected missing RIPCLONE_DISPATCH_CMD to fail"),
        };
        assert!(
            err.to_string().contains("RIPCLONE_DISPATCH_CMD"),
            "got: {err}"
        );

        unsafe {
            std::env::set_var("RIPCLONE_DISPATCH", "http");
            std::env::remove_var("RIPCLONE_DISPATCH_URL");
        }
        let err = match get_compute_provider(SelectProviderOptions::default()) {
            Err(e) => e,
            Ok(_) => panic!("expected missing RIPCLONE_DISPATCH_URL to fail"),
        };
        assert!(
            err.to_string().contains("RIPCLONE_DISPATCH_URL"),
            "got: {err}"
        );

        match saved_dispatch {
            Some(v) => unsafe { std::env::set_var("RIPCLONE_DISPATCH", v) },
            None => unsafe { std::env::remove_var("RIPCLONE_DISPATCH") },
        }
        match saved_cmd {
            Some(v) => unsafe { std::env::set_var("RIPCLONE_DISPATCH_CMD", v) },
            None => unsafe { std::env::remove_var("RIPCLONE_DISPATCH_CMD") },
        }
        match saved_url {
            Some(v) => unsafe { std::env::set_var("RIPCLONE_DISPATCH_URL", v) },
            None => unsafe { std::env::remove_var("RIPCLONE_DISPATCH_URL") },
        }
    }
}
