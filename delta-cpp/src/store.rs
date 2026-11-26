//! Object stores, and the engines built on them.
//!
//! The kernel's engine reads through one injected `ObjectStore` and looks files up by the path of
//! the URL alone: scheme, bucket and account are ignored. So there is one store, and one engine,
//! per bucket (or container), and a split is read with the engine for its own URL.
//!
//! Credentials: stores are built with each builder's `from_env()`, so the node's own identity
//! works with no options (EC2 instance profile, EKS web identity / IRSA and ECS task roles for S3;
//! application-default credentials or the metadata server for GCS; managed identity or workload
//! identity for Azure). Explicit options, in `object_store`'s config-key syntax (`aws_region`,
//! `aws_endpoint`, ...), are applied on top and take precedence.
//!
//! Two things are NOT auto-detected and usually need setting: the S3 region (`AWS_REGION` in the
//! environment, or `aws_region` as an option; it defaults to `us-east-1`) and, for a custom
//! endpoint, `aws_endpoint` (plus `aws_allow_http` for plain http).
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex, OnceLock};

use delta_kernel_default_engine::executor::tokio::TokioMultiThreadExecutor;
use delta_kernel_default_engine::{DefaultEngine, DefaultEngineBuilder};
use object_store::aws::{AmazonS3Builder, AmazonS3ConfigKey};
use object_store::azure::{AzureConfigKey, MicrosoftAzureBuilder};
use object_store::gcp::{GoogleCloudStorageBuilder, GoogleConfigKey};
use object_store::local::LocalFileSystem;
use object_store::memory::InMemory;
use object_store::{ObjectStore, ObjectStoreScheme};
use url::Url;

pub type SharedEngine = DefaultEngine<TokioMultiThreadExecutor>;

/// Environment variable that sets the number of worker threads shared by every engine.
pub const WORKER_THREADS_ENV: &str = "DELTA_CPP_WORKER_THREADS";

/// One executor for every engine. Its worker threads do the I/O and the Parquet decode, so the
/// calling (Velox) threads only wait for results. Defaults to 4 workers or the number of cores,
/// whichever is smaller.
fn executor() -> Result<Arc<TokioMultiThreadExecutor>, String> {
    static EXECUTOR: OnceLock<Result<Arc<TokioMultiThreadExecutor>, String>> = OnceLock::new();
    EXECUTOR
        .get_or_init(|| {
            let workers = std::env::var(WORKER_THREADS_ENV)
                .ok()
                .and_then(|v| v.parse::<usize>().ok())
                .filter(|n| *n > 0)
                .unwrap_or_else(|| {
                    std::thread::available_parallelism()
                        .map(|n| n.get())
                        .unwrap_or(4)
                        .min(4)
                });
            TokioMultiThreadExecutor::new_owned_runtime(Some(workers), None)
                .map(Arc::new)
                .map_err(|e| format!("cannot start the I/O runtime: {e}"))
        })
        .clone()
}

/// Identifies one store: its kind, its bucket / container / account, and the options it was built
/// with. Deliberately not `Debug`: the options may hold credentials.
#[derive(Clone, PartialEq, Eq, Hash)]
struct EngineKey {
    kind: &'static str,
    authority: String,
    options: BTreeMap<String, String>,
}

/// Where a URL lives, as far as picking a store goes.
fn classify(url: &Url) -> Result<(ObjectStoreScheme, &'static str, String), String> {
    // Only the schemes Spark hands us. `https://` forms are rejected on purpose: for those the
    // bucket is the first path segment, which the engine's path-only addressing cannot tell apart.
    let kind = match url.scheme() {
        "file" => "file",
        "memory" => "memory",
        "s3" | "s3a" => "s3",
        "gs" => "gcs",
        "abfs" | "abfss" | "az" | "adl" | "azure" => "azure",
        other => {
            return Err(format!(
                "unsupported storage scheme '{other}' in {}: expected file, s3, s3a, gs, abfs, \
                 abfss, az, adl or azure",
                redact(url)
            ))
        }
    };
    let (scheme, _) = ObjectStoreScheme::parse(url)
        .map_err(|e| format!("cannot use {} as a storage location: {e}", redact(url)))?;
    let authority = match kind {
        "file" | "memory" => String::new(),
        _ => {
            let host = url
                .host_str()
                .filter(|h| !h.is_empty())
                .ok_or_else(|| format!("{} has no bucket or account", redact(url)))?;
            // Azure: `abfss://container@account.dfs.core.windows.net/...`
            format!("{}@{host}", url.username())
        }
    };
    Ok((scheme, kind, authority))
}

/// A URL without credentials, for error messages.
fn redact(url: &Url) -> String {
    let mut u = url.clone();
    let _ = u.set_password(None);
    u.to_string()
}

fn build_store(
    scheme: &ObjectStoreScheme,
    url: &Url,
    options: &BTreeMap<String, String>,
) -> Result<Arc<dyn ObjectStore>, String> {
    let err = |e: object_store::Error| format!("cannot open {}: {e}", redact(url));
    // Options a store does not recognise are ignored, so one options map can serve every store.
    Ok(match scheme {
        ObjectStoreScheme::Local => Arc::new(LocalFileSystem::new()),
        ObjectStoreScheme::Memory => Arc::new(InMemory::new()),
        ObjectStoreScheme::AmazonS3 => {
            let mut b = AmazonS3Builder::from_env().with_url(url.as_str());
            for (k, v) in options {
                if let Ok(key) = k.parse::<AmazonS3ConfigKey>() {
                    b = b.with_config(key, v);
                }
            }
            Arc::new(b.build().map_err(err)?)
        }
        ObjectStoreScheme::GoogleCloudStorage => {
            let mut b = GoogleCloudStorageBuilder::from_env().with_url(url.as_str());
            for (k, v) in options {
                if let Ok(key) = k.parse::<GoogleConfigKey>() {
                    b = b.with_config(key, v);
                }
            }
            Arc::new(b.build().map_err(err)?)
        }
        ObjectStoreScheme::MicrosoftAzure => {
            let mut b = MicrosoftAzureBuilder::from_env().with_url(url.as_str());
            for (k, v) in options {
                if let Ok(key) = k.parse::<AzureConfigKey>() {
                    b = b.with_config(key, v);
                }
            }
            Arc::new(b.build().map_err(err)?)
        }
        other => return Err(format!("unsupported store {other:?} for {}", redact(url))),
    })
}

type Cache = Mutex<HashMap<EngineKey, Arc<SharedEngine>>>;

fn cache() -> &'static Cache {
    static CACHE: OnceLock<Cache> = OnceLock::new();
    CACHE.get_or_init(Default::default)
}

/// The engine for the store that holds `url`, built on first use and then shared. Splits of the
/// same bucket with the same options share one engine, and so one connection pool.
///
/// Engines are never evicted: there is one per distinct (bucket, options) a process reads from,
/// which is small, but credentials that rotate by changing the options would grow the cache. That
/// is for the vended-credentials work, which needs a refreshable credential provider instead.
pub fn engine_for(
    url: &Url,
    options: &BTreeMap<String, String>,
) -> Result<Arc<SharedEngine>, String> {
    let (scheme, kind, authority) = classify(url)?;
    let key = EngineKey {
        kind,
        authority,
        options: options.clone(),
    };
    // Building a store does no I/O, so holding the lock across it is fine.
    let mut engines = cache().lock().unwrap_or_else(|p| p.into_inner());
    if let Some(engine) = engines.get(&key) {
        return Ok(engine.clone());
    }
    let store = build_store(&scheme, url, options)?;
    let engine = Arc::new(
        DefaultEngineBuilder::new(store)
            .with_task_executor(executor()?)
            .build(),
    );
    engines.insert(key, engine.clone());
    Ok(engine)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn opts(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    fn engine(url: &str, o: &BTreeMap<String, String>) -> Result<Arc<SharedEngine>, String> {
        engine_for(&Url::parse(url).unwrap(), o)
    }

    #[test]
    fn same_bucket_and_options_share_one_engine() {
        let o = opts(&[("aws_region", "us-west-2")]);
        let a = engine("s3://shared-a/x/y", &o).unwrap();
        // s3 and s3a are the same store; the path does not matter.
        let b = engine("s3a://shared-a/other/path", &o).unwrap();
        assert!(Arc::ptr_eq(&a, &b));
    }

    #[test]
    fn different_bucket_or_options_get_different_engines() {
        let o = opts(&[]);
        let a = engine("s3://bucket-one/x", &o).unwrap();
        let b = engine("s3://bucket-two/x", &o).unwrap();
        let c = engine("s3://bucket-one/x", &opts(&[("aws_region", "eu-west-1")])).unwrap();
        assert!(!Arc::ptr_eq(&a, &b));
        assert!(!Arc::ptr_eq(&a, &c));
        // Option order does not matter.
        let d = engine("gs://order-test/x", &opts(&[("a", "1"), ("b", "2")])).unwrap();
        let e = engine("gs://order-test/x", &opts(&[("b", "2"), ("a", "1")])).unwrap();
        assert!(Arc::ptr_eq(&d, &e));
    }

    #[test]
    fn every_supported_scheme_builds_a_store_without_network_or_credentials() {
        let o = opts(&[
            ("aws_region", "us-east-1"),
            ("azure_storage_account_name", "account"),
        ]);
        for url in [
            "s3://bkt/k",
            "s3a://bkt/k",
            "gs://bkt/k",
            "abfss://container@account.dfs.core.windows.net/k",
            "abfs://container@account.dfs.core.windows.net/k",
            "az://container/k",
            "file:///tmp/x",
            "memory:///x",
        ] {
            assert!(engine(url, &o).is_ok(), "{url}");
        }
    }

    #[test]
    fn options_for_other_stores_are_ignored() {
        // An S3 option on a GCS URL, and a nonsense key, must not fail the build.
        let o = opts(&[("aws_region", "us-west-2"), ("not_a_real_option", "x")]);
        assert!(engine("gs://ignore-test/k", &o).is_ok());
        assert!(engine("s3://ignore-test/k", &o).is_ok());
    }

    #[test]
    fn unsupported_locations_are_rejected_and_redacted() {
        let o = opts(&[]);
        for url in [
            "ftp://host/file",
            "http://host/file",
            "https://s3.us-west-2.amazonaws.com/bucket/key",
            "hdfs://namenode/x",
        ] {
            let err = engine(url, &o).err().unwrap();
            assert!(err.contains("unsupported storage scheme"), "{url}: {err}");
        }
        let err = engine("s3://", &o).err().unwrap();
        assert!(!err.is_empty());
        let err = engine("ftp://user:hunter2@host/file", &o).err().unwrap();
        assert!(!err.contains("hunter2"), "{err}");
    }
}
