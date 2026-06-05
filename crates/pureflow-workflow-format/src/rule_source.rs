//! Pluggable rule set source abstraction for resolving `rule_set_ref` URIs.
//!
//! `RuleSetSource` implementations resolve a URI string into a validated
//! `RuleSet`. Plain paths (no scheme) are handled by `LocalFsSource`.
//! Applications needing remote or database-backed rule delivery register
//! their own implementations in a `SourceRegistry`.

use std::error::Error;
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use futures::future::BoxFuture;
use pureflow_rules::RuleSet;

/// Error returned when a rule set source cannot resolve or parse a URI.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RuleSourceError {
    /// The URI scheme is not registered in the source registry.
    UnknownScheme {
        /// The unrecognised URI scheme prefix (e.g. `"guardiandb"`).
        scheme: String,
    },
    /// The referenced file does not exist.
    FileNotFound {
        /// Path that was not found.
        path: String,
    },
    /// The file could not be read.
    ReadError {
        /// Path that could not be read.
        path: String,
        /// Underlying I/O error description.
        reason: String,
    },
    /// The content could not be parsed as a valid rule set.
    ParseError {
        /// Human-readable description of the parse failure.
        reason: String,
    },
}

impl fmt::Display for RuleSourceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownScheme { scheme } => {
                write!(f, "no source registered for URI scheme `{scheme}`")
            }
            Self::FileNotFound { path } => {
                write!(f, "rule set file not found: `{path}`")
            }
            Self::ReadError { path, reason } => {
                write!(f, "failed to read rule set file `{path}`: {reason}")
            }
            Self::ParseError { reason } => {
                write!(f, "failed to parse rule set: {reason}")
            }
        }
    }
}

impl Error for RuleSourceError {}

/// Async source that loads a `RuleSet` from a URI string.
///
/// Implementations are object-safe via `BoxFuture`. The URI scheme determines
/// which implementation handles each `ref_uri`:
///
/// - Plain paths (no `://`) → `LocalFsSource`
/// - `embedded://` → `EmbeddedSource`
/// - All other schemes → application-registered implementations
///
/// Application code registers custom implementations in a `SourceRegistry`.
/// There is no global singleton; sources are injected at construction time.
pub trait RuleSetSource: Send + Sync {
    /// Load a `RuleSet` from the given URI, resolving it against this source.
    fn load<'a>(&'a self, ref_uri: &'a str) -> BoxFuture<'a, Result<RuleSet, RuleSourceError>>;
}

/// Registry that dispatches `rule_set_ref` URIs to registered `RuleSetSource`
/// implementations.
///
/// Dispatch rules:
/// - URIs without a `://` separator are treated as plain filesystem paths and
///   handled by the built-in `LocalFsSource` using the configured base directory.
/// - URIs with a scheme prefix (e.g. `"guardiandb://..."`) are forwarded to the
///   registered source for that scheme, if any.
/// - URIs with an unrecognised scheme return `RuleSourceError::UnknownScheme`.
///
/// Two built-in sources are always available:
/// - `LocalFsSource` — plain paths and `file://` URIs
/// - `EmbeddedSource` — `embedded://` URIs (pre-parsed inline rule sets)
#[derive(Default)]
pub struct SourceRegistry {
    /// Registered (scheme, source) pairs, checked in insertion order.
    sources: Vec<(String, Arc<dyn RuleSetSource>)>,
    /// Base directory for resolving plain path URIs.
    base_dir: Option<PathBuf>,
}

impl SourceRegistry {
    /// Create an empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a registry with a base directory for plain-path resolution.
    #[must_use]
    pub fn with_base_dir(base_dir: impl Into<PathBuf>) -> Self {
        Self {
            sources: Vec::new(),
            base_dir: Some(base_dir.into()),
        }
    }

    /// Register a source for the given URI scheme prefix.
    ///
    /// The scheme should be the bare prefix without `://`, e.g. `"guardiandb"`.
    /// Registrations are checked in insertion order; the first matching scheme
    /// wins.
    pub fn register(&mut self, scheme: impl Into<String>, source: Arc<dyn RuleSetSource>) {
        self.sources.push((scheme.into(), source));
    }

    /// Resolve a `rule_set_ref` URI to a `RuleSet`.
    ///
    /// # Errors
    ///
    /// Returns `RuleSourceError::UnknownScheme` when the URI has a scheme that
    /// is not registered. Returns other variants for I/O and parse failures.
    pub async fn load(&self, ref_uri: &str) -> Result<RuleSet, RuleSourceError> {
        if let Some(scheme) = extract_scheme(ref_uri) {
            // Scheme-based dispatch.
            for (registered_scheme, source) in &self.sources {
                if registered_scheme == scheme {
                    return source.load(ref_uri).await;
                }
            }
            return Err(RuleSourceError::UnknownScheme {
                scheme: scheme.to_owned(),
            });
        }

        // Plain path — use LocalFsSource with the configured base directory.
        let base = self.base_dir.as_deref().unwrap_or(Path::new("."));
        LocalFsSource::new(base).load(ref_uri).await
    }
}

impl fmt::Debug for SourceRegistry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SourceRegistry")
            .field("schemes", &self.sources.iter().map(|(s, _)| s.as_str()).collect::<Vec<_>>())
            .field("base_dir", &self.base_dir)
            .finish()
    }
}

/// Extract the scheme prefix from a URI (the part before `://`), if any.
fn extract_scheme(uri: &str) -> Option<&str> {
    uri.find("://").map(|pos| &uri[..pos])
}

/// Built-in source that reads rule sets from the local filesystem.
///
/// Plain paths are resolved relative to `base_dir`. Absolute paths are used
/// as-is. `file://` URIs are also handled by this source (scheme is stripped).
#[derive(Debug, Clone)]
pub struct LocalFsSource {
    base_dir: PathBuf,
}

impl LocalFsSource {
    /// Create a local filesystem source that resolves relative paths against
    /// `base_dir` (typically the workflow file's parent directory).
    #[must_use]
    pub fn new(base_dir: impl Into<PathBuf>) -> Self {
        Self {
            base_dir: base_dir.into(),
        }
    }

    fn resolve_path(&self, ref_uri: &str) -> PathBuf {
        // Strip `file://` scheme if present.
        let path_str = ref_uri.strip_prefix("file://").unwrap_or(ref_uri);
        let path = Path::new(path_str);
        if path.is_absolute() {
            path.to_path_buf()
        } else {
            self.base_dir.join(path)
        }
    }
}

impl RuleSetSource for LocalFsSource {
    fn load<'a>(&'a self, ref_uri: &'a str) -> BoxFuture<'a, Result<RuleSet, RuleSourceError>> {
        Box::pin(async move {
            let path = self.resolve_path(ref_uri);
            let path_str = path.to_string_lossy().into_owned();

            if !path.exists() {
                return Err(RuleSourceError::FileNotFound { path: path_str });
            }

            let content = fs::read_to_string(&path).map_err(|e| RuleSourceError::ReadError {
                path: path_str.clone(),
                reason: e.to_string(),
            })?;

            serde_json::from_str::<RuleSet>(&content).map_err(|e| RuleSourceError::ParseError {
                reason: format!("{path_str}: {e}"),
            })
        })
    }
}

/// Built-in source that wraps a pre-parsed `RuleSet`.
///
/// Used for:
/// - Inline rule sets already deserialized from the workflow JSON document.
/// - `embedded://` URI scheme (the URI string is ignored; the contained rule
///   set is returned unconditionally).
/// - Test fixtures that need a source without filesystem access.
#[derive(Debug, Clone)]
pub struct EmbeddedSource {
    rule_set: Arc<RuleSet>,
}

impl EmbeddedSource {
    /// Wrap an already-validated rule set.
    #[must_use]
    pub fn new(rule_set: RuleSet) -> Self {
        Self {
            rule_set: Arc::new(rule_set),
        }
    }

    /// Deserialize a rule set from inline JSON.
    ///
    /// # Errors
    ///
    /// Returns `RuleSourceError::ParseError` if the JSON does not represent a
    /// valid `RuleSet`.
    pub fn from_json(json: &serde_json::Value) -> Result<Self, RuleSourceError> {
        let rule_set: RuleSet =
            serde_json::from_value(json.clone()).map_err(|e| RuleSourceError::ParseError {
                reason: e.to_string(),
            })?;
        Ok(Self::new(rule_set))
    }
}

impl RuleSetSource for EmbeddedSource {
    fn load<'a>(&'a self, _ref_uri: &'a str) -> BoxFuture<'a, Result<RuleSet, RuleSourceError>> {
        let rule_set = (*self.rule_set).clone();
        Box::pin(async move { Ok(rule_set) })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use pureflow_rules::{Condition, EvaluationStrategy, Rule, RuleAction, RuleSet};
    use pureflow_types::PortId;

    fn port(s: &str) -> PortId { PortId::new(s).unwrap() }

    fn sample_rule_set() -> RuleSet {
        RuleSet::new(
            "test-set",
            EvaluationStrategy::FirstMatch,
            vec![Rule::new(
                "always-route",
                Condition::Always,
                RuleAction::Route(port("out")),
                10,
                "route everything",
            ).unwrap()],
            RuleAction::Drop,
            false,
        ).unwrap()
    }

    #[test]
    fn extract_scheme_from_uri_with_scheme() {
        assert_eq!(extract_scheme("guardiandb://path/to/rules"), Some("guardiandb"));
        assert_eq!(extract_scheme("file:///absolute/path.json"), Some("file"));
        assert_eq!(extract_scheme("embedded://"), Some("embedded"));
    }

    #[test]
    fn extract_scheme_returns_none_for_plain_paths() {
        assert_eq!(extract_scheme("rules/account-router.json"), None);
        assert_eq!(extract_scheme("./relative/path.json"), None);
        assert_eq!(extract_scheme("/absolute/path.json"), None);
    }

    #[test]
    fn embedded_source_round_trips_rule_set() {
        let original = sample_rule_set();
        let source = EmbeddedSource::new(original.clone());
        let loaded = futures::executor::block_on(source.load("embedded://any-uri"))
            .expect("embedded source must return rule set");
        assert_eq!(loaded, original);
    }

    #[test]
    fn embedded_source_from_json_deserializes_rule_set() {
        let original = sample_rule_set();
        let json = serde_json::to_value(&original).expect("rule set serializes");
        let source = EmbeddedSource::from_json(&json).expect("valid JSON parses");
        let loaded = futures::executor::block_on(source.load("embedded://"))
            .expect("embedded source must return rule set");
        assert_eq!(loaded, original);
    }

    #[test]
    fn embedded_source_from_invalid_json_returns_parse_error() {
        let bad = serde_json::json!({"not_a_rule_set": true});
        let err = EmbeddedSource::from_json(&bad).expect_err("invalid JSON must fail");
        assert!(matches!(err, RuleSourceError::ParseError { .. }));
    }

    #[test]
    fn source_registry_unknown_scheme_returns_error() {
        let registry = SourceRegistry::new();
        let err = futures::executor::block_on(registry.load("unknownscheme://path/to/rules"))
            .expect_err("unknown scheme must fail");
        assert!(
            matches!(&err, RuleSourceError::UnknownScheme { scheme } if scheme == "unknownscheme")
        );
    }

    #[test]
    fn source_registry_dispatches_to_registered_source() {
        let original = sample_rule_set();
        let source = Arc::new(EmbeddedSource::new(original.clone()));

        let mut registry = SourceRegistry::new();
        registry.register("myscheme", source);

        let loaded = futures::executor::block_on(registry.load("myscheme://whatever"))
            .expect("registered scheme must dispatch");
        assert_eq!(loaded, original);
    }

    #[test]
    fn local_fs_source_missing_file_returns_not_found() {
        let source = LocalFsSource::new("/nonexistent/dir");
        let err = futures::executor::block_on(source.load("missing.json"))
            .expect_err("missing file must fail");
        assert!(matches!(err, RuleSourceError::FileNotFound { .. }));
    }

    #[test]
    fn local_fs_source_resolves_relative_path_against_base_dir() {
        // Write a temporary rule set file and load it.
        let dir = tempfile::tempdir().expect("temp dir");
        let original = sample_rule_set();
        let content = serde_json::to_string(&original).expect("serializes");
        let file_path = dir.path().join("test.rules.json");
        std::fs::write(&file_path, &content).expect("write temp file");

        let source = LocalFsSource::new(dir.path());
        let loaded = futures::executor::block_on(source.load("test.rules.json"))
            .expect("relative path must resolve");
        assert_eq!(loaded, original);
    }

    #[test]
    fn source_registry_plain_path_uses_local_fs() {
        let dir = tempfile::tempdir().expect("temp dir");
        let original = sample_rule_set();
        let content = serde_json::to_string(&original).expect("serializes");
        let file_path = dir.path().join("router.rules.json");
        std::fs::write(&file_path, &content).expect("write temp file");

        let registry = SourceRegistry::with_base_dir(dir.path());
        let loaded = futures::executor::block_on(registry.load("router.rules.json"))
            .expect("plain path must resolve via LocalFsSource");
        assert_eq!(loaded, original);
    }
}
