//! Media storage abstraction.

use async_trait::async_trait;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::errors::{GatewayError, Result};

/// Boxed, shared media backend handle used across the framework.
pub type DynMediaBackend = Arc<dyn MediaBackend>;

/// Abstract backend for persisting media files.
///
/// Backends are addressed by an opaque `key` (string). The key is also what
/// gets stamped onto `ContentPart.local_path` for downstream readers, so its
/// semantics are fully owned by the backend implementation.
#[async_trait]
pub trait MediaBackend: Send + Sync {
    /// Persist media data under the given key.
    async fn save(&self, data: &[u8], key: &str) -> Result<()>;

    /// Read previously saved media data. Returns an error if missing.
    async fn read(&self, key: &str) -> Result<Vec<u8>>;

    /// Check whether a key has been saved.
    async fn exists(&self, key: &str) -> bool;

    /// Resolve the on-disk path for a key, when one exists (filesystem backends).
    fn get_local_path(&self, key: &str) -> Option<PathBuf> {
        let _ = key;
        None
    }
}

/// Filesystem-based media backend. Stores files at `{root_path}/{key}`.
pub struct FileSystemMediaBackend {
    root: PathBuf,
}

impl FileSystemMediaBackend {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        FileSystemMediaBackend { root: root.into() }
    }

    pub fn root_path(&self) -> &Path {
        &self.root
    }

    fn resolve(&self, key: &str) -> PathBuf {
        // Treat the key as a relative path under root (keys are already
        // namespaced as "{channel_type}/{channel_id}/...").
        let clean = key.trim_start_matches('/');
        self.root.join(clean)
    }
}

#[async_trait]
impl MediaBackend for FileSystemMediaBackend {
    async fn save(&self, data: &[u8], key: &str) -> Result<()> {
        let path = self.resolve(key);
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        tokio::fs::write(&path, data).await?;
        Ok(())
    }

    async fn read(&self, key: &str) -> Result<Vec<u8>> {
        let path = self.resolve(key);
        if !path.exists() {
            return Err(GatewayError::MediaNotFound(key.to_string()));
        }
        let data = tokio::fs::read(&path).await?;
        Ok(data)
    }

    async fn exists(&self, key: &str) -> bool {
        self.resolve(key).exists()
    }

    fn get_local_path(&self, key: &str) -> Option<PathBuf> {
        Some(self.resolve(key))
    }
}
