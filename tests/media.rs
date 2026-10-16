//! Tests for the filesystem media backend (mirrors tests/test_media.py).

use std::time::{SystemTime, UNIX_EPOCH};

use buddy_gateway::media::{FileSystemMediaBackend, MediaBackend};

fn temp_root() -> std::path::PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!("buddy-gw-media-{nanos}"));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

#[tokio::test]
async fn save_read_exists_round_trip() {
    let root = temp_root();
    let backend = FileSystemMediaBackend::new(&root);

    assert!(!backend.exists("a/b.bin").await);
    backend.save(b"hello bytes", "a/b.bin").await.unwrap();
    assert!(backend.exists("a/b.bin").await);

    let data = backend.read("a/b.bin").await.unwrap();
    assert_eq!(data, b"hello bytes");

    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn read_missing_errors() {
    let root = temp_root();
    let backend = FileSystemMediaBackend::new(&root);
    assert!(backend.read("nope.bin").await.is_err());
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn get_local_path_resolves_under_root() {
    let root = temp_root();
    let backend = FileSystemMediaBackend::new(&root);
    let path = backend.get_local_path("x/y.png").unwrap();
    assert!(path.starts_with(&root));
    assert!(path.ends_with("x/y.png") || path.ends_with("x\\y.png"));
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn overwrite_existing_key() {
    let root = temp_root();
    let backend = FileSystemMediaBackend::new(&root);
    backend.save(b"v1", "k.bin").await.unwrap();
    backend.save(b"v2", "k.bin").await.unwrap();
    assert_eq!(backend.read("k.bin").await.unwrap(), b"v2");
    let _ = std::fs::remove_dir_all(&root);
}
