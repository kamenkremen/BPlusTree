extern crate chunkfs;

use bplus_tree::bplus_tree::BPlus;
use std::collections::HashMap;
use std::path::PathBuf;
use tempdir::TempDir;

#[tokio::test(flavor = "multi_thread")]
async fn test_non_existent_key() {
    let tempdir = TempDir::new("non_existent").unwrap();
    let tree: BPlus<usize> = BPlus::new(2, tempdir.path().into()).unwrap();
    tree.insert(1, vec![1]).await.unwrap();
    assert!(tree.get(&2).await.is_err());
}

#[tokio::test(flavor = "multi_thread")]
async fn test_overwrite_existing_key() {
    let tempdir = TempDir::new("overwrite").unwrap();
    let tree: BPlus<usize> = BPlus::new(2, tempdir.path().into()).unwrap();

    tree.insert(1, vec![1]).await.unwrap();
    tree.insert(1, vec![42]).await.unwrap();

    assert_eq!(tree.get(&1).await.unwrap(), vec![42]);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_insert_and_find() {
    let tempdir = TempDir::new("1").unwrap();
    let path = PathBuf::new().join(tempdir.path());
    let tree: BPlus<usize> = BPlus::new(2, path).unwrap();
    for i in 1..6 {
        tree.insert(i, vec![i as u8; 1]).await.unwrap();
    }

    for i in 1..6 {
        let a = tree.get(&i).await.unwrap();
        assert_eq!(a, vec![i as u8; 1]);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_insert_and_find_many_nodes() {
    let tempdir = TempDir::new("4").unwrap();
    let path = PathBuf::new().join(tempdir.path());
    let tree: BPlus<usize> = BPlus::new(2, path).unwrap();
    for i in 1..255 {
        tree.insert(i, vec![i as u8; 1]).await.unwrap();
    }

    for i in 1..255 {
        assert_eq!(tree.get(&(i as usize)).await.unwrap(), vec![i as u8; 1]);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_large_data_consecutive_numbers() {
    let tempdir = TempDir::new("6").unwrap();
    let path = PathBuf::new().join(tempdir.path());
    let tree: BPlus<usize> = BPlus::new(100, path).unwrap();
    for i in 1..10000 {
        tree.insert(i, vec![i as u8; 1064]).await.unwrap();
    }
    for i in 1..10000 {
        let a = tree.get(&i).await.unwrap();
        assert_eq!(a, vec![i as u8; 1064]);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_large_data() {
    let tempdir = TempDir::new("7").unwrap();
    let path = PathBuf::new().join(tempdir.path());
    let tree: BPlus<usize> = BPlus::new(2, path).unwrap();
    let mut htable = HashMap::<usize, Vec<u8>>::new();
    for i in 1..10000 {
        let key = i * 113;
        tree.insert(key, vec![key as u8; 1064]).await.unwrap();
        htable.insert(key, vec![key as u8; 1064]);
    }
    for (key, value) in htable {
        assert_eq!(tree.get(&key).await.unwrap(), value);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_couple_of_same_keys_inserted() {
    let tempdir = TempDir::new("8").unwrap();
    let tree: BPlus<usize> = BPlus::new(2, PathBuf::new().join(tempdir.path())).unwrap();
    for i in 1..100 {
        tree.insert(i, vec![1u8]).await.unwrap();
    }

    for i in 1..100 {
        for j in 1..100 {
            tree.insert(i, vec![j as u8]).await.unwrap();
        }
    }
    for i in 1..100 {
        for _ in 1..100 {
            tree.get(&i).await.unwrap();
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_same_keys_inserted() {
    let tempdir = TempDir::new("10").unwrap();
    let tree: BPlus<usize> = BPlus::new(2, PathBuf::new().join(tempdir.path())).unwrap();
    let mut keys = vec![];
    for _ in 1..100000 {
        let key: usize = rand::random::<usize>() % 10000;
        keys.push(key);
    }

    for key in keys.clone() {
        tree.insert(key, vec![key as u8]).await.unwrap();
    }

    for key in keys {
        assert_eq!(vec![key as u8], tree.get(&key).await.unwrap());
    }

    let key: usize = rand::random();
    tree.insert(key, vec![0u8]).await.unwrap();
    for i in 1..255 {
        assert_eq!(vec![i - 1u8], tree.get(&key).await.unwrap());
        tree.insert(key, vec![i]).await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_same_10k_keys_inserted() {
    let tempdir = TempDir::new("same_10k_keys").unwrap();
    let tree: BPlus<usize> = BPlus::new(2, PathBuf::new().join(tempdir.path())).unwrap();

    for i in 0..10000 {
        tree.insert(i, vec![i as u8]).await.unwrap();
    }

    for i in 0..10000 {
        tree.insert(i, vec![i as u8]).await.unwrap();
    }

    for key in 1..10000 {
        assert_eq!(vec![key as u8], tree.get(&key).await.unwrap());
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_empty_tree() {
    let tempdir = TempDir::new("empty").unwrap();
    let tree: BPlus<usize> = BPlus::new(2, tempdir.path().into()).unwrap();
    assert!(tree.get(&1).await.is_err());
}

#[tokio::test(flavor = "multi_thread")]
async fn test_single_entry() {
    let tempdir = TempDir::new("single").unwrap();
    let tree = BPlus::new(2, tempdir.path().into()).unwrap();
    tree.insert(42, vec![1, 2, 3]).await.unwrap();
    assert_eq!(tree.get(&42).await.unwrap(), vec![1, 2, 3]);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_reverse_order_insert() {
    let tempdir = TempDir::new("reverse").unwrap();
    let tree: BPlus<usize> = BPlus::new(3, tempdir.path().into()).unwrap();

    for i in (1..100).rev() {
        tree.insert(i, vec![i as u8]).await.unwrap();
    }

    for i in 1..100 {
        assert_eq!(tree.get(&i).await.unwrap(), vec![i as u8]);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_minimal_degree() {
    let tempdir = TempDir::new("min_degree").unwrap();
    let tree = BPlus::new(1, tempdir.path().into()).unwrap();

    for i in 1..=10 {
        tree.insert(i, vec![i as u8]).await.unwrap();
    }

    assert_eq!(tree.get(&5).await.unwrap(), vec![5]);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_key_duplication() {
    let tempdir = TempDir::new("dupes").unwrap();
    let tree = BPlus::new(2, tempdir.path().into()).unwrap();

    for _ in 0..10 {
        tree.insert(42, vec![1]).await.unwrap();
        tree.insert(42, vec![2]).await.unwrap();
    }

    assert_eq!(tree.get(&42).await.unwrap(), vec![2]);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_string_keys() {
    let tempdir = TempDir::new("string_keys").unwrap();
    let tree = BPlus::new(2, tempdir.path().into()).unwrap();

    tree.insert("apple".to_string(), b"fruit".to_vec())
        .await
        .unwrap();
    tree.insert("banana".to_string(), b"yellow".to_vec())
        .await
        .unwrap();

    assert_eq!(tree.get(&"apple".to_string()).await.unwrap(), b"fruit");
    assert_eq!(tree.get(&"banana".to_string()).await.unwrap(), b"yellow");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_stress_1m_entries() {
    let tempdir = TempDir::new("stress_1m").unwrap();
    let tree = BPlus::new(100, tempdir.path().into()).unwrap();

    for i in 0..1_000_000 {
        tree.insert(i, vec![i as u8]).await.unwrap();
    }

    for i in 0..1_000_000 {
        assert_eq!(tree.get(&i).await.unwrap(), vec![i as u8]);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 16)]
async fn test_stress_1m_entries_concurrent() {
    use std::sync::Arc;
    use tokio::task;

    let tempdir = TempDir::new("stress_1m").unwrap();
    let tree = Arc::new(BPlus::new(100, tempdir.path().into()).unwrap());
    let num_tasks = 1000;
    let entries_per_task = 1000;
    let barrier = Arc::new(tokio::sync::Barrier::new(num_tasks + 1));

    let mut insert_handles = Vec::with_capacity(num_tasks);
    for task_id in 0..num_tasks {
        let tree = Arc::clone(&tree);
        let barrier = Arc::clone(&barrier);

        insert_handles.push(task::spawn(async move {
            barrier.wait().await;

            for i in 0..entries_per_task {
                let key = (task_id * entries_per_task) + i;
                tree.insert(key, vec![key as u8]).await.unwrap();
            }
        }));
    }

    barrier.wait().await;

    for handle in insert_handles {
        handle.await.unwrap();
    }

    let mut verify_handles = Vec::with_capacity(num_tasks);
    for task_id in 0..num_tasks {
        let tree = Arc::clone(&tree);

        verify_handles.push(task::spawn(async move {
            for i in 0..entries_per_task {
                let key = (task_id * entries_per_task) + i;
                let value = tree.get(&key).await.unwrap();
                assert_eq!(value, vec![key as u8], "Invalid value for key {}", key);
            }
        }));
    }

    for handle in verify_handles {
        handle.await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_find_nonexistent_after_splits() {
    let tempdir = TempDir::new("nonexistent").unwrap();
    let tree = BPlus::new(2, tempdir.path().into()).unwrap();

    for i in 0..1000 {
        tree.insert(i, vec![1]).await.unwrap();
    }

    assert!(tree.get(&1001).await.is_err());
}
