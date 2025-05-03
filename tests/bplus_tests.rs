extern crate chunkfs;

use std::collections::HashMap;
use std::path::PathBuf;

use bplus_tree::bplus_tree::BPlus;
use rand::seq::SliceRandom;
use tempdir::TempDir;

#[test]
fn test_non_existent_key() {
    let tempdir = TempDir::new("non_existent").unwrap();
    let mut tree: BPlus<usize> = BPlus::new(2, tempdir.path().into()).unwrap();
    tree.insert(1, vec![1]).unwrap();
    assert!(tree.get(&2).is_err());
}

#[test]
fn test_overwrite_existing_key() {
    let tempdir = TempDir::new("overwrite").unwrap();
    let mut tree: BPlus<usize> = BPlus::new(2, tempdir.path().into()).unwrap();

    tree.insert(1, vec![1]).unwrap();
    tree.insert(1, vec![42]).unwrap();

    assert_eq!(tree.get(&1).unwrap(), vec![42]);
}

#[test]
fn test_insert_and_find() {
    let tempdir = TempDir::new("1").unwrap();
    let path = PathBuf::new().join(tempdir.path());
    let mut tree: BPlus<usize> = BPlus::new(2, path).unwrap();
    for i in 1..6 {
        let _ = tree.insert(i, vec![i as u8; 1]);
    }

    for i in 1..6 {
        let a = tree.get(&i).unwrap();
        assert_eq!(a, vec![i as u8; 1]);
    }
}

#[test]
fn test_insert_and_find_many_nodes() {
    let tempdir = TempDir::new("4").unwrap();
    let path = PathBuf::new().join(tempdir.path());
    let mut tree: BPlus<usize> = BPlus::new(2, path).unwrap();
    for i in 1..255 {
        let _ = tree.insert(i, vec![i as u8; 1]);
    }

    for i in 1..255 {
        assert_eq!(tree.get(&(i as usize)).unwrap(), vec![i as u8; 1]);
    }
}

#[test]
fn test_large_data_consecutive_numbers() {
    let tempdir = TempDir::new("6").unwrap();
    let path = PathBuf::new().join(tempdir.path());
    let mut tree: BPlus<usize> = BPlus::new(100, path).unwrap();
    for i in 1..10000 {
        let _ = tree.insert(i, vec![i as u8; 1064]);
    }
    for i in 1..10000 {
        let a = tree.get(&i).unwrap();
        assert_eq!(a, vec![i as u8; 1064]);
    }
}

#[test]
fn test_large_data() {
    let tempdir = TempDir::new("7").unwrap();
    let path = PathBuf::new().join(tempdir.path());
    let mut tree: BPlus<usize> = BPlus::new(2, path).unwrap();
    let mut htable = HashMap::<usize, Vec<u8>>::new();
    for i in 1..10000 {
        let key;
        key = i * 113;
        let _ = tree.insert(key, vec![key as u8; 1064]);
        htable.insert(key, vec![key as u8; 1064]);
    }
    for (key, value) in htable {
        assert_eq!(tree.get(&key).unwrap(), value);
    }
}

#[test]
fn test_couple_of_same_keys_inserted() {
    let tempdir = TempDir::new("8").unwrap();
    let mut tree: BPlus<usize> = BPlus::new(2, PathBuf::new().join(tempdir.path())).unwrap();
    for i in 1..100 {
        tree.insert(i, vec![1u8]).unwrap();
    }

    for i in 1..100 {
        for j in 1..100 {
            tree.insert(i, vec![j as u8]).unwrap();
        }
    }
    for i in 1..100 {
        for _ in 1..100 {
            tree.get(&i).unwrap();
        }
    }
}

#[test]
fn test_same_keys_inserted() {
    let tempdir = TempDir::new("10").unwrap();
    let mut tree: BPlus<usize> = BPlus::new(2, PathBuf::new().join(tempdir.path())).unwrap();
    let mut keys = vec![];
    for _ in 1..1000 {
        let key: usize = rand::random::<usize>() % 10000;
        keys.push(key);
    }

    for key in keys.clone() {
        tree.insert(key, vec![key as u8]).unwrap();
    }

    for key in keys {
        assert_eq!(vec![key as u8], tree.get(&key).unwrap());
    }

    let key: usize = rand::random();
    tree.insert(key, vec![0u8]).unwrap();
    for i in 1..255 {
        assert_eq!(vec![i - 1u8], tree.get(&key).unwrap());
        tree.insert(key, vec![i]).unwrap();
    }
}

#[test]
fn test_iterator() {
    let tempdir = TempDir::new("iterator_test").unwrap();
    let mut tree: BPlus<usize> = BPlus::new(2, tempdir.path().into()).unwrap();

    for i in 1..5 {
        tree.insert(i, vec![i as u8]).unwrap();
    }

    let mut iter = tree.into_iter();
    assert_eq!(iter.next(), Some((1, vec![1])));
    assert_eq!(iter.next(), Some((2, vec![2])));
    assert_eq!(iter.next(), Some((3, vec![3])));
    assert_eq!(iter.next(), Some((4, vec![4])));
    assert_eq!(iter.next(), None);
}
#[test]
fn test_empty_tree() {
    let tempdir = TempDir::new("empty").unwrap();
    let tree: BPlus<usize> = BPlus::new(2, tempdir.path().into()).unwrap();
    assert!(tree.get(&1).is_err());
}

#[test]
fn test_single_entry() {
    let tempdir = TempDir::new("single").unwrap();
    let mut tree = BPlus::new(2, tempdir.path().into()).unwrap();
    tree.insert(42, vec![1, 2, 3]).unwrap();
    assert_eq!(tree.get(&42).unwrap(), vec![1, 2, 3]);
}

#[test]
fn test_reverse_order_insert() {
    let tempdir = TempDir::new("reverse").unwrap();
    let mut tree: BPlus<usize> = BPlus::new(3, tempdir.path().into()).unwrap();

    for i in (1..100).rev() {
        tree.insert(i, vec![i as u8]).unwrap();
    }

    for i in 1..100 {
        assert_eq!(tree.get(&i).unwrap(), vec![i as u8]);
    }
}

#[test]
fn test_iterator_empty() {
    let tempdir = TempDir::new("iter_empty").unwrap();
    let tree: BPlus<usize> = BPlus::new(2, tempdir.path().into()).unwrap();
    let mut iter = tree.into_iter();
    assert_eq!(iter.next(), None);
}

#[test]
fn test_iterator_single() {
    let tempdir = TempDir::new("iter_single").unwrap();
    let mut tree = BPlus::new(2, tempdir.path().into()).unwrap();
    tree.insert(1, vec![1]).unwrap();
    let mut iter = tree.into_iter();
    assert_eq!(iter.next(), Some((1, vec![1])));
    assert_eq!(iter.next(), None);
}

#[test]
fn test_iterator_large_dataset() {
    let tempdir = TempDir::new("iter_large").unwrap();
    let mut tree = BPlus::new(10, tempdir.path().into()).unwrap();
    let mut expected = Vec::new();

    for i in 1..=21 {
        tree.insert(i, vec![i as u8]).unwrap();
        expected.push((i, vec![i as u8]));
    }

    let result: Vec<_> = tree.into_iter().collect();
    assert_eq!(result, expected);
}

#[test]
fn test_minimal_degree() {
    let tempdir = TempDir::new("min_degree").unwrap();
    let mut tree = BPlus::new(1, tempdir.path().into()).unwrap();

    for i in 1..=10 {
        tree.insert(i, vec![i as u8]).unwrap();
    }

    assert_eq!(tree.get(&5).unwrap(), vec![5]);
}

#[test]
fn test_key_duplication() {
    let tempdir = TempDir::new("dupes").unwrap();
    let mut tree = BPlus::new(2, tempdir.path().into()).unwrap();

    for _ in 0..10 {
        tree.insert(42, vec![1]).unwrap();
        tree.insert(42, vec![2]).unwrap();
    }

    assert_eq!(tree.get(&42).unwrap(), vec![2]);
}

#[test]
fn test_string_keys() {
    let tempdir = TempDir::new("string_keys").unwrap();
    let mut tree = BPlus::new(2, tempdir.path().into()).unwrap();

    tree.insert("apple".to_string(), b"fruit".to_vec()).unwrap();
    tree.insert("banana".to_string(), b"yellow".to_vec())
        .unwrap();

    assert_eq!(tree.get(&"apple".to_string()).unwrap(), b"fruit");
    assert_eq!(tree.get(&"banana".to_string()).unwrap(), b"yellow");
}

#[test]
fn test_stress_1m_entries() {
    let tempdir = TempDir::new("stress_1m").unwrap();
    let mut tree = BPlus::new(100, tempdir.path().into()).unwrap();

    for i in 0..1_000_000 {
        tree.insert(i, vec![i as u8]).unwrap();
    }

    for i in 0..1_000_000 {
        assert_eq!(tree.get(&i).unwrap(), vec![i as u8]);
    }
}

#[test]
fn test_find_nonexistent_after_splits() {
    let tempdir = TempDir::new("nonexistent").unwrap();
    let mut tree = BPlus::new(2, tempdir.path().into()).unwrap();

    for i in 0..1000 {
        tree.insert(i, vec![1]).unwrap();
    }

    assert!(tree.get(&1001).is_err());
}

#[test]
fn test_entry_ordering() {
    let tempdir = TempDir::new("ordering").unwrap();
    let mut tree = BPlus::new(3, tempdir.path().into()).unwrap();
    let mut rng = rand::thread_rng();
    let mut nums: Vec<u32> = (0..1000).collect();
    nums.shuffle(&mut rng);

    for &num in &nums {
        tree.insert(num, vec![num as u8]).unwrap();
    }

    let mut sorted = nums.clone();
    sorted.sort_unstable();
    let result: Vec<_> = tree.into_iter().map(|(k, _)| k).collect();
    assert_eq!(result, sorted);
}

#[test]
fn test_custom_data_types() {
    #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Default)]
    struct ComplexKey {
        id: u64,
        name: String,
    }

    let tempdir = TempDir::new("custom_type").unwrap();
    let mut tree = BPlus::new(2, tempdir.path().into()).unwrap();

    let key1 = ComplexKey {
        id: 1,
        name: "A".to_string(),
    };
    let key2 = ComplexKey {
        id: 2,
        name: "B".to_string(),
    };

    tree.insert(key1.clone(), b"data1".to_vec()).unwrap();
    tree.insert(key2.clone(), b"data2".to_vec()).unwrap();

    assert_eq!(tree.get(&key1).unwrap(), b"data1");
    assert_eq!(tree.get(&key2).unwrap(), b"data2");
}
