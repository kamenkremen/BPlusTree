use chunkfs::{Data, DataContainer, Database};

use crate::chunk_pointer::{ChunkHandler, ChunkPointer};
use std::{
    cell::RefCell,
    fmt::Debug,
    fs::File,
    io::{self, ErrorKind},
    os::unix::fs::FileExt,
    path::PathBuf,
    rc::Rc,
};

extern crate chunkfs;

/// A type that represents a reference to another node
type Link<K> = Rc<RefCell<Node<K>>>;

/// Represents a node in a B+ tree.
/// All data resides in leaf nodes, while internal nodes
/// manage navigation between children
#[derive(Clone)]
enum Node<K> {
    Internal(InternalNode<K>),
    Leaf(Leaf<K>),
}

/// Internal node in a B+ tree
#[derive(Clone)]
struct InternalNode<K> {
    children: Vec<Link<K>>,
    keys: Vec<Rc<K>>,
}

/// Leaf node in a B+ tree
#[derive(Default, Clone)]
struct Leaf<K> {
    entries: Vec<(Rc<K>, ChunkHandler)>,
    next: Option<Link<K>>,
}

/// B+ tree
pub struct BPlus<K> {
    root: Node<K>,
    t: usize,
    path: PathBuf,
    file_number: usize,
    offset: u64,
    current_file: File,
    max_file_size: u64,
}

#[allow(dead_code)]
impl<K: Ord + Debug> BPlus<K> {
    /// Prints B+ tree for debug purposes
    pub fn print_tree(&self) {
        BPlus::print_node(&self.root, 0);
    }

    fn print_node(node: &Node<K>, level: usize) {
        match node {
            Node::Internal(internal) => {
                println!("{}[Internal] keys: {:?}", "  ".repeat(level), internal.keys);
                for child in &internal.children {
                    BPlus::print_node(&child.borrow(), level + 1);
                }
            }
            Node::Leaf(leaf) => {
                let entries: Vec<String> = leaf
                    .entries
                    .iter()
                    .map(|(k, _)| format!("{:?}", k))
                    .collect();
                println!("{}[Leaf] entries: {:?}", "  ".repeat(level), entries);
            }
        }
    }
}

#[allow(dead_code)]
impl<K: Default + Ord + Clone + Debug> BPlus<K> {
    /// Creates new instance of B+ tree with given t and path
    /// t represents minimal and maximal quantity of keys in node
    /// All data will be written in files in directory by given path
    pub fn new(t: usize, path: PathBuf) -> io::Result<Self> {
        let path_to_file = path.join("0");
        let current_file = File::create(path_to_file)?;
        Ok(Self {
            root: Node::Leaf(Leaf::default()),
            t,
            path,
            file_number: 0,
            offset: 0,
            current_file,
            max_file_size: 2 << 20,
        })
    }

    /// Creates new chunk_handler and writes data to a file
    fn get_chunk_handler(&mut self, value: Vec<u8>) -> io::Result<ChunkHandler> {
        if self.offset >= self.max_file_size {
            self.file_number += 1;
            self.offset = 0;
            self.current_file =
                File::create(self.path.join(format!("{}", self.file_number))).unwrap();
        }

        let value_size = value.len();
        self.current_file.write_at(&value, self.offset)?;
        let value_to_insert = ChunkHandler::new(
            self.path.join(format!("{0}", self.file_number)),
            self.offset,
            value.len(),
        );
        self.offset += value_size as u64;
        Ok(value_to_insert)
    }

    /// Inserts given value by given key in the B+ tree
    /// Returns Err(_) if file could not be created
    pub fn insert(&mut self, key: K, value: Vec<u8>) -> io::Result<()> {
        let value_to_insert = self.get_chunk_handler(value).unwrap();

        let key = Rc::new(key);

        if let Some((new_node_link, new_key)) = self.root.insert(key, value_to_insert, self.t) {
            {
                let mut prev_root = self.root.clone();
                if let Node::Leaf(mut leaf) = prev_root {
                    leaf.next = Some(new_node_link.clone());
                    prev_root = Node::Leaf(leaf);
                }

                let new_root = Node::<K>::Internal(InternalNode {
                    children: vec![Rc::new(RefCell::new(prev_root)), new_node_link],
                    keys: vec![new_key],
                });

                self.root = new_root;
            }
        }
        Ok(())
    }

    #[allow(unused_variables)]
    fn remove(&mut self, key: Rc<K>) -> io::Result<()> {
        unimplemented!()
    }

    /// Gets value from a B+ tree by given key
    pub fn get(&self, key: &K) -> io::Result<Vec<u8>> {
        self.root.get(key)
    }
}

impl<K: Clone + Ord + Debug> Node<K> {
    /// Splits node into two and returns new node with it first key
    fn split(&mut self, t: usize) -> (Link<K>, Rc<K>) {
        match self {
            Node::Leaf(leaf) => {
                let new_leaf_entries = leaf.entries.split_off(t);
                let middle_key = new_leaf_entries[0].0.clone();

                let new_leaf = Node::Leaf(Leaf {
                    entries: new_leaf_entries,
                    next: leaf.next.take(),
                });

                let new_leaf_link = Rc::new(RefCell::new(new_leaf.clone()));
                leaf.next = Some(new_leaf_link.clone());

                (new_leaf_link, middle_key)
            }
            Node::Internal(internal_node) => {
                let mut new_node_keys = internal_node.keys.split_off(t - 1);
                let middle_key = new_node_keys.remove(0);

                let new_node_children = internal_node.children.split_off(t);

                let new_node = Node::Internal(InternalNode {
                    children: new_node_children,
                    keys: new_node_keys,
                });

                (Rc::new(RefCell::new(new_node)), middle_key)
            }
        }
    }

    /// Inserts given value by given key
    fn insert(&mut self, key: Rc<K>, value: ChunkHandler, t: usize) -> Option<(Link<K>, Rc<K>)> {
        match self {
            Node::Leaf(leaf) => {
                match leaf.entries.binary_search_by(|(k, _)| k.cmp(&key)) {
                    Ok(x) => leaf.entries[x] = (key.clone(), value),
                    Err(x) => leaf.entries.insert(x, (key.clone(), value)),
                };

                if leaf.entries.len() == 2 * t {
                    return Some(self.split(t));
                }

                None
            }
            Node::Internal(internal_node) => {
                let pos = match internal_node.keys.binary_search(&key) {
                    Ok(x) => x + 1,
                    Err(x) => x,
                };
                let child = internal_node.children[pos].clone();
                let mut borrowed_child = child.borrow_mut();
                let result = borrowed_child.insert(key, value, t);

                match result {
                    Some((new_child, key)) => {
                        internal_node.keys.insert(pos, key.clone());
                        internal_node.children.insert(pos + 1, new_child);

                        match internal_node.keys.len() {
                            val if val == 2 * t - 1 => Some(self.split(t)),
                            _ => None,
                        }
                    }
                    None => None,
                }
            }
        }
    }

    #[allow(unused_variables, dead_code)]
    fn remove(&mut self, key: &K, t: usize) -> io::Result<()> {
        unimplemented!()
    }

    /// Gets value from a B+ tree by given key
    fn get(&self, key: &K) -> io::Result<Vec<u8>> {
        match self {
            Node::Leaf(leaf) => match leaf.entries.binary_search_by(|(k, _)| k.as_ref().cmp(key)) {
                Ok(pos) => Ok(leaf.entries[pos].1.read().unwrap()),
                Err(_) => Err(ErrorKind::NotFound.into()),
            },
            Node::Internal(internal_node) => {
                let pos = match internal_node.keys.binary_search_by(|k| k.as_ref().cmp(key)) {
                    Ok(x) => x + 1,
                    Err(x) => x,
                };

                let child = internal_node.children.get(pos);
                match child {
                    Some(x) => x.clone().borrow().get(key),
                    None => Err(ErrorKind::NotFound.into()),
                }
            }
        }
    }
}

impl<K: Ord + Clone + Default + Debug> Database<K, DataContainer<()>> for BPlus<K> {
    fn insert(&mut self, key: K, value: DataContainer<()>) -> io::Result<()> {
        match value.extract() {
            Data::Chunk(chunk) => self.insert(key, chunk.clone()),
            Data::TargetChunk(_chunk) => unimplemented!(),
        }
    }

    fn get(&self, key: &K) -> io::Result<DataContainer<()>> {
        self.root.get(key).map(DataContainer::from)
    }

    fn contains(&self, key: &K) -> bool {
        self.root.get(key).is_ok()
    }
}

/// Iterator for B+ tree
pub struct BPlusIterator<K> {
    current_leaf: Option<Rc<RefCell<Node<K>>>>,
    current_index: usize,
}

impl<K: Ord + Clone + Debug> Iterator for BPlusIterator<K> {
    type Item = (K, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let current = self.current_leaf.as_ref()?.borrow().clone();
            if let Node::Leaf(leaf) = current {
                if self.current_index < leaf.entries.len() {
                    let (key, handler) = &leaf.entries[self.current_index];
                    let value = handler.read().ok()?;
                    let key = Rc::unwrap_or_clone(key.clone());
                    self.current_index += 1;
                    return Some((key, value));
                } else {
                    match &leaf.next {
                        Some(next_leaf) => {
                            self.current_leaf = Some(Rc::clone(next_leaf));
                            self.current_index = 0;
                        }
                        None => {
                            println!("erm");
                            self.current_leaf = None;
                            return None;
                        }
                    }
                }
            } else {
                return None;
            }
        }
    }
}

impl<K: Ord + Clone + Default + Debug> IntoIterator for BPlus<K> {
    type Item = (K, Vec<u8>);
    type IntoIter = BPlusIterator<K>;

    fn into_iter(self) -> Self::IntoIter {
        let mut current = Some(Rc::new(RefCell::new(self.root)));
        while let Some(node) = current.clone() {
            let borrowed = node.borrow();
            match &*borrowed {
                Node::Internal(internal) => {
                    current = Some(Rc::clone(&internal.children[0]));
                }
                Node::Leaf(_) => break,
            }
        }

        BPlusIterator {
            current_leaf: current,
            current_index: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use rand::seq::SliceRandom;
    use tempdir::TempDir;

    use super::*;

    #[test]
    fn test_non_existent_key() {
        let tempdir = TempDir::new("non_existent").unwrap();
        let mut tree: BPlus<usize> = BPlus::new(2, tempdir.path().into()).unwrap();
        tree.insert(1, vec![1]).unwrap();
        assert!(tree.get(&2).is_err());
    }

    #[test]
    fn test_node_split_leaf() {
        let tempdir = TempDir::new("split_leaf").unwrap();
        let mut tree: BPlus<usize> = BPlus::new(2, tempdir.path().into()).unwrap();

        for i in 1..5 {
            tree.insert(i, vec![i as u8]).unwrap();
        }

        if let Node::Internal(root) = &tree.root {
            assert_eq!(root.keys.len(), 1);
            assert_eq!(root.children.len(), 2);
        } else {
            assert!(false);
        }
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
    fn test_file_rotation() {
        let tempdir = TempDir::new("file_rotation").unwrap();
        let mut tree = BPlus::new(2, tempdir.path().into()).unwrap();
        tree.max_file_size = 128;

        let data = vec![0; 200];
        tree.insert(1, data).unwrap();

        let small_data = vec![0; 10];
        tree.insert(2, small_data).unwrap();

        assert_eq!(tree.file_number, 1);
        assert_eq!(tree.offset, 10);
    }

    #[test]
    fn test_leaf_linking() {
        let tempdir = TempDir::new("leaf_link").unwrap();
        let mut tree: BPlus<usize> = BPlus::new(2, tempdir.path().into()).unwrap();

        for i in 1..=9 {
            tree.insert(i, vec![i as u8]).unwrap();
        }

        let mut current = Some(Rc::new(RefCell::new(tree.root)));
        while let Some(node) = current.clone() {
            let borrowed = node.borrow();
            match &*borrowed {
                Node::Internal(internal) => {
                    current = Some(Rc::clone(&internal.children[0]));
                }
                Node::Leaf(_) => break,
            }
        }

        let mut collected = vec![];
        if let Node::Leaf(first_leaf) = &*current.clone().as_ref().unwrap().borrow() {
            let mut current_leaf = Some(Rc::new(RefCell::new(Node::Leaf(first_leaf.clone()))));

            while let Some(leaf_ref) = current_leaf {
                let leaf_guard = leaf_ref.borrow();

                if let Node::Leaf(leaf) = &*leaf_guard {
                    collected.extend(leaf.entries.iter().map(|(k, _)| **k));
                    current_leaf = leaf.next.as_ref().map(|next| Rc::clone(next));
                } else {
                    break;
                }
            }
        }

        assert_eq!(collected, (1..=9).collect::<Vec<_>>());
        assert_eq!(collected.len(), 9);
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
    fn test_concurrent_file_handling() {
        let tempdir = TempDir::new("concurrent").unwrap();
        let mut tree = BPlus::new(2, tempdir.path().into()).unwrap();
        tree.max_file_size = 256;

        for i in 0..500 {
            tree.insert(i, vec![i as u8; 100]).unwrap();
        }

        assert!(tree.file_number > 1);
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
    fn test_node_consistency_after_splits() {
        let tempdir = TempDir::new("consistency").unwrap();
        let mut tree = BPlus::new(2, tempdir.path().into()).unwrap();

        for i in 0..10 {
            tree.insert(i, vec![i as u8]).unwrap();
        }

        if let Node::Internal(root) = &tree.root {
            assert!(root.keys.len() >= 1);
            assert!(root.children.len() >= 2);

            for key in &root.keys {
                let key_val = **key;
                assert!(key_val > 0 && key_val < 10);
            }
        }
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
}
