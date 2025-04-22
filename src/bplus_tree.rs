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

        if let Some((new_node, new_key)) = self.root.insert(key, value_to_insert, self.t) {
            {
                let new_root = Node::<K>::Internal(InternalNode {
                    children: vec![
                        Rc::new(RefCell::new(self.root.clone())),
                        Rc::new(RefCell::new(new_node)),
                    ],
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
    fn split(&mut self, t: usize) -> (Node<K>, Rc<K>) {
        match self {
            Node::Leaf(leaf) => {
                let new_leaf_entries = leaf.entries.split_off(t);
                let middle_key = new_leaf_entries[0].0.clone();

                let new_leaf = Node::Leaf(Leaf {
                    entries: new_leaf_entries,
                    next: leaf.next.take(),
                });

                leaf.next = Some(Rc::new(RefCell::new(new_leaf.clone())));

                (new_leaf, middle_key)
            }
            Node::Internal(internal_node) => {
                let mut new_node_keys = internal_node.keys.split_off(t - 1);
                let middle_key = new_node_keys.remove(0);

                let new_node_children = internal_node.children.split_off(t);

                let new_node = Node::Internal(InternalNode {
                    children: new_node_children,
                    keys: new_node_keys,
                });

                (new_node, middle_key)
            }
        }
    }

    /// Inserts given value by given key
    fn insert(&mut self, key: Rc<K>, value: ChunkHandler, t: usize) -> Option<(Node<K>, Rc<K>)> {
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
                        internal_node
                            .children
                            .insert(pos + 1, Rc::new(RefCell::new(new_child)));

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

        for i in 1..10 {
            tree.insert(i, vec![i as u8]).unwrap();
        }

        if let Node::Leaf(first_leaf) = &tree.root {
            let mut current = Some(Rc::clone(first_leaf.next.as_ref().unwrap()));
            let mut collected = vec![];

            while let Some(leaf) = current.clone() {
                let borrowed = leaf.borrow();
                if let Node::Leaf(leaf) = &*borrowed {
                    collected.extend(leaf.entries.iter().map(|(k, _)| **k));
                    current = leaf.next.as_ref().map(Rc::clone);
                }
            }

            assert_eq!(collected, (5..10).collect::<Vec<_>>());
        }
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
}
