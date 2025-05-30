use chunkfs::{Data, DataContainer, Database};

use std::{
    cell::RefCell,
    fmt::{self, Debug},
    fs::{create_dir_all, File},
    io::{self, ErrorKind},
    os::unix::fs::FileExt,
    path::PathBuf,
    rc::Rc,
};

const DEFAULT_MAX_FILE_SIZE: u64 = 2 << 20;

extern crate chunkfs;

/// Structure that handles chunks written in files.
#[derive(Clone, Default, Debug)]
pub struct ChunkHandler {
    /// Path to file with chunk.
    path: PathBuf,
    /// Offset in file with chunk.
    offset: u64,
    /// Size of chunk.
    size: usize,
}

impl ChunkHandler {
    /// Creates new ChunkHandler, that points to the chunk, that stored in file by path
    fn new(path: PathBuf, offset: u64, size: usize) -> Self {
        ChunkHandler { path, offset, size }
    }

    /// Reads data pointed by ChunkHandler.
    ///
    /// Returns Err(_) if there is error in opening the file or reading the chunk.
    fn read(&self) -> io::Result<Vec<u8>> {
        let file = File::open(self.path.clone())?;
        let mut buf = vec![0; self.size];
        file.read_exact_at(&mut buf, self.offset)?;
        Ok(buf)
    }
}

/// A type that represents a reference to another node.
type Link<K> = Rc<RefCell<Node<K>>>;

/// Represents a node in a B+ tree.
/// All data resides in leaf nodes, while internal nodes.
/// manage navigation between children.
#[derive(Clone)]
enum Node<K> {
    Internal(InternalNode<K>),
    Leaf(Leaf<K>),
}

/// Internal node in a B+ tree
#[derive(Clone)]
struct InternalNode<K> {
    /// Children of that node.
    children: Vec<Link<K>>,
    /// Keys of that node.
    keys: Vec<Rc<K>>,
}

/// Leaf node in a B+ tree
#[derive(Default, Clone)]
struct Leaf<K> {
    /// Data entries that stored in that leaf.
    entries: Vec<(Rc<K>, ChunkHandler)>,
    /// Link to the next leaf; None if there are none.
    next: Option<Link<K>>,
}

/// B+ tree
pub struct BPlus<K> {
    /// Root of the B+ tree.
    root: Node<K>,
    /// Parameter, that represents minimal and maximal amount of node keys.
    t: usize,
    /// Path to the directory, in which all data will be writen.
    path: PathBuf,
    /// Number of current file.
    file_number: usize,
    /// Current offset in current file.
    offset: u64,
    /// Current file.
    current_file: File,
    /// Max file size.
    max_file_size: u64,
}

impl<K: Ord + fmt::Debug> fmt::Display for BPlus<K> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        BPlus::fmt_node(&self.root, 0, f)
    }
}

impl<K: Ord + fmt::Debug> BPlus<K> {
    fn fmt_node(node: &Node<K>, level: usize, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match node {
            Node::Internal(internal) => {
                writeln!(
                    f,
                    "{}[Internal] keys: {:?}",
                    "  ".repeat(level),
                    internal.keys
                )?;

                for child in &internal.children {
                    BPlus::fmt_node(&child.borrow(), level + 1, f)?;
                }
                Ok(())
            }
            Node::Leaf(leaf) => {
                writeln!(
                    f,
                    "{}[Leaf] entries: {:?}",
                    "  ".repeat(level),
                    leaf.entries
                )
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
        create_dir_all(&path)?;
        let current_file = File::create(path_to_file)?;
        Ok(Self {
            root: Node::Leaf(Leaf::default()),
            t,
            path,
            file_number: 0,
            offset: 0,
            current_file,
            max_file_size: DEFAULT_MAX_FILE_SIZE,
        })
    }

    /// Creates new chunk_handler and writes data to a file
    fn get_chunk_handler(&mut self, value: Vec<u8>) -> io::Result<ChunkHandler> {
        if self.offset >= self.max_file_size {
            self.file_number += 1;
            self.offset = 0;
            self.current_file = File::create(self.path.join(self.file_number.to_string())).unwrap();
        }

        let value_size = value.len();
        self.current_file.write_at(&value, self.offset)?;
        let value_to_insert = ChunkHandler::new(
            self.path.join(self.file_number.to_string()),
            self.offset,
            value.len(),
        );
        self.offset += value_size as u64;
        Ok(value_to_insert)
    }

    /// Inserts given value by given key in the B+ tree
    ///
    /// Returns Err(_) if file could not be created
    pub fn insert(&mut self, key: K, value: Vec<u8>) -> io::Result<()> {
        let value_to_insert = self.get_chunk_handler(value).unwrap();

        let key = Rc::new(key);

        if let Some((new_node_link, new_key)) = self.root.insert(key, value_to_insert, self.t) {
            let mut prev_root = self.root.clone();
            if let Node::Leaf(mut leaf) = prev_root {
                leaf.next = Some(new_node_link.clone());
                prev_root = Node::Leaf(leaf);
            }
            let mut new_root_children = Vec::with_capacity(2 * self.t);
            let mut new_root_keys = Vec::with_capacity(2 * self.t - 1);
            new_root_children.push(Rc::new(RefCell::new(prev_root)));
            new_root_children.push(new_node_link);
            new_root_keys.push(new_key);
            let new_root = Node::<K>::Internal(InternalNode {
                children: new_root_children,
                keys: new_root_keys,
            });

            self.root = new_root;
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
                let mut new_leaf_entries = leaf.entries.split_off(t);
                new_leaf_entries.reserve_exact(t);
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

                let mut new_node_children = internal_node.children.split_off(t);
                new_node_keys.reserve_exact(t);
                new_node_children.reserve_exact(t);

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
                Ok(pos) => {
                    let data_read_result = leaf.entries[pos].1.read()?;
                    Ok(data_read_result)
                }
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
    use tempdir::TempDir;

    use super::*;

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
                    current_leaf = leaf.next.as_ref().map(Rc::clone);
                } else {
                    break;
                }
            }
        }

        assert_eq!(collected, (1..=9).collect::<Vec<_>>());
        assert_eq!(collected.len(), 9);
    }

    #[test]
    fn test_consecutive_file_handling() {
        let tempdir = TempDir::new("consecutive").unwrap();
        let mut tree = BPlus::new(2, tempdir.path().into()).unwrap();
        tree.max_file_size = 256;

        for i in 0..500 {
            tree.insert(i, vec![i as u8; 100]).unwrap();
        }

        assert!(tree.file_number > 1);
    }

    #[test]
    fn test_node_consistency_after_splits() {
        let tempdir = TempDir::new("consistency").unwrap();
        let mut tree = BPlus::new(2, tempdir.path().into()).unwrap();

        for i in 0..10 {
            tree.insert(i, vec![i as u8]).unwrap();
        }

        if let Node::Internal(root) = &tree.root {
            assert!(!root.keys.is_empty());
            assert!(root.children.len() >= 2);

            for key in &root.keys {
                let key_val = **key;
                assert!(key_val > 0 && key_val < 10);
            }
        }
    }
}
