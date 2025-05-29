use std::{
    collections::{HashSet, VecDeque},
    fmt::Debug,
    fs::{create_dir_all, File},
    io::{self, ErrorKind},
    mem,
    os::unix::fs::FileExt,
    path::PathBuf,
    rc::Rc,
    sync::{
        atomic::{AtomicU64, AtomicUsize},
        Arc,
    },
    thread, time,
};

use chunkfs::{Data, DataContainer, Database};
use tokio::{
    self,
    runtime::Runtime,
    sync::{Mutex, RwLock},
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
type Link<K> = Arc<RwLock<Node<K>>>;

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
    keys: Vec<Arc<K>>,
}

/// Leaf node in a B+ tree
#[derive(Default, Clone)]
struct Leaf<K> {
    /// Data entries that stored in that leaf.
    entries: Vec<(Arc<K>, ChunkHandler)>,
    /// Link to the next leaf; None if there are none.
    next: Option<Link<K>>,
}

/// B+ tree
pub struct BPlus<K> {
    /// Root of the B+ tree.
    root: Link<K>,
    /// Parameter, that represents minimal and maximal amount of node keys.
    t: usize,
    /// Path to the directory, in which all data will be writen.
    path: PathBuf,
    /// Number of current file.
    file_number: AtomicUsize,
    /// Current offset in current file.
    offset: AtomicU64,
    /// Current file.
    current_file: Arc<RwLock<File>>,
    /// Max file size.
    max_file_size: u64,
    // Latch for root
    latch: RwLock<()>,
}

/// Wrapper for BPlusTree with sync functions with async runtime
pub struct BPlusStorage {
    /// BPlusTree
    tree: Arc<BPlus<Vec<u8>>>,
    /// Async tokio runtime for operations
    runtime: Runtime,
    /// Currently inserting keys
    keys_set: Arc<Mutex<HashSet<Vec<u8>>>>,
}

impl BPlusStorage {
    /// Creates new instance of B+ tree with given runtime, t and path
    /// runtime is tokio runtime
    /// t represents minimal and maximum quantity of keys in the node
    /// All data will be written in directory by given path
    pub fn new(runtime: Runtime, t: usize, path: PathBuf) -> io::Result<Self> {
        let tree = BPlus::new(t, path).unwrap();
        Ok(Self {
            tree: Arc::new(tree),
            runtime,
            keys_set: Arc::new(Mutex::new(HashSet::new())),
        })
    }
}

impl Database<Vec<u8>, DataContainer<()>> for BPlusStorage {
    /// Inserts given value by given key in the B+ tree
    fn insert(&mut self, key: Vec<u8>, value: DataContainer<()>) -> io::Result<()> {
        let tree = self.tree.clone();

        let value = match value.extract() {
            Data::Chunk(chunk) => chunk.clone(),
            Data::TargetChunk(_chunk) => unimplemented!(),
        };

        let set_clone = self.keys_set.clone();

        self.runtime.spawn(async move {
            set_clone.lock().await.insert(key.clone());
            tree.insert(key.clone(), value).await.unwrap();
            set_clone.lock().await.remove(&key);
        });
        Ok(())
    }

    /// Gets value by given key from B+ tree
    fn get(&self, key: &Vec<u8>) -> io::Result<DataContainer<()>> {
        let tree = self.tree.clone();
        let set_clone = self.keys_set.clone();
        Ok(self
            .runtime
            .block_on(async move {
                while set_clone.lock().await.contains(key) {
                    thread::sleep(time::Duration::from_millis(10));
                }
                tree.get(key).await.unwrap()
            })
            .into())
    }

    /// Returns whether key is contained in the B+ tree or not
    fn contains(&self, key: &Vec<u8>) -> bool {
        self.get(key).is_ok()
    }
}

#[allow(dead_code)]
impl<K: Default + Ord + Clone + Debug + Sized> BPlus<K> {
    /// Creates new instance of B+ tree with given t and path
    /// t represents minimal and maximal quantity of keys in node
    /// All data will be written in files in directory by given path
    pub fn new(t: usize, path: PathBuf) -> io::Result<Self> {
        let path_to_file = path.join("0");
        create_dir_all(&path)?;
        let current_file = File::create(path_to_file)?;

        Ok(Self {
            root: Arc::new(RwLock::new(Node::Leaf(Leaf::default()))),
            t,
            path,
            file_number: 0.into(),
            offset: 0.into(),
            current_file: Arc::new(RwLock::new(current_file)),
            max_file_size: DEFAULT_MAX_FILE_SIZE,
            latch: RwLock::new(()),
        })
    }

    /// Creates new chunk_handler and writes data to a file
    async fn get_chunk_handler(&self, value: Vec<u8>) -> io::Result<ChunkHandler> {
        let mut file_guard = self.current_file.write().await;
        if self.offset.load(std::sync::atomic::Ordering::SeqCst) >= self.max_file_size {
            self.file_number
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.offset.store(0, std::sync::atomic::Ordering::SeqCst);
            *file_guard = File::create(
                self.path.join(
                    self.file_number
                        .load(std::sync::atomic::Ordering::SeqCst)
                        .to_string(),
                ),
            )
            .unwrap();
        }

        let value_size = value.len();
        file_guard.write_at(
            &value,
            self.offset.load(std::sync::atomic::Ordering::SeqCst),
        )?;
        let value_to_insert = ChunkHandler::new(
            self.path.join(
                self.file_number
                    .load(std::sync::atomic::Ordering::SeqCst)
                    .to_string(),
            ),
            self.offset.load(std::sync::atomic::Ordering::SeqCst),
            value.len(),
        );
        self.offset
            .fetch_add(value_size as u64, std::sync::atomic::Ordering::SeqCst);
        Ok(value_to_insert)
    }

    /// Inserts given value by given key in the B+ tree
    ///
    /// Returns Err(_) if file could not be created
    pub async fn insert(&self, key: K, value: Vec<u8>) -> io::Result<()> {
        let key = Arc::new(key);
        let value = self.get_chunk_handler(value).await.unwrap();
        let mut path = Vec::new(); // Path to leaf
        let mut latch_guard = Some(self.latch.write());
        let mut current = self.root.clone();
        let mut split_result;
        let mut guards = VecDeque::new();

        // Descent to the leaf
        loop {
            let mut current_node = current.write_owned().await;
            if let Some(guard) = latch_guard {
                drop(guard);
                latch_guard = None;
            };
            match &mut *current_node {
                Node::Leaf(leaf) => {
                    match leaf.entries.binary_search_by(|(k, _)| k.cmp(&key)) {
                        Ok(pos) => leaf.entries[pos] = (key.clone(), value),
                        Err(pos) => leaf.entries.insert(pos, (key.clone(), value)),
                    };

                    println!("leaf.len = {}", leaf.entries.len());

                    split_result = if leaf.entries.len() == 2 * self.t {
                        Some(current_node.split(self.t))
                    } else {
                        None
                    };

                    // if path is empty, then current node is root
                    if path.is_empty() {
                        guards.push_back(current_node);
                    }

                    break;
                }
                Node::Internal(internal) => {
                    let pos = match internal.keys.binary_search(&key) {
                        Ok(pos) => pos + 1,
                        Err(pos) => pos,
                    };

                    // droping guards if nodes are not going to be changed
                    if internal.keys.len() != 2 * self.t - 2 {
                        while guards.len() > 1 {
                            drop(guards.pop_front().unwrap());
                        }
                    }

                    let next_node = internal.children[pos].clone();

                    path.push(pos);

                    current = next_node;
                }
            }

            guards.push_back(current_node);
        }

        // Going up to the root splitting nodes if needed
        while let Some(pos) = path.pop() {
            if let Some((new_node, median)) = split_result.take() {
                let mut node = guards.pop_back().unwrap();
                if let Node::Internal(internal) = &mut *node {
                    internal.keys.insert(pos, median.clone());
                    internal.children.insert(pos + 1, new_node);
                    if internal.keys.len() == 2 * self.t - 1 {
                        split_result = Some(node.split(self.t));
                    } else {
                        split_result = None;
                    }
                }
                if path.is_empty() {
                    guards.push_back(node);
                } else {
                    drop(node);
                }
            }
        }

        // splitting root if needed
        if let Some((new_node, median)) = split_result.take() {
            // if path is empty, then current node is root
            if path.is_empty() {
                if let Some(mut node) = guards.pop_back() {
                    match &mut *node {
                        Node::Internal(internal) => {
                            let mut old_root_children = Vec::new();
                            let mut old_root_keys = Vec::new();
                            mem::swap(&mut old_root_keys, &mut internal.keys);
                            mem::swap(&mut old_root_children, &mut internal.children);
                            let old_root = Node::<K>::Internal(InternalNode {
                                children: (old_root_children),
                                keys: (old_root_keys),
                            });
                            internal.children.push(Arc::new(RwLock::new(old_root)));
                            internal.children.push(new_node);
                            internal.keys.push(median.clone());
                        }
                        Node::Leaf(leaf) => {
                            let mut old_root_entries = Vec::new();
                            let old_root_next = leaf.next.clone();
                            mem::swap(&mut old_root_entries, &mut leaf.entries);
                            let old_root = Node::<K>::Leaf(Leaf {
                                entries: old_root_entries,
                                next: old_root_next,
                            });
                            let new_root = Node::<K>::Internal(InternalNode {
                                children: (vec![Arc::new(RwLock::new(old_root)), new_node]),
                                keys: (vec![median.clone()]),
                            });
                            *node = new_root;
                        }
                    }
                }
            }
        }

        Ok(())
    }
    #[allow(unused_variables)]
    fn remove(&mut self, key: Rc<K>) -> io::Result<()> {
        unimplemented!()
    }

    /// Gets value from a B+ tree by given key
    pub async fn get(&self, key: &K) -> io::Result<Vec<u8>> {
        let mut latch_guard = Some(self.latch.read());
        let mut current = self.root.clone();

        let mut prev_guard = None;
        loop {
            let node = current.read_owned().await;
            if let Some(guard) = latch_guard {
                drop(guard);
                latch_guard = None;
            }
            if prev_guard.is_some() {
                drop(prev_guard);
            }
            match &*node {
                Node::Leaf(leaf) => {
                    return match leaf.entries.binary_search_by(|(k, _)| k.as_ref().cmp(key)) {
                        Ok(pos) => {
                            let data_read_result = leaf.entries[pos].1.read()?;
                            drop(node);
                            Ok(data_read_result)
                        }
                        Err(_) => {
                            drop(node);
                            Err(ErrorKind::NotFound.into())
                        }
                    };
                }
                Node::Internal(internal) => {
                    let pos = match internal.keys.binary_search_by(|k| k.as_ref().cmp(key)) {
                        Ok(pos) => pos + 1,
                        Err(pos) => pos,
                    };

                    current = match internal.children.get(pos) {
                        Some(child) => child.clone(),
                        None => {
                            drop(node);
                            return Err(ErrorKind::NotFound.into());
                        }
                    };
                }
            }
            prev_guard = Some(node);
        }
    }
}

impl<K: Clone + Ord + Debug> Node<K> {
    /// Splits node into two and returns new node with it first key
    fn split(&mut self, t: usize) -> (Link<K>, Arc<K>) {
        match self {
            Node::Leaf(leaf) => {
                let mut new_leaf_entries = leaf.entries.split_off(t);
                new_leaf_entries.reserve_exact(t);
                let middle_key = new_leaf_entries[0].0.clone();

                let new_leaf = Node::Leaf(Leaf {
                    entries: new_leaf_entries,
                    next: leaf.next.take(),
                });

                let new_leaf_link = Arc::new(RwLock::new(new_leaf));
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

                (Arc::new(RwLock::new(new_node)), middle_key)
            }
        }
    }

    #[allow(unused_variables, dead_code)]
    fn remove(&mut self, key: &K, t: usize) -> io::Result<()> {
        unimplemented!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use tokio::test;

    fn create_test_tree(t: usize, name: &str) -> (BPlus<i32>, TempDir) {
        let temp_dir = TempDir::with_prefix(name).unwrap();
        let tree = BPlus::new(t, temp_dir.path().to_path_buf()).unwrap();
        (tree, temp_dir)
    }

    #[test]
    async fn test_multiple_inserts() {
        let (tree, _temp) = create_test_tree(2, "multiple_inserts");

        for i in 1..=4 {
            tree.insert(i, vec![i as u8]).await.unwrap(); // let _ = handle.spawn(async move {tree.insert(i, vec![i as u8]).await}).await.unwrap();
        }

        for i in 1..=4 {
            let result = tree.get(&i).await.unwrap();
            assert_eq!(result, vec![i as u8]);
        }
    }

    #[test]
    async fn test_concurrent_inserts() {
        let (tree, _temp) = create_test_tree(2, "concurrent_inserts");
        let tree = Arc::new(tokio::sync::RwLock::new(tree));

        let mut handles = vec![];
        for i in 0..50 {
            let tree = tree.clone();
            handles.push(tokio::spawn(async move {
                let tree = tree.write().await;
                tree.insert(i, vec![i as u8]).await.unwrap();
            }));
        }

        for handle in handles {
            handle.await.unwrap();
        }

        let tree = tree.read().await;
        for i in 0..50 {
            let result = tree.get(&i).await.unwrap();
            assert_eq!(result, vec![i as u8]);
        }
    }

    #[test]
    async fn test_root_split() {
        let (tree, _temp) = create_test_tree(2, "root_split");

        tree.insert(1, vec![1]).await.unwrap();
        tree.insert(2, vec![2]).await.unwrap();
        tree.insert(3, vec![3]).await.unwrap();
        tree.insert(4, vec![4]).await.unwrap();

        let root = tree.root.read().await;
        match &*root {
            Node::Internal(internal) => {
                assert_eq!(internal.keys.len(), 1);
                assert_eq!(internal.children.len(), 2);
            }
            _ => panic!("Root should be internal node after split"),
        }
    }

    #[test]
    async fn test_large_value_storage() {
        let temp_dir = TempDir::new().unwrap();
        let mut tree = BPlus::new(2, temp_dir.path().to_path_buf()).unwrap();
        tree.max_file_size = 100;

        let large_data = vec![7; 150];
        tree.insert(1, large_data.clone()).await.unwrap();

        let result = tree.get(&1).await.unwrap();
        assert_eq!(result, large_data);
        tree.insert(2, large_data.clone()).await.unwrap();
        let result = tree.get(&1).await.unwrap();
        assert_eq!(result, large_data);

        assert!(
            tree.file_number.load(std::sync::atomic::Ordering::SeqCst) >= 1,
            "Should create multiple files"
        );
    }
}
