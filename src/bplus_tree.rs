use std::{
    collections::{HashSet, VecDeque},
    fmt::Debug,
    fs::{create_dir_all, File},
    io::{self, BufReader, BufWriter, ErrorKind},
    mem,
    os::unix::fs::FileExt,
    path::{Path, PathBuf},
    rc::Rc,
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    thread, time,
};

use async_recursion::async_recursion;

use serde::{Deserialize, Serialize};

use chunkfs::{Data, DataContainer, Database};
use tokio::{self, runtime::Runtime, sync::RwLock};

const DEFAULT_MAX_FILE_SIZE: u64 = 2 << 20;

pub trait BPlusKey: Default + Ord + Clone + Sized + Sync + Send {}
impl<T: Default + Ord + Clone + Sized + Sync + Send> BPlusKey for T {}

pub trait BPlusKeySerializable: BPlusKey + Serialize + for<'de> Deserialize<'de> {}
impl<T: Default + Ord + Clone + Sized + Sync + Send + Serialize + for<'de> Deserialize<'de>>
    BPlusKeySerializable for T
{
}

extern crate chunkfs;

/// Serializable version of BPlusTree
#[derive(Serialize, Deserialize)]
struct SerializableBPlus<K> {
    t: usize,
    path: PathBuf,
    file_number: usize,
    offset: u64,
    max_file_size: u64,
    root: SerializableNode<K>,
}

/// Easily serializable version of BPlusTree Node
#[derive(Serialize, Deserialize)]
enum SerializableNode<K> {
    Internal(SerializableInternalNode<K>),
    Leaf(SerializableLeaf<K>),
}

#[derive(Serialize, Deserialize)]
struct SerializableInternalNode<K> {
    keys: Vec<K>,
    children: Vec<SerializableNode<K>>,
}

#[derive(Serialize, Deserialize)]
struct SerializableLeaf<K> {
    entries: Vec<(K, ChunkHandler)>,
}

impl<K: Clone + Send + Sync> BPlus<K> {
    /// Returns new instance of SerializableBPlus with data from provided BPlus
    async fn serialize(&self) -> SerializableBPlus<K> {
        SerializableBPlus {
            t: self.t,
            path: self.path.clone(),
            file_number: self.file_number.load(Ordering::SeqCst),
            offset: self.offset.load(Ordering::SeqCst),
            max_file_size: self.max_file_size,
            root: self.root.read().await.serialize().await,
        }
    }
}

impl<K: Clone + Send + Sync> Node<K> {
    #[async_recursion]
    /// Returns new instance of SerializableNode with data from provided Node
    async fn serialize(&self) -> SerializableNode<K> {
        match self {
            Node::Internal(internal) => {
                let keys = internal.keys.iter().map(|k| (**k).clone()).collect();

                let children_clone = internal.children.clone();
                let mut children = Vec::new();
                for child in children_clone {
                    children.push(child.read().await.serialize().await);
                }

                SerializableNode::Internal(SerializableInternalNode { keys, children })
            }
            Node::Leaf(leaf) => SerializableNode::Leaf(SerializableLeaf {
                entries: leaf
                    .entries
                    .iter()
                    .map(|(k, v)| ((**k).clone(), v.clone()))
                    .collect(),
            }),
        }
    }
}

impl<K: BPlusKeySerializable> SerializableBPlus<K> {
    /// Returns new instance of BPlus with data from provided BPlusSerializable
    async fn deserialize(self) -> BPlus<K> {
        let root = Arc::new(RwLock::new(Node::from(self.root)));

        let tree = BPlus {
            root: root.clone(),
            t: self.t,
            path: self.path.clone(),
            file_number: AtomicUsize::new(self.file_number),
            offset: AtomicU64::new(self.offset),
            current_file: BPlus::<K>::open_current_file(&self.path, self.file_number).unwrap(),
            max_file_size: self.max_file_size,
            latch: RwLock::new(()),
        };

        tree.rebuild_links().await;
        tree
    }
}

impl<K> From<SerializableNode<K>> for Node<K> {
    fn from(node: SerializableNode<K>) -> Self {
        match node {
            SerializableNode::Internal(internal) => Node::Internal(InternalNode {
                keys: internal.keys.into_iter().map(Arc::new).collect(),
                children: internal
                    .children
                    .into_iter()
                    .map(|c| Arc::new(RwLock::new(Node::from(c))))
                    .collect(),
            }),
            SerializableNode::Leaf(leaf) => Node::Leaf(Leaf {
                entries: leaf
                    .entries
                    .into_iter()
                    .map(|(k, v)| (Arc::new(k), v))
                    .collect(),
                next: None,
            }),
        }
    }
}

/// Structure that handles chunks written in files.
#[derive(Clone, Default, Debug, Serialize, Deserialize)]
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
pub struct BPlusStorage<K> {
    /// BPlusTree
    tree: Arc<BPlus<K>>,
    /// Async tokio runtime for operations
    runtime: Runtime,
    /// Currently inserting keys
    keys_set: Arc<Mutex<HashSet<K>>>,
}

impl<K: BPlusKey> BPlusStorage<K> {
    /// Creates new instance of B+ tree with given runtime, t and path
    ///
    /// runtime is tokio runtime
    ///
    /// t represents minimal and maximum quantity of keys in the node
    ///
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

impl<K: std::hash::Hash + 'static + BPlusKey> Database<K, DataContainer<()>> for BPlusStorage<K> {
    /// Inserts given value by given key in the B+ tree
    fn insert(&mut self, key: K, value: DataContainer<()>) -> io::Result<()> {
        let tree = self.tree.clone();

        let value = match value.extract() {
            Data::Chunk(chunk) => chunk.clone(),
            Data::TargetChunk(_chunk) => unimplemented!(),
        };

        let set_clone = self.keys_set.clone();
        set_clone.lock().unwrap().insert(key.clone());

        self.runtime.spawn(async move {
            tree.insert(key.clone(), value).await;
            set_clone.lock().unwrap().remove(&key);
        });
        Ok(())
    }

    /// Gets value by given key from B+ tree
    fn get(&self, key: &K) -> io::Result<DataContainer<()>> {
        let tree = self.tree.clone();
        let set_clone = self.keys_set.clone();

        Ok(self
            .runtime
            .block_on(async move {
                while set_clone.lock().unwrap().contains(key) {
                    thread::sleep(time::Duration::from_millis(10));
                }
                tree.get(key).await.unwrap()
            })
            .into())
    }

    /// Returns whether key is contained in the B+ tree or not
    fn contains(&self, key: &K) -> bool {
        self.get(key).is_ok()
    }
}

#[allow(dead_code)]
impl<K: BPlusKey> BPlus<K> {
    /// Creates new instance of B+ tree with given t and path
    ///
    /// t represents minimal and maximal quantity of keys in node
    ///
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
            let file_number = self.file_number.load(Ordering::SeqCst).to_string();
            let file_path = self.path.join(file_number);

            *file_guard = File::create(file_path).unwrap();
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
    pub async fn insert(&self, key: K, value: Vec<u8>) {
        let value = self.get_chunk_handler(value).await.unwrap();
        let mut path = Vec::new(); // Path to leaf
                                   // Insert that implies that target leaf is safe. Otherwise returns Err()
        if self
            .optimistic_insert(key.clone(), value.clone())
            .await
            .is_ok()
        {
            return;
        }
        let mut latch_guard = Some(self.latch.write());
        let key = Arc::new(key);
        let mut current = self.root.clone();
        let mut split_result;
        let mut guards = VecDeque::new();

        // Descent to the leaf
        loop {
            let mut current_node = current.write_owned().await;
            if let Some(guard) = latch_guard.take() {
                drop(guard);
                latch_guard = None;
            };
            match &mut *current_node {
                Node::Leaf(leaf) => {
                    match leaf.entries.binary_search_by(|(k, _)| k.cmp(&key)) {
                        Ok(pos) => leaf.entries[pos] = (key.clone(), value),
                        Err(pos) => leaf.entries.insert(pos, (key.clone(), value)),
                    };

                    split_result = if leaf.entries.len() == 2 * self.t {
                        Some(current_node.split(self.t))
                    } else {
                        while !guards.is_empty() {
                            drop(guards.pop_front().unwrap());
                        }
                        None
                    };

                    // if path is empty, then current node is root
                    if path.is_empty() {
                        guards.push_back(current_node);
                    } else {
                        drop(current_node);
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
                        while !guards.is_empty() {
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
                    drop(node);
                }
            }
        }

        for guard in guards {
            drop(guard);
        }
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

    /// For optimistic latch crabbing
    ///
    /// Insert firstly implies that leaf is safe
    ///
    /// If it is safe, than inserts(without write locks on other nodes) to the leaf and returns Ok
    ///
    /// Else, returns Err
    ///
    /// Also returns Err if root is leaf
    async fn optimistic_insert(&self, key: K, value: ChunkHandler) -> Result<(), ()> {
        let mut latch_guard = Some(self.latch.read());
        let mut current = self.root.clone();
        let key = Arc::new(key);

        let mut prev_guard = None;
        let mut last_child_index = None;

        loop {
            let node = current.read_owned().await;

            if let Some(guard) = latch_guard.take() {
                drop(guard);
                if matches!(&*node, Node::Leaf(_)) {
                    return Err(());
                }
            }

            if matches!(&*node, Node::Leaf(_)) {
                break;
            }

            prev_guard = Some(node);

            if let Node::Internal(internal) = prev_guard.as_deref().unwrap() {
                let pos = match internal.keys.binary_search(&key) {
                    Ok(pos) => pos + 1,
                    Err(pos) => pos,
                };
                last_child_index = Some(pos);
                current = internal.children[pos].clone();
            } else {
                unreachable!();
            }
        }

        let prev_guard = prev_guard.unwrap();
        let prev_node = prev_guard.clone();
        let leaf_lock = {
            let pos = last_child_index.unwrap();
            if let Node::Internal(internal) = prev_node {
                internal.children[pos].clone()
            } else {
                unreachable!();
            }
        };

        let mut leaf = leaf_lock.write().await;
        drop(prev_guard);
        let Node::Leaf(leaf_node) = &mut *leaf else {
            unreachable!()
        };

        if leaf_node.entries.len() == 2 * self.t - 1 {
            return Err(());
        }

        match leaf_node.entries.binary_search_by(|(k, _)| k.cmp(&key)) {
            Ok(pos) => leaf_node.entries[pos].1 = value, // Обновляем без клонирования
            Err(pos) => leaf_node.entries.insert(pos, (key.clone(), value)),
        };
        Ok(())
    }
}

impl<K: BPlusKeySerializable> BPlus<K> {
    /// Rebuilds links in BPlusTree after loading from file
    async fn rebuild_links(&self) {
        let leaves = self.collect_leaves().await;
        if self.offset.load(Ordering::Acquire) == 0 && self.file_number.load(Ordering::Acquire) == 0
        {
            return;
        }

        let key_futures: Vec<_> = leaves
            .iter()
            .map(|leaf| {
                let leaf = Arc::clone(leaf);
                async move {
                    let guard = leaf.read().await;
                    match &*guard {
                        Node::Leaf(leaf_data) => leaf_data.entries[0].0.clone(),
                        _ => unreachable!(),
                    }
                }
            })
            .collect();

        let keys = futures::future::join_all(key_futures).await;

        let mut sorted_leaves: Vec<_> = keys.into_iter().zip(leaves.into_iter()).collect();

        sorted_leaves.sort_by(|(a, _), (b, _)| a.cmp(b));

        for i in 0..sorted_leaves.len() - 1 {
            let current = &sorted_leaves[i].1;
            let next = sorted_leaves[i + 1].1.clone();

            let mut guard = current.write().await;
            if let Node::Leaf(leaf) = &mut *guard {
                leaf.next = Some(next);
            }
        }
    }

    /// Collects all leaves from BPlusTree
    async fn collect_leaves(&self) -> Vec<Arc<RwLock<Node<K>>>> {
        let mut leaves = Vec::new();
        let mut queue = VecDeque::new();
        queue.push_back(self.root.clone());

        while let Some(node) = queue.pop_front() {
            let guard = node.read().await;
            match &*guard {
                Node::Internal(internal) => {
                    for child in &internal.children {
                        queue.push_back(child.clone());
                    }
                }
                Node::Leaf(_) => {
                    leaves.push(node.clone());
                }
            }
        }

        leaves
    }

    fn open_current_file(path: &Path, number: usize) -> io::Result<Arc<RwLock<File>>> {
        Ok(Arc::new(RwLock::new(
            File::open(path.join(number.to_string())).unwrap(),
        )))
    }

    /// Saves this tree by the provided path
    pub async fn save(&self, path: &Path) -> io::Result<()> {
        let _guard = self.latch.write().await;
        let serializable = self.serialize().await;
        let file = File::create(path)?;
        let writer = BufWriter::new(file);
        bincode::serialize_into(writer, &serializable).map_err(io::Error::other)
    }

    /// Loads tree from file by provided path
    pub async fn load(path: &Path) -> io::Result<Self> {
        let file = File::open(path)?;
        let reader = BufReader::new(file);
        let serializable: SerializableBPlus<K> =
            bincode::deserialize_from(reader).map_err(io::Error::other)?;

        Ok(serializable.deserialize().await)
    }
}

impl<K: Clone + Ord> Node<K> {
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

    fn create_test_tree(t: usize, name: &str) -> (BPlus<i32>, TempDir) {
        let temp_dir = TempDir::with_prefix(name).unwrap();
        let tree = BPlus::new(t, temp_dir.path().to_path_buf()).unwrap();
        (tree, temp_dir)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_multiple_inserts() {
        let (tree, _temp) = create_test_tree(2, "multiple_inserts");

        for i in 1..=4 {
            tree.insert(i, vec![i as u8]).await;
        }

        for i in 1..=4 {
            let result = tree.get(&i).await.unwrap();
            assert_eq!(result, vec![i as u8]);
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_concurrent_inserts() {
        let (tree, _temp) = create_test_tree(2, "concurrent_inserts");
        let tree = Arc::new(tokio::sync::RwLock::new(tree));

        let mut handles = vec![];
        for i in 0..50 {
            let tree = tree.clone();
            handles.push(tokio::spawn(async move {
                let tree = tree.write().await;
                tree.insert(i, vec![i as u8]).await;
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

    #[tokio::test(flavor = "multi_thread")]
    async fn test_root_split() {
        let (tree, _temp) = create_test_tree(2, "root_split");

        tree.insert(1, vec![1]).await;
        tree.insert(2, vec![2]).await;
        tree.insert(3, vec![3]).await;
        tree.insert(4, vec![4]).await;

        let root = tree.root.read().await;
        match &*root {
            Node::Internal(internal) => {
                assert_eq!(internal.keys.len(), 1);
                assert_eq!(internal.children.len(), 2);
            }
            _ => panic!("Root should be internal node after split"),
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_large_value_storage() {
        let temp_dir = TempDir::new().unwrap();
        let mut tree = BPlus::new(2, temp_dir.path().to_path_buf()).unwrap();
        tree.max_file_size = 100;

        let large_data = vec![7; 150];
        tree.insert(1, large_data.clone()).await;

        let result = tree.get(&1).await.unwrap();
        assert_eq!(result, large_data);
        tree.insert(2, large_data.clone()).await;
        let result = tree.get(&1).await.unwrap();
        assert_eq!(result, large_data);

        assert!(
            tree.file_number.load(std::sync::atomic::Ordering::SeqCst) >= 1,
            "Should create multiple files"
        );
    }

    #[tokio::test]
    async fn test_save_load_empty_tree() {
        let tempdir = TempDir::new().unwrap();
        let tree_path = tempdir.path().join("empty_tree.bin");

        let tree = BPlus::<u64>::new(2, tempdir.path().into()).unwrap();

        tree.save(&tree_path).await.unwrap();

        let loaded_tree = BPlus::<u64>::load(&tree_path).await.unwrap();

        assert_eq!(tree.t, loaded_tree.t);
        assert_eq!(tree.path, loaded_tree.path);
        assert_eq!(
            tree.file_number.load(Ordering::SeqCst),
            loaded_tree.file_number.load(Ordering::SeqCst)
        );
        assert_eq!(
            tree.offset.load(Ordering::SeqCst),
            loaded_tree.offset.load(Ordering::SeqCst)
        );
        assert!(loaded_tree.get(&42).await.is_err());
    }
}
