use crate::chunk_pointer::{ChunkHandler, ChunkPointer};
use std::{
    fs::File,
    io::{self, ErrorKind},
    mem,
    os::unix::fs::FileExt,
    path::PathBuf,
};

extern crate chunkfs;
use chunkfs::{Data, DataContainer, Database};

const MAXSIZE: u64 = 2 << 20;

/// B+ tree structure
pub struct BPlus<K> {
    /// Parameter responsible for the minimum and maximum number of children
    t: usize,
    /// Root of the tree
    root: Box<Node<K>>,
    /// Path to the directory where chunks will be saved
    path: PathBuf,
    /// Number of current file with chunkgs
    file_number: usize,
    /// Current offset in the current file
    offset: u64,
    /// The file where chunks are currently being written
    current_file: File,
}

/// Node for B+ tree
#[derive(Clone)]
struct Node<K> {
    /// Indicates whether the node is a leaf
    is_leaf: bool,
    /// Number of keys in the node
    key_num: usize,
    /// Vector of keys in the node
    keys: Vec<K>,
    /// Vector of children of this node
    children: Vec<Option<Box<Node<K>>>>,
    /// Vector of pointers to data stored in a node
    pointers: Vec<ChunkHandler>,
}

impl<K: Ord + Clone + Default> Node<K> {
    fn new(is_leaf: bool) -> Self {
        Node {
            is_leaf,
            key_num: 0,
            keys: Vec::new(),
            children: Vec::new(),
            pointers: Vec::new(),
        }
    }
}
#[allow(dead_code)]
impl<K: Ord + Clone + Default> BPlus<K> {
    pub fn new(t: usize, path: PathBuf) -> io::Result<Self> {
        let path_to_file = path.join("0");
        let current_file = File::create(path_to_file)?;
        Ok(Self {
            t,
            root: Box::new(Node::new(true)),
            path,
            file_number: 0,
            offset: 0,
            current_file,
        })
    }

    fn find_leaf<'a>(node: &'a Node<K>, key: &K) -> Option<&'a Node<K>> {
        let i = match node.keys.binary_search(key) {
            Ok(x) => x + 1,
            Err(x) => x,
        };

        if node.is_leaf {
            if node.keys.contains(key) {
                Some(node)
            } else {
                None
            }
        } else {
            BPlus::find_leaf(node.children[i].as_ref().unwrap(), key)
        }
    }

    pub fn get(&self, key: &K) -> io::Result<Vec<u8>> {
        let maybe_node = BPlus::find_leaf(self.root.as_ref(), key);
        let Some(node) = maybe_node else {
            return Err(ErrorKind::NotFound.into());
        };

        let i = match node.keys.binary_search(key) {
            Ok(x) => x,
            Err(x) => {
                if x == node.key_num {
                    return Err(ErrorKind::NotFound.into());
                }
                x
            }
        };

        node.pointers[i].read()
    }

    /// Split one of the children of the node into two
    fn split_children(&mut self, parent: &mut Box<Node<K>>, child_index: usize) {
        parent.children.push(None);
        let mut children = parent.children.swap_remove(child_index).unwrap();
        let mut new_children = Node::new(children.is_leaf);
        if new_children.is_leaf {
            new_children.key_num = self.t;
        } else {
            new_children.key_num = self.t - 1;
        }

        new_children.keys.resize(new_children.key_num, K::default());
        new_children
            .pointers
            .resize(new_children.key_num, ChunkHandler::default());
        new_children.children.resize(new_children.key_num + 1, None);

        let not_leaf_const = if !new_children.is_leaf { 1 } else { 0 };

        for j in 0..new_children.key_num {
            new_children.keys[j] = mem::replace(
                &mut children.keys[j + self.t + not_leaf_const],
                K::default(),
            );
            if new_children.is_leaf {
                new_children.pointers[j] = children.pointers[j + self.t + not_leaf_const].clone();
            }
        }

        if !children.is_leaf {
            for j in 0..=new_children.key_num {
                new_children.children.push(None);
                new_children.children[j] =
                    children.children.swap_remove(j + self.t + not_leaf_const);
            }
        }

        children.key_num = self.t;

        parent.children.push(None);

        for j in (child_index + 1..=parent.key_num).rev() {
            parent.children.push(None);
            parent.children[j + 1] = parent.children.swap_remove(j);
        }

        parent.children[child_index + 1] = Some(Box::new(new_children));
        parent.keys.push(K::default());
        if child_index != parent.key_num {
            for j in (child_index..parent.key_num).rev() {
                parent.keys[j + 1] = mem::replace(&mut parent.keys[j], K::default());
            }
        }
        parent.key_num += 1;
        parent.keys[child_index] = mem::replace(&mut children.keys[self.t], K::default());
        let key_num = children.key_num;
        children.keys.resize(key_num, K::default());
        children.pointers.resize(key_num, ChunkHandler::default());
        children.children.resize(key_num + 1, None);
        parent.children[child_index] = Some(children);
    }

    pub fn contains(&self, key: &K) -> bool {
        let result = self.get(key);
        result.is_ok()
    }

    fn insert_helper(&mut self, node: &mut Box<Node<K>>, key: &K, value: ChunkHandler) {
        let i = match node.keys.binary_search(key) {
            Ok(x) => {
                if node.is_leaf {
                    node.pointers[x] = value;
                    return;
                } else {
                    x + 1
                }
            }
            Err(x) => x,
        };

        if !node.is_leaf {
            let key_num = {
                let temp = node.children[i].as_mut().unwrap();
                self.insert_helper(temp, key, value);
                temp.key_num
            };

            if key_num == 2 * self.t {
                self.split_children(node, i);
            }
        } else {
            node.keys.push(K::default());
            node.pointers.push(ChunkHandler::default());
            if node.key_num > i {
                for j in (i..node.key_num).rev() {
                    node.keys[j + 1] = mem::replace(&mut node.keys[j], K::default());
                    node.pointers[j + 1] =
                        mem::replace(&mut node.pointers[j], ChunkHandler::default());
                }
            }

            node.keys[i] = key.clone();
            node.pointers[i] = value;
            node.key_num += 1;
        }
    }

    pub fn insert(&mut self, key: K, value: Vec<u8>) -> io::Result<()> {
        let mut root = self.root.clone();
        if self.offset >= MAXSIZE {
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
        self.insert_helper(&mut root, &key, value_to_insert);
        if root.key_num == 2 * self.t {
            let mut new_root = Node::new(false);
            new_root.children.push(Some(root.clone()));
            let mut new_root = Box::new(new_root);
            self.split_children(&mut new_root, 0);
            self.root = new_root;
        }

        self.root = root;
        Ok(())
    }

    pub fn remove(&mut self, key: &K) {
        let mut root = self.root.clone();
        self.remove_helper(&mut root, key);
        if root.key_num == 0 && !root.is_leaf {
            let temp = root.children[0].clone();
            self.root = temp.unwrap();
        }

        self.root = root;
    }

    fn remove_helper(&mut self, node: &mut Box<Node<K>>, key: &K) {
        if node.is_leaf {
            let res = node.keys.binary_search(key);
            let i = match res {
                Ok(x) => x,
                Err(_x) => return,
            };
            if i != node.key_num {
                node.key_num -= 1;
                let key_num = node.key_num;
                for j in i..key_num {
                    node.keys[j] = node.keys[j + 1].clone();
                    node.pointers[j] = node.pointers[j + 1].clone();
                }
                node.keys.resize(key_num, K::default());
                node.pointers.resize(key_num, ChunkHandler::default());
            }
            return;
        }

        let res = node.keys.binary_search(key);
        let i = match res {
            Ok(x) => x + 1,
            Err(x) => x,
        };
        node.children.push(None);
        let mut temp = node.children.swap_remove(i).unwrap();
        self.remove_helper(&mut temp, key);

        if temp.key_num != self.t - 2 {
            return;
        }

        if i != 0 {
            if node.children[i - 1].as_ref().unwrap().key_num == self.t - 1 {
                node.children.push(None);
                let mut temp1 = node.children.swap_remove(i - 1).unwrap();
                self.merge(&mut temp1, &mut temp);
                node.key_num -= 1;
                let key_num = node.key_num;
                for j in i..key_num {
                    node.keys[j] = node.keys[j + 1].clone();
                    node.keys[j] = node.keys[j + 1].clone();
                }
                node.keys.resize(key_num, K::default());
                node.children[i - 1] = Some(temp1);
            } else {
                node.children.push(None);
                let mut prev_children = node.children.swap_remove(i - 1).unwrap();
                let moved_key = prev_children.keys[prev_children.key_num - 1].clone();
                let moved_value = prev_children.pointers[prev_children.key_num - 1].clone();
                self.insert_helper(&mut temp, &moved_key, moved_value);
                self.remove_helper(&mut prev_children, &moved_key);
                node.keys[i - 1] = temp.keys[0].clone();
                node.children[i - 1] = Some(prev_children);
            }
        } else if node.children[i + 1].as_ref().unwrap().key_num == self.t - 1 {
            node.children.push(None);
            let mut temp1 = node.children.swap_remove(i + 1).unwrap();
            self.merge(&mut temp, &mut temp1);
            node.key_num -= 1;
            let key_num = node.key_num;
            for j in 0..key_num {
                node.keys[j] = node.keys[j + 1].clone();
                node.keys[j] = node.keys[j + 1].clone();
            }
            node.keys.resize(key_num, K::default());
            node.children[i + 1] = Some(temp1);
        } else {
            node.children.push(None);
            let mut prev_children = node.children.swap_remove(i + 1).unwrap();
            let moved_key = prev_children.keys[0].clone();
            let moved_value = prev_children.pointers[0].clone();
            self.insert_helper(&mut temp, &moved_key, moved_value);
            self.remove_helper(&mut prev_children, &moved_key);
            node.keys[i] = prev_children.keys[0].clone();
            node.children[i + 1] = Some(prev_children);
        }

        if temp.key_num == 0 {
            node.children[i] = temp.children.swap_remove(0);
        } else {
            node.children[i] = Some(temp);
        }
    }

    fn merge(&self, node1: &mut Box<Node<K>>, node2: &mut Box<Node<K>>) {
        let new_key_num = node1.key_num + node2.key_num;

        node1.keys.resize(new_key_num, K::default());
        if node1.is_leaf {
            node1.pointers.resize(new_key_num, ChunkHandler::default());
        } else {
            node1.children.resize(new_key_num + 2, None);
        }
        for i in 0..node2.key_num {
            let temp = node1.key_num;
            node1.keys[temp + i] = node2.keys[i].clone();
            if node1.is_leaf {
                node1.pointers[temp] = node2.pointers[i].clone();
            } else {
                node1.children[temp + i + 1] = node2.children[i].clone();
            }
        }

        if !node1.is_leaf {
            node1.children[new_key_num + 1] = node2.children[node2.key_num].clone();
        }
    }
}

impl<K: Ord + Clone + Default> Database<K, DataContainer<()>> for BPlus<K> {
    fn insert(&mut self, key: K, value: DataContainer<()>) -> io::Result<()> {
        match value.extract() {
            Data::Chunk(chunk) => self.insert(key, chunk.clone()),
            Data::TargetChunk(_chunk) => unimplemented!(),
        }
    }

    fn get(&self, key: &K) -> io::Result<DataContainer<()>> {
        self.get(key).map(DataContainer::from)
    }

    fn contains(&self, key: &K) -> bool {
        self.contains(key)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use tempdir::TempDir;

    use super::*;

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
    fn test_insert_and_delete() {
        let tempdir = TempDir::new("2").unwrap();
        let path = PathBuf::new().join(tempdir.path());
        let mut tree: BPlus<usize> = BPlus::new(2, path).unwrap();
        for i in 1..6 {
            let _ = tree.insert(i, vec![i as u8; 1]);
        }

        tree.remove(&1);
        let a = tree.get(&1);
        assert!(a.is_err());
    }

    #[test]
    fn test_insert_delete_and_find() {
        let tempdir = TempDir::new("3").unwrap();
        let path = PathBuf::new().join(tempdir.path());
        let mut tree: BPlus<usize> = BPlus::new(2, path).unwrap();
        let _ = tree.insert(1, vec![1 as u8; 1]);
        let _ = tree.insert(2, vec![2 as u8; 1]);
        let _ = tree.insert(3, vec![2 as u8; 1]);
        let _ = tree.insert(4, vec![2 as u8; 1]);
        let _ = tree.insert(5, vec![2 as u8; 1]);
        tree.remove(&2);
        tree.remove(&3);
        tree.remove(&4);
        tree.remove(&5);
        let a = tree.get(&1).unwrap();
        assert_eq!(a, vec![1 as u8; 1]);
    }

    #[test]
    fn test_insert_and_find_many_nodes() {
        let tempdir = TempDir::new("4").unwrap();
        let path = PathBuf::new().join(tempdir.path());
        let mut tree: BPlus<usize> = BPlus::new(2, path).unwrap();
        for i in 1..256 {
            let _ = tree.insert(i, vec![i as u8; 1]);
        }
        for i in 1..256 {
            assert_eq!(tree.get(&(i as usize)).unwrap(), vec![i as u8; 1]);
        }
    }

    #[test]
    fn test_insert_and_delete_many_nodes() {
        let tempdir = TempDir::new("5").unwrap();
        let path = PathBuf::new().join(tempdir.path());
        let mut tree: BPlus<usize> = BPlus::new(2, path).unwrap();
        let _ = tree.insert(1, vec![1 as u8; 1]);
        for i in 2..30 {
            let _ = tree.insert(i, vec![i as u8; 1]);
        }

        for i in 2..15 {
            tree.remove(&i);
        }
        for i in 2..15 {
            let a = tree.get(&i);
            match a {
                Ok(_x) => assert!(false),
                Err(_x) => continue,
            }
        }
        for i in 15..30 {
            let a = tree.get(&i).unwrap();
            assert_eq!(a, vec![i as u8; 1]);
        }
        let a = tree.get(&1).unwrap();
        assert_eq!(a, vec![1 as u8; 1]);
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
            println!("{}", key);
            println!("{:?}", tree.get(&key).unwrap());
            assert_eq!(vec![key as u8], tree.get(&key).unwrap());
        }

        let key: usize = rand::random();
        tree.insert(key, vec![0u8]).unwrap();
        for i in 1..255 {
            assert_eq!(vec![i - 1u8], tree.get(&key).unwrap());
            tree.insert(key, vec![i]).unwrap();
        }
    }
}
