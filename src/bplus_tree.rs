use crate::chunk_pointer::{ChunkHandler, ChunkPointer};
use std::{
    fs::File,
    io::{self, ErrorKind},
    os::unix::fs::FileExt,
    path::PathBuf,
};
extern crate chunkfs;
use chunkfs::{Data, DataContainer, Database};

const MAXSIZE: u64 = 2 << 20;

pub struct BPlus<K> {
    t: usize,
    root: Option<Box<Node<K>>>,
    path: PathBuf,
    file_number: usize,
    offset: u64,
    current_file: File,
}

#[derive(Clone)]
#[allow(dead_code)]
struct Node<K> {
    leaf: bool,
    key_num: usize,
    keys: Vec<K>,
    children: Vec<Option<Box<Node<K>>>>,
    pointers: Vec<ChunkHandler>,
}

impl<K: Ord + Clone + Default> Node<K> {
    fn new(leaf: bool) -> Self {
        let new_node = Node {
            leaf: leaf,
            key_num: 0,
            keys: Vec::new(),
            children: Vec::new(),
            pointers: Vec::new(),
        };
        new_node
    }
}
#[allow(dead_code)]
impl<K: Ord + Clone + Default> BPlus<K> {
    pub fn new(t: usize, path: PathBuf) -> Self {
        let root = Node::new(true);
        let mut nodes: Vec<Box<Node<K>>> = Vec::new();
        nodes.push(Box::new(root));
        let path_to_file = path.join("0");
        let res = File::create(path_to_file);
        let current_file;
        match res {
            Ok(x) => current_file = x,
            Err(_x) => panic!(),
        }
        Self {
            t,
            root: Some(Box::new(Node::new(true))),
            path,
            file_number: 0,
            offset: 0,
            current_file,
        }
    }

    fn find_leaf<'a>(&self, node: &'a Box<Node<K>>, key: &K) -> Option<&'a Box<Node<K>>> {
        let i;
        let res = node.keys.binary_search(key);
        match res {
            Ok(x) => i = x + 1,
            Err(x) => i = x,
        }
        if node.leaf {
            for i in 0..node.key_num {
                if node.keys[i] == *key {
                    return Some(node);
                }
            }
            return None;
        } else {
            return self.find_leaf(node.children[i].as_ref().unwrap(), key);
        }
    }

    pub fn get(&self, key: &K) -> io::Result<Vec<u8>> {
        let i;
        let maybe_node = self.find_leaf(self.root.as_ref().unwrap(), key);

        let node;
        match maybe_node {
            Some(x) => node = x,
            None => return Err(ErrorKind::NotFound.into()),
        }

        let res = node.keys.binary_search(key);
        match res {
            Ok(x) => i = x,
            Err(x) => {
                if x == node.key_num {
                    return Err(ErrorKind::NotFound.into());
                }
                i = x
            }
        }
        let res = node.pointers[i].read();
        match res {
            Err(error) => Err(error),
            Ok(result) => Ok(result),
        }
    }

    fn split_children(&mut self, parent: &mut Box<Node<K>>, i: usize) {
        parent.children.push(None);
        let mut children = parent.children.swap_remove(i).unwrap().clone();
        let mut new_children = Node::new(children.leaf);
        if new_children.leaf {
            new_children.key_num = self.t;
        } else {
            new_children.key_num = self.t - 1;
        }

        new_children.keys.resize(new_children.key_num, K::default());
        new_children
            .pointers
            .resize(new_children.key_num, ChunkHandler::default());
        new_children.children.resize(new_children.key_num + 1, None);

        let mut not_leaf_const = 0;
        if !new_children.leaf {
            not_leaf_const = 1;
        }

        for j in 0..new_children.key_num {
            new_children.keys[j] = children.keys[j + self.t + not_leaf_const].clone();
            if new_children.leaf {
                new_children.pointers[j] = children.pointers[j + self.t + not_leaf_const].clone();
            }
        }

        if !children.leaf {
            for j in 0..=new_children.key_num {
                new_children.children.push(None);
                new_children.children[j] =
                    children.children.swap_remove(j + self.t + not_leaf_const);
            }
        }

        children.key_num = self.t;

        parent.children.push(None);

        for j in parent.key_num..i {
            parent.children.push(None);
            parent.children[j + 1] = parent.children.swap_remove(j);
        }

        parent.children[i + 1] = Some(Box::new(new_children));
        parent.keys.push(K::default());
        if i != parent.key_num {
            for j in parent.key_num - 1..=i {
                parent.keys[j + 1] = parent.keys[j].clone();
            }
        }
        parent.key_num += 1;
        parent.keys[i] = children.keys[self.t].clone();
        let key_num = children.key_num;
        children.keys.resize(key_num, K::default());
        children.pointers.resize(key_num, ChunkHandler::default());
        children.children.resize(key_num + 1, None);
        parent.children[i] = Some(children);
    }

    pub fn contains(&self, key: &K) -> bool {
        let result = self.get(key);
        match result {
            Ok(_x) => true,
            Err(_x) => false,
        }
    }

    fn insert_helper(&mut self, node: &mut Box<Node<K>>, key: &K, value: ChunkHandler) {
        let i;
        let res = node.keys.binary_search(key);
        match res {
            Ok(x) => i = x + 1,
            Err(x) => i = x,
        }
        if !node.leaf {
            let key_num;
            {
                let temp = node.children[i].as_mut().unwrap();
                self.insert_helper(temp, key, value);
                key_num = temp.key_num;
            }

            if key_num == 2 * self.t {
                self.split_children(node, i);
            }
        } else {
            node.keys.push(K::default());
            node.pointers.push(ChunkHandler::default());
            if node.key_num as i32 - 1 >= i as i32 {
                for j in node.key_num - 1..=i {
                    node.keys[j + 1] = node.keys[j].clone();
                    node.pointers[j + 1] = node.pointers[j].clone();
                }
            }

            node.keys[i] = key.clone();
            node.pointers[i] = value;
            node.key_num += 1;
        }
    }

    pub fn insert(&mut self, key: K, value: Vec<u8>) -> io::Result<()> {
        let mut root = self.root.clone().unwrap();
        if self.contains(&key) {
            return Ok(());
        }

        if self.offset >= MAXSIZE {
            self.file_number += 1;
            self.offset = 0;
            self.current_file =
                File::create(self.path.join(format!("{}", self.file_number))).unwrap();
        }

        let value_size = value.len();
        let res = self.current_file.write_at(&value, self.offset);
        if res.is_err() {
            panic!();
        }
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
            self.root = Some(new_root);
        }

        self.root = Some(root);
        Ok(())
    }

    pub fn remove(&mut self, key: &K) {
        let mut root = self.root.clone().unwrap();
        self.remove_helper(&mut root, key);
        if root.key_num == 0 && !root.leaf {
            let temp = root.children[0].clone();
            self.root = Some(temp.unwrap());
        }

        self.root = Some(root);
    }

    fn remove_helper(&mut self, node: &mut Box<Node<K>>, key: &K) {
        let i ;

        if node.leaf {
            let res = node.keys.binary_search(key);
            match res {
                Ok(x) => i = x,
                Err(_x) => return,
            }
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
        } else {
            let res = node.keys.binary_search(key);
            match res {
                Ok(x) => i = x + 1,
                Err(x) => i = x,
            }
            node.children.push(None);
            let mut temp = node.children.swap_remove(i).unwrap();
            self.remove_helper(&mut temp, key);
            if temp.key_num == self.t - 2 {
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
                } else {
                    if node.children[i + 1].as_ref().unwrap().key_num == self.t - 1 {
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
                }

                if temp.key_num == 0 {
                    node.children[i] = temp.children.swap_remove(0);
                } else {
                    node.children[i] = Some(temp);
                }
            }
        }
    }

    fn merge(&self, node1: &mut Box<Node<K>>, node2: &mut Box<Node<K>>) {
        let new_key_num = node1.key_num + node2.key_num;

        node1.keys.resize(new_key_num, K::default());
        if node1.leaf {
            node1.pointers.resize(new_key_num, ChunkHandler::default());
        } else {
            node1.children.resize(new_key_num + 2, None);
        }
        for i in 0..node2.key_num {
            let temp = node1.key_num;
            node1.keys[temp + i] = node2.keys[i].clone();
            if node1.leaf {
                node1.pointers[temp] = node2.pointers[i].clone();
            } else {
                node1.children[temp + i + 1] = node2.children[i].clone();
            }
        }

        if !node1.leaf {
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
        match self.get(key) {
            Ok(chunk) => Ok(DataContainer::from(chunk)),
            Err(error) => Err(error),
        }
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
        let tempdir = &TempDir::new("1").unwrap();
        let path = PathBuf::new().join(tempdir.path());
        let mut tree: BPlus<usize> = BPlus::new(2, path);
        let _ = tree.insert(1, vec![1 as u8; 1]);
        let _ = tree.insert(2, vec![2 as u8; 1]);
        let _ = tree.insert(3, vec![2 as u8; 1]);
        let _ = tree.insert(4, vec![2 as u8; 1]);
        let _ = tree.insert(5, vec![2 as u8; 1]);
        let a = tree.get(&1).unwrap();
        assert_eq!(a, vec![1 as u8; 1]);
    }

    #[test]
    fn test_insert_and_delete() {
        let tempdir = &TempDir::new("2").unwrap();
        let path = PathBuf::new().join(tempdir.path());
        let mut tree: BPlus<usize> = BPlus::new(2, path);
        let _ = tree.insert(1, vec![1 as u8; 1]);
        let _ = tree.insert(2, vec![2 as u8; 1]);
        let _ = tree.insert(3, vec![2 as u8; 1]);
        let _ = tree.insert(4, vec![2 as u8; 1]);
        let _ = tree.insert(5, vec![2 as u8; 1]);
        tree.remove(&1);
        let a = tree.get(&1);
        match a {
            Ok(_x) => assert!(false),
            Err(_x) => return,
        }
    }

    #[test]
    fn test_insert_delete_and_find() {
        let tempdir = &TempDir::new("3").unwrap();
        let path = PathBuf::new().join(tempdir.path());
        let mut tree: BPlus<usize> = BPlus::new(2, path);
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
        let tempdir = &TempDir::new("4").unwrap();
        let path = PathBuf::new().join(tempdir.path());
        let mut tree: BPlus<usize> = BPlus::new(2, path);
        for i in 1..256 {
            let _ = tree.insert(i, vec![i as u8; 1]);
        }
        for i in 1..256 {
            assert_eq!(tree.get(&(i as usize)).unwrap(), vec![i as u8; 1]);
        }
    }

    #[test]
    fn test_insert_and_delete_many_nodes() {
        let tempdir = &TempDir::new("5").unwrap();
        let path = PathBuf::new().join(tempdir.path());
        let mut tree: BPlus<usize> = BPlus::new(2, path);
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
        let tempdir = &TempDir::new("6").unwrap();
        let path = PathBuf::new().join(tempdir.path());
        let mut tree: BPlus<usize> = BPlus::new(100, path);
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
        let tempdir = &TempDir::new("7").unwrap();
        let path = PathBuf::new().join(tempdir.path());
        let mut tree: BPlus<usize> = BPlus::new(100, path);
        let mut htable = HashMap::<usize, Vec<u8>>::new();
        for i in 1..10000 {
            let key;
            key = (i * 113) % 10000000;
            let _ = tree.insert(key, vec![key as u8; 1064]);
            htable.insert(key, vec![key as u8; 1064]);
        }
        for i in htable {
            assert_eq!(tree.get(&i.0).unwrap(), i.1);
        }
    }
}
