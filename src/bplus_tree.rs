use crate::chunk_pointer::{ChunkHandler, ChunkPointer};
use std::{
    cell::RefCell,
    fs::File,
    io::{self, ErrorKind},
    os::unix::fs::FileExt,
    path::PathBuf,
    rc::Rc,
};

extern crate chunkfs;

#[allow(dead_code)]
type Link<K> = Option<Rc<RefCell<Node<K>>>>;

#[allow(dead_code)]
#[derive(Clone)]
enum Node<K> {
    Internal(InternalNode<K>),
    Leaf(Leaf<K>),
}

#[allow(dead_code)]
#[derive(Clone)]
struct InternalNode<K> {
    children: Vec<Link<K>>,
    keys: Vec<Rc<K>>,
}

#[allow(dead_code)]
#[derive(Default, Clone)]
struct Leaf<K> {
    entries: Vec<(Rc<K>, ChunkHandler)>,
    next: Link<K>,
}

#[allow(dead_code)]
struct BPlus<K> {
    root: Node<K>,
    t: usize,
    path: PathBuf,
    file_number: usize,
    offset: u64,
    current_file: File,
    max_file_size: u64,
}

#[allow(dead_code, unused_variables)]
impl<K: Default + Ord + Clone> BPlus<K> {
    fn new(t: usize, path: PathBuf) -> io::Result<Self> {
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

    fn insert(&mut self, key: Rc<K>, value: Vec<u8>) -> io::Result<()> {
        let value_to_insert = self.get_chunk_handler(value).unwrap();

        if let Some((new_node, new_key)) = self.root.insert(key, value_to_insert, self.t) {
            {
                let new_root = Node::<K>::Internal(InternalNode {
                    children: vec![
                        Some(Rc::new(RefCell::new(self.root.clone()))),
                        Some(Rc::new(RefCell::new(new_node))),
                    ],
                    keys: vec![new_key],
                });
                self.root = new_root;
            }
        }
        Ok(())
    }

    fn remove(&mut self, key: Rc<K>) -> io::Result<()> {
        todo!()
    }

    fn get(&self, key: Rc<K>) -> io::Result<Vec<u8>> {
        self.root.get(key)
    }
}

impl<K: Clone + Ord> Node<K> {
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
                let new_node_keys = internal_node.keys.split_off(t);
                let middle_key = new_node_keys[0].clone();

                let new_node_children = internal_node.children.split_off(t);

                let new_node = Node::Internal(InternalNode {
                    children: new_node_children,
                    keys: new_node_keys,
                });

                (new_node, middle_key)
            }
        }
    }

    fn insert(&mut self, key: Rc<K>, value: ChunkHandler, t: usize) -> Option<(Node<K>, Rc<K>)> {
        match self {
            Node::Leaf(leaf) => {
                let pos = leaf
                    .entries
                    .binary_search_by(|(k, _)| k.cmp(&key))
                    .unwrap_or_else(|e| e);
                leaf.entries.insert(pos, (key.clone(), value));
                if leaf.entries.len() == 2 * t {
                    return Some(self.split(t));
                }

                None
            }
            Node::Internal(internal_node) => {
                let pos = internal_node.keys.binary_search(&key).unwrap_or_else(|e| e);
                let child = internal_node.children[pos].clone().unwrap();
                let mut borrowed_child = child.borrow_mut();
                let result = borrowed_child.insert(key, value, t);

                match result {
                    Some((new_child, key)) => {
                        internal_node.keys.insert(pos, key.clone());
                        internal_node
                            .children
                            .insert(pos + 1, Some(Rc::new(RefCell::new(new_child))));

                        match internal_node.keys.len() {
                            val if val == 2 * t => Some(self.split(t)),
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
        todo!()
    }

    fn get(&self, key: Rc<K>) -> io::Result<Vec<u8>> {
        match self {
            Node::Leaf(leaf) => match leaf.entries.binary_search_by(|(k, _)| k.cmp(&key)) {
                Ok(pos) => Ok(leaf.entries[pos].1.read().unwrap()),
                Err(_) => Err(ErrorKind::NotFound.into()),
            },
            Node::Internal(internal_node) => {
                let pos = internal_node
                    .keys
                    .binary_search_by(|k| k.cmp(&key))
                    .unwrap_or_else(|e| e);
                let child = internal_node.children.get(pos);
                match child {
                    Some(x) => x.clone().unwrap().borrow().get(key),
                    None => Err(ErrorKind::NotFound.into()),
                }
            }
        }
    }
}
