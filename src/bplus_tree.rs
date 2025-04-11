use crate::chunk_pointer::ChunkHandler;
use std::{cell::RefCell, io, rc::Rc};

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
}

#[allow(dead_code)]
impl<K: Default + Ord + Clone> BPlus<K> {
    fn new(t: usize) -> Self {
        BPlus {
            root: Node::Leaf(Leaf::default()),
            t,
        }
    }

    fn insert(&mut self, key: &K, value: Vec<u8>) -> io::Result<()> {
        self.root.insert(key, value, self.t)
    }

    fn remove(&mut self, key: &K) -> io::Result<()> {
        self.root.remove(key, self.t)
    }

    fn get(&self, key: &K) -> io::Result<Vec<u8>> {
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

    fn insert(&mut self, key: &K, value: Vec<u8>, t: usize) -> io::Result<()> {
        todo!()
    }

    fn remove(&mut self, key: &K, t: usize) -> io::Result<()> {
        todo!()
    }

    fn get(&self, key: &K) -> io::Result<Vec<u8>> {
        todo!()
    }
}
