use crate::chunk_pointer::ChunkHandler;
use std::{cell::RefCell, io, rc::Rc};

extern crate chunkfs;

#[allow(dead_code)]
type Link<K> = Option<Rc<RefCell<dyn Node<K>>>>;

#[allow(dead_code)]
trait Node<K> {
    fn insert(&mut self, key: &K, value: Vec<u8>) -> io::Result<()>;
    fn get(&self, key: &K) -> io::Result<Vec<u8>>;
    fn remove(&mut self, key: &K) -> io::Result<()>;
    fn split(&mut self) -> (InternalNode<K>, InternalNode<K>, K);
}

#[allow(dead_code)]
struct InternalNode<K> {
    children: Vec<Link<K>>,
    keys: Vec<Rc<K>>,
}

#[allow(dead_code)]
#[derive(Default)]
struct Leaf<K> {
    pointers: Vec<(Rc<K>, ChunkHandler)>,
    next: Link<K>,
}

#[allow(dead_code)]
struct BPlus<K> {
    root: Box<dyn Node<K>>,
    t: usize,
}

#[allow(dead_code)]
impl<K: Default + 'static> BPlus<K> {
    fn new(t: usize) -> Self {
        BPlus {
            root: Box::new(Leaf::default()),
            t,
        }
    }

    fn insert(&mut self, key: &K, value: Vec<u8>) -> io::Result<()> {
        self.root.insert(key, value)
    }

    fn remove(&mut self, key: &K) -> io::Result<()> {
        self.root.remove(key)
    }

    fn get(&self, key: &K) -> io::Result<Vec<u8>> {
        self.root.get(key)
    }
}

impl<K> Node<K> for Leaf<K> {
    fn split(&mut self) -> (InternalNode<K>, InternalNode<K>, K) {
        todo!()
    }

    fn insert(&mut self, key: &K, value: Vec<u8>) -> io::Result<()> {
        todo!()
    }

    fn remove(&mut self, key: &K) -> io::Result<()> {
        todo!()
    }

    fn get(&self, key: &K) -> io::Result<Vec<u8>> {
        todo!()
    }
}

#[allow(dead_code)]
impl<K> Node<K> for InternalNode<K> {
    fn split(&mut self) -> (InternalNode<K>, InternalNode<K>, K) {
        todo!()
    }

    fn insert(&mut self, key: &K, value: Vec<u8>) -> io::Result<()> {
        todo!()
    }

    fn remove(&mut self, key: &K) -> io::Result<()> {
        todo!()
    }

    fn get(&self, key: &K) -> io::Result<Vec<u8>> {
        todo!()
    }
}
