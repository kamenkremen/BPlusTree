use crate::chunk_pointer::ChunkHandler;
use std::{cell::RefCell, rc::Rc};

extern crate chunkfs;

#[allow(dead_code)]
type Link<K> = Option<Rc<RefCell<Node<K>>>>;

#[allow(dead_code)]
#[derive(Debug)]
enum Node<K> {
    Internal(InternalNode<K>),
    Leaf(Leaf<K>),
}

#[allow(dead_code)]
#[derive(Debug)]
struct InternalNode<K> {
    children: Vec<Link<K>>,
    keys: Vec<Rc<K>>,
}

#[allow(dead_code)]
#[derive(Debug, Default)]
struct Leaf<K> {
    pointers: Vec<(Rc<K>, ChunkHandler)>,
    next: Link<K>,
}

#[allow(dead_code)]
#[derive(Debug)]
struct BPlus<K> {
    root: Node<K>,
    t: usize,
}
