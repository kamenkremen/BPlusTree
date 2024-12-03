use crate::chunk::FileHandler;

struct BPlus {
    t: usize,
    root: Option<Box<Node>>,
}

type ValueType = FileHandler;
#[derive(Clone)]
#[allow(dead_code)]
struct Node {
    leaf: bool,
    key_num: usize,
    keys: Vec<usize>,
    children: Vec<Option<Box<Node>>>,
    pointers: Vec<ValueType>
    /*left: Option<usize>,
    right: Option<usize>,*/
}
#[allow(dead_code)]
impl BPlus {
    fn new(t: usize) -> Self {
        let root = Self::create_new_node(true);
        let mut nodes: Vec<Box<Node>> = Vec::new();
        nodes.push(Box::new(root));
        Self {t, root: Some(Box::new(Self::create_new_node(true)))}
    }

    fn create_new_node(leaf: bool) -> Node {
        let new_node = Node{leaf : leaf, key_num: 0, keys: Vec::new(), children: Vec::new(), pointers: Vec::new()};
        new_node
    }

    fn find_leaf<'a>(&self, node: &'a Box<Node>, key: usize) -> Option<&'a Box<Node>> {
        let mut i = 0;
        while i < node.key_num {
            if node.keys[i] > key {
                break;
            }
            i += 1; 
        } 
        if node.leaf {
            for i in 0..node.key_num {
                if node.keys[i] == key {
                    return Some(node);
                }
            }
            return None;
        } else {
            return self.find_leaf(node.children[i].as_ref().unwrap(), key);
        }
    }

    fn find(&self, key: usize) -> Option<&ValueType> {
        let mut i: usize = 0;
        let maybe_node = self.find_leaf(self.root.as_ref().unwrap(), key);
        
        let node;
        match maybe_node {
            Some(x) => node = x,
            None => return None
        }

        while i < node.key_num && key != node.keys[i] {
            i += 1;
        }

        if i == node.key_num {
            return None;
        } else {
            return Some(&node.pointers[i]);
        }
    }

    fn split_children(self, parent: &mut Box<Node>, i: usize) -> BPlus {
        parent.children.push(None);
        let mut children = parent.children.swap_remove(i).unwrap().clone();
        let mut new_children = Self::create_new_node(children.leaf);
        if new_children.leaf {
            new_children.key_num = self.t;
        } else {
            new_children.key_num = self.t - 1;
        }
        
        new_children.keys.resize(new_children.key_num , 0);
        new_children.pointers.resize(new_children.key_num , FileHandler::default());
        new_children.children.resize(new_children.key_num + 1, None);
        
        let mut not_leaf_const = 0;
        if !new_children.leaf {
            not_leaf_const = 1;
        }

        for j in 0..new_children.key_num {
            new_children.keys[j] = children.keys[j + self.t + not_leaf_const];
            if new_children.leaf {
                new_children.pointers[j] = children.pointers[j + self.t + not_leaf_const].clone();
            }
        }

        if !children.leaf {
            for j in 0..=new_children.key_num {
                new_children.children.push(None);
                new_children.children[j] = children.children.swap_remove(j + self.t + not_leaf_const);
            }
        }
        
        children.key_num = self.t;

        parent.children.push(None);

        for j in parent.key_num..i {
            parent.children.push(None);
            parent.children[j + 1] = parent.children.swap_remove(j);
        }

        parent.children[i + 1] = Some(Box::new(new_children));
        parent.keys.push(0);
        if i != parent.key_num {
            for j in parent.key_num-1..=i {
                parent.keys[j + 1] = parent.keys[j];
            }
        }
        parent.key_num += 1;
        parent.keys[i] = children.keys[self.t];
        let key_num = children.key_num;
        children.keys.resize(key_num, 0);
        children.pointers.resize(key_num, FileHandler::default());
        children.children.resize(key_num + 1, None);
        parent.children[i] = Some(children);
        self
    }

    fn insert_helper(mut self, node: &mut Box<Node>, key: usize, value: ValueType) -> BPlus {
        let mut i = 0;
        while i < node.key_num {
            if node.keys[i] > key {
                break;
            }
            i += 1;
        }
        if !node.leaf {
            let key_num;
            {
                let temp = node.children[i].as_mut().unwrap();
                self = self.insert_helper(temp, key, value);
                key_num = temp.key_num;
            }
            
            if key_num == 2 * self.t {
                self = self.split_children(node, i);
            }
        } else {
            node.keys.push(0);
            node.pointers.push(FileHandler::default());
            if node.key_num as i32 - 1 >= i as i32  {
                for j in node.key_num - 1..=i {
                    node.keys[j + 1] = node.keys[j];
                    node.pointers[j + 1] = node.pointers[j].clone();
                }
            }

            node.keys[i] = key;
            node.pointers[i] = value;
            node.key_num += 1;
        }

        self
    }

    fn insert(mut self, key: usize, value: ValueType) -> BPlus {
        let mut root = self.root.clone().unwrap();
        self = self.insert_helper(&mut root, key, value);
        if root.key_num == 2 * self.t {
            let mut new_root = Self::create_new_node(false);
            new_root.children.push(Some(root.clone()));
            let mut new_root = Box::new(new_root);
            self = Self::split_children(self, &mut new_root, 0);
            self.root = Some(new_root);
        }

        self.root = Some(root);
        self
    }

    fn delete(mut self, key: usize) -> BPlus {
        let mut root = self.root.clone().unwrap();
        self = self.delete_helper(&mut root, key);
        if root.key_num == 0 && !root.leaf {
            let temp = root.children[0].clone();
            self.root = Some(temp.unwrap());
        }

        self.root = Some(root);
        self
    }

    fn delete_helper(mut self, node: &mut Box<Node>, key: usize) -> BPlus {
        let mut i = 0;

        if node.leaf {
            while i < node.key_num {
                if node.keys[i] == key {
                    break;
                }
                i += 1;
            }
            if i != node.key_num {
                node.key_num -= 1;
                let key_num = node.key_num;
                for j in i..key_num {
                    node.keys[j] = node.keys[j + 1];
                    node.pointers[j] = node.pointers[j + 1].clone();
                }
                node.keys.resize(key_num, 0);
                node.pointers.resize(key_num, FileHandler::default());
            }
        } else {
            while i < node.key_num {
                if node.keys[i] > key {
                    break;
                }
                i += 1;
            }
            node.children.push(None);
            let mut temp = node.children.swap_remove(i).unwrap();
            self = self.delete_helper(&mut temp, key);
            if temp.key_num == self.t - 2 {
                if i != 0 {
                    if node.children[i - 1].as_ref().unwrap().key_num == self.t - 1 {
                        node.children.push(None);

                        let mut temp1 = node.children.swap_remove(i - 1).unwrap();
                        self = self.merge(&mut temp1, &mut  temp);
                        node.key_num -= 1;
                        let key_num = node.key_num;
                        for j in i..key_num{
                            node.keys[j] = node.keys[j + 1];
                            node.keys[j] = node.keys[j + 1];
                        }
                        node.keys.resize(key_num, 0);
                        node.children[i - 1] = Some(temp1);
                    } else {
                        node.children.push(None);
                        let mut prev_children = node.children.swap_remove(i - 1).unwrap();
                        let moved_key = prev_children.keys[prev_children.key_num - 1];
                        let moved_value = prev_children.pointers[prev_children.key_num - 1].clone();
                        self = self.insert_helper(&mut temp, moved_key, moved_value);
                        self = self.delete_helper(&mut prev_children, moved_key);
                        node.keys[i - 1] = temp.keys[0];
                        node.children[i - 1] = Some(prev_children);
                    }
                } else {
                    if node.children[i + 1].as_ref().unwrap().key_num == self.t - 1 {
                        node.children.push(None);
                        let mut temp1 = node.children.swap_remove(i + 1).unwrap();
                        self = self.merge(&mut temp, &mut temp1);
                        node.key_num -= 1;
                        let key_num = node.key_num;
                        for j in 0..key_num{
                            node.keys[j] = node.keys[j + 1];
                            node.keys[j] = node.keys[j + 1];
                        }
                        node.keys.resize(key_num, 0);
                        node.children[i + 1] = Some(temp1);
                    } else {
                        node.children.push(None);
                        let mut prev_children = node.children.swap_remove(i + 1).unwrap();
                        let moved_key = prev_children.keys[0];
                        let moved_value = prev_children.pointers[0].clone();
                        self = self.insert_helper(&mut temp, moved_key, moved_value);
                        self = self.delete_helper(&mut prev_children, moved_key);
                        node.keys[i] = prev_children.keys[0];
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
        self
    }

    fn merge(self, node1: &mut Box<Node>, node2: &mut Box<Node>) -> BPlus {
        let new_key_num = node1.key_num + node2.key_num;
        
        node1.keys.resize(new_key_num, 0);
        if node1.leaf {
            node1.pointers.resize(new_key_num, FileHandler::default());
        } else {
            node1.children.resize(new_key_num + 2, None);
        }
        for i in 0..node2.key_num {
            let temp = node1.key_num;
            node1.keys[temp + i] = node2.keys[i];
            if node1.leaf {
                node1.pointers[temp] = node2.pointers[i].clone();
            } else {
                node1.children[temp + i + 1] = node2.children[i].clone();
            }
        }

        if !node1.leaf {
            node1.children[new_key_num + 1] = node2.children[node2.key_num].clone();
        }

        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chunk::ChunkPointer;

    #[test]
    fn test_insert_and_find() {
        let mut tree: BPlus = BPlus::new(2);
        tree = tree.insert(1, FileHandler::new("file1.txt".to_string()));
        tree = tree.insert(2, FileHandler::new("file2.txt".to_string()));
        tree = tree.insert(3, FileHandler::new("file3.txt".to_string()));
        tree = tree.insert(4, FileHandler::new("file4.txt".to_string()));
        tree = tree.insert(5, FileHandler::new("file5.txt".to_string()));
        let a = tree.find(1).unwrap();
        let _res = a.write(b"test1");
        let a = tree.find(1).unwrap();
        let mut buf = Vec::new();
        let _res = a.read(&mut buf);
        assert_eq!(std::str::from_utf8(&buf).unwrap(), "test1");
    }

    #[test]
    fn test_insert_and_delete() {
        let mut tree: BPlus = BPlus::new(2);
        tree = tree.insert(1, FileHandler::new("file1.txt".to_string()));
        tree = tree.insert(2, FileHandler::new("file2.txt".to_string()));
        tree = tree.insert(3, FileHandler::new("file3.txt".to_string()));
        tree = tree.insert(4, FileHandler::new("file4.txt".to_string()));
        tree = tree.insert(5, FileHandler::new("file5.txt".to_string()));
        tree = tree.delete(1);
        let a = tree.find(1);
        match a {
            Some(_x) => assert!(false),
            None => return
        }
    }

    #[test]
    fn test_insert_delete_and_find() {
        let mut tree: BPlus = BPlus::new(2);
        tree = tree.insert(1, FileHandler::new("file1.txt".to_string()));
        tree = tree.insert(2, FileHandler::new("file2.txt".to_string()));
        tree = tree.insert(3, FileHandler::new("file3.txt".to_string()));
        tree = tree.insert(4, FileHandler::new("file4.txt".to_string()));
        tree = tree.insert(5, FileHandler::new("file5.txt".to_string()));
        tree = tree.delete(2);
        tree = tree.delete(3);
        tree = tree.delete(4);
        tree = tree.delete(5);
        let a = tree.find(1).unwrap();
        assert_eq!(a.path, "file1.txt");
    }

    #[test]
    fn test_insert_and_find_many_nodes() {
        let mut tree: BPlus = BPlus::new(2);
        tree = tree.insert(1, FileHandler::new("file1.txt".to_string()));
        for i in 2..30 {
            tree = tree.insert(i, FileHandler::new("file2.txt".to_string()));
        }
        let a = tree.find(1).unwrap();
        assert_eq!(a.path, "file1.txt");
    }

    #[test]
    fn test_insert_and_delete_many_nodes() {
        let mut tree: BPlus = BPlus::new(2);
        tree = tree.insert(1, FileHandler::new("file1.txt".to_string()));
        for i in 2..30 {
            tree = tree.insert(i, FileHandler::new("file2.txt".to_string()));
        }

        for i in 2..15 {
            tree = tree.delete(i);
        }
        for i in 2..15 {
            let a = tree.find(i);
            match a {
                Some(_x) => assert!(false),
                None => continue
            }
        }
        for i in 15..30 {
            let a = tree.find(i).unwrap();
            assert_eq!(a.path, "file2.txt");
        }
        let a = tree.find(1).unwrap();
        assert_eq!(a.path, "file1.txt");
    }
}