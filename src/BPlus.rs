struct BPlus {
    t: usize,
    root: Option<usize>,
    nodes: Vec<Box<Node>>,
}

type ValueType = usize;

struct Node {
    leaf: bool,
    key_num: usize,
    keys: Vec<usize>,
    child: Vec<usize>,
    pointers: Vec<ValueType>,
    left: Option<usize>,
    right: Option<usize>,
}
#[allow(dead_code)]
impl BPlus {
    fn new(t: usize) -> Self {
        let root = Self::create_new_node(true);
        let mut nodes: Vec<Box<Node>> = Vec::new();
        nodes.push(Box::new(root));
        Self {t, root: Some(0), nodes: nodes}
    }

    fn create_new_node(leaf: bool) -> Node {
        let new_node = Node{leaf : leaf, key_num: 0, keys: Vec::new(), child: Vec::new(), pointers: Vec::new(), left: None, right: None};
        new_node
    }

    fn find_leaf(&self, node: usize, key: usize) -> Option<usize> {
        let mut i = 0;
        while i < self.nodes[node].key_num {
            if self.nodes[node].keys[i] > key {
                break;
            }
            i += 1;
        }
        if self.nodes[node].leaf {
            for i in 0..self.nodes[node].key_num {
                if self.nodes[node].keys[i] == key {
                    return Some(node);
                }
            }
            return None;
        } else {
            return self.find_leaf(self.nodes[node].child[i], key);
        }
    }

    fn find(&self, key: usize) -> Option<usize> {
        let mut i: usize = 0;

        let maybe_node = self.find_leaf(self.root.unwrap(), key);
        let node;
        match maybe_node {
            Some(x) => node = x,
            None => return None
        }

        while i < self.nodes[node].key_num && key != self.nodes[node].keys[i] {
            i += 1;
        }

        if i == self.nodes[node].key_num {
            return None;
        } else {
            return Some(self.nodes[node].pointers[i]);
        }
    }

    fn split_child(mut self, parent: usize, child: usize, i: usize) -> BPlus {
        let mut new_child = Self::create_new_node(self.nodes[child].leaf);
        if new_child.leaf {
            new_child.key_num = self.t;
        } else {
            new_child.key_num = self.t - 1;
        }
        
        new_child.keys.resize(new_child.key_num , 0);
        new_child.pointers.resize(new_child.key_num , 0);
        new_child.child.resize(new_child.key_num + 1, 0);
        
        let mut not_leaf_const = 0;
        if !new_child.leaf {
            not_leaf_const = 1;
        }

        for j in 0..new_child.key_num {
            new_child.keys[j] = self.nodes[child].keys[j + self.t + not_leaf_const];
            if new_child.leaf {
                new_child.pointers[j] = self.nodes[child].pointers[j + self.t + not_leaf_const];
            }
        }

        if !self.nodes[child].leaf {
            for j in 0..=new_child.key_num {
                new_child.child[j] = self.nodes[child].child[j + self.t + not_leaf_const];
            }
        }
        
        self.nodes[child].key_num = self.t;

        self.nodes[parent].child.push(0);

        for j in self.nodes[parent].key_num..i {
            self.nodes[parent].child[j + 1] = self.nodes[parent].child[j];
        }

        self.nodes.push(Box::new(new_child));
        self.nodes[parent].child[i + 1] = self.nodes.len() - 1;
        self.nodes[parent].keys.push(0);
        if i != self.nodes[parent].key_num {
            for j in self.nodes[parent].key_num-1..=i {
                self.nodes[parent].keys[j + 1] = self.nodes[parent].keys[j];
            }
        }
        self.nodes[parent].key_num += 1;
        self.nodes[parent].keys[i] = self.nodes[child].keys[self.t];
        let key_num = self.nodes[child].key_num;
        self.nodes[child].keys.resize(key_num, 0);
        self.nodes[child].pointers.resize(key_num, 0);
        self.nodes[child].child.resize(key_num + 1, 0);
        self
    }

    fn insert_helper(mut self, node: usize, key: usize, value: usize) -> BPlus {
        let mut i = 0;
        while i < self.nodes[node].key_num {
            if self.nodes[node].keys[i] > key {
                break;
            }
            i += 1;
        }
        if !self.nodes[node].leaf {
            let temp = self.nodes[node].child[i];
            self = self.insert_helper(temp, key, value);
            if self.nodes[temp].key_num == 2 * self.t {
                self = self.split_child(node, temp, i);
            }
        } else {
            self.nodes[node].keys.push(0);
            self.nodes[node].pointers.push(0);
            if self.nodes[node].key_num as i32 - 1 >= i as i32  {
                for j in self.nodes[node].key_num - 1..=i {
                    self.nodes[node].keys[j + 1] = self.nodes[node].keys[j];
                    self.nodes[node].pointers[j + 1] = self.nodes[node].pointers[j];
                }
            }

            self.nodes[node].keys[i] = key;
            self.nodes[node].pointers[i] = value;
            self.nodes[node].key_num += 1;
        }

        self
    }

    fn insert(mut self, key: usize, value: usize) -> BPlus {
        let root = self.root.unwrap();
        self = self.insert_helper(root, key, value);
        if self.nodes[root].key_num == 2 * self.t {
            let mut new_root = Self::create_new_node(false);
            new_root.child.push(root);
            self.nodes.push(Box::new(new_root));
            let new_root= self.nodes.len() - 1;
            self = Self::split_child(self, new_root, root, 0);
            self.root = Some(new_root);
        }

        self
    }

    fn print_subtree(&self, node: usize) {
        for i in 0..self.nodes[node].key_num {
            if self.nodes[node].leaf {
                print!("L");
            }
            print!("{} ", self.nodes[node].keys[i]);
        }

        println!();

        if !self.nodes[node].leaf {
            for i in 0..=self.nodes[node].key_num {
                self.print_subtree(self.nodes[node].child[i]);
            }
        }
    }

    fn delete(mut self, key: usize) -> BPlus {
        let root = self.root.unwrap();
        self = self.delete_helper(root, key);
        if self.nodes[root].key_num == 0 && !self.nodes[root].leaf {
            self.root = Some(self.nodes[root].child[0]);
        }

        self
    }

    fn delete_helper(mut self, node: usize, key: usize) -> BPlus {
        let mut i = 0;

        if self.nodes[node].leaf {
            while i < self.nodes[node].key_num {
                if self.nodes[node].keys[i] == key {
                    break;
                }
                i += 1;
            }
            if i != self.nodes[node].key_num {
                self.nodes[node].key_num -= 1;
                let key_num = self.nodes[node].key_num;
                for j in i..key_num {
                    self.nodes[node].keys[j] = self.nodes[node].keys[j + 1];
                    self.nodes[node].pointers[j] = self.nodes[node].pointers[j + 1];
                }
                self.nodes[node].keys.resize(key_num, 0);
                self.nodes[node].pointers.resize(key_num, 0);
            }
        } else {
            while i < self.nodes[node].key_num {
                if self.nodes[node].keys[i] > key {
                    break;
                }
                i += 1;
            }
            let temp = self.nodes[node].child[i];
            self = self.delete_helper(temp, key);
            if self.nodes[temp].key_num == self.t - 2 {
                if i != 0 {
                    if self.nodes[self.nodes[node].child[i - 1]].key_num == self.t - 1 {
                        let temp1 = self.nodes[node].child[i - 1];
                        let temp2 = self.nodes[node].child[i];
                        self = self.merge(temp1, temp2);
                        self.nodes[node].key_num -= 1;
                        let key_num = self.nodes[node].key_num;
                        for j in i..key_num{
                            self.nodes[node].keys[j] = self.nodes[node].keys[j + 1];
                            self.nodes[node].keys[j] = self.nodes[node].keys[j + 1];
                        }
                        self.nodes[node].keys.resize(key_num, 0);
                    } else {
                        let prev_child = self.nodes[node].child[i - 1];
                        let child = self.nodes[node].child[i];
                        let moved_key = self.nodes[prev_child].keys[self.nodes[prev_child].key_num - 1];
                        let moved_value = self.nodes[prev_child].pointers[self.nodes[prev_child].key_num - 1];
                        self = self.insert_helper(child, moved_key, moved_value);
                        self = self.delete_helper(prev_child, moved_key);
                        self.nodes[node].keys[i - 1] = self.nodes[child].keys[0];
                    }
                } else {
                    if self.nodes[self.nodes[node].child[i + 1]].key_num == self.t - 1 {
                        let temp1 = self.nodes[node].child[i + 1];
                        let temp2 = self.nodes[node].child[i];
                        self = self.merge(temp2, temp1);
                        self.nodes[node].key_num -= 1;
                        let key_num = self.nodes[node].key_num;
                        for j in 0..key_num{
                            self.nodes[node].keys[j] = self.nodes[node].keys[j + 1];
                            self.nodes[node].keys[j] = self.nodes[node].keys[j + 1];
                        }
                        self.nodes[node].keys.resize(key_num, 0);
                    } else {
                        let prev_child = self.nodes[node].child[i + 1];
                        let child = self.nodes[node].child[i];
                        let moved_key = self.nodes[prev_child].keys[0];
                        let moved_value = self.nodes[prev_child].pointers[0];
                        self = self.insert_helper(child, moved_key, moved_value);
                        self = self.delete_helper(prev_child, moved_key);
                        self.nodes[node].keys[i] = self.nodes[prev_child].keys[0];
                    }
                }

                if self.nodes[temp].key_num == 0 {
                    self.nodes[node].child[i] = self.nodes[temp].child[0];
                }
            }
        }
        self
    }

    fn merge(mut self, node1: usize, node2: usize) -> BPlus {
        let new_key_num = self.nodes[node1].key_num + self.nodes[node2].key_num;
        
        self.nodes[node1].keys.resize(new_key_num, 0);
        if self.nodes[node1].leaf {
            self.nodes[node1].pointers.resize(new_key_num, 0);
        } else {
            self.nodes[node1].child.resize(new_key_num + 2, 0);
        }
        for i in 0..self.nodes[node2].key_num {
            let temp = self.nodes[node1].key_num;
            self.nodes[node1].keys[temp + i] = self.nodes[node2].keys[i];
            if self.nodes[node1].leaf {
                self.nodes[node1].pointers[temp] = self.nodes[node2].pointers[i];
            } else {
                self.nodes[node1].child[temp + i + 1] = self.nodes[node2].child[i];
            }
        }

        if !self.nodes[node1].leaf {
            self.nodes[node1].child[new_key_num + 1] = self.nodes[node2].child[self.nodes[node2].key_num];
        }

        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insert_and_find() {
        let mut tree: BPlus = BPlus::new(4);
        tree = tree.insert(1, 1);
        assert_eq!(tree.find(1), Some(1));
    }

    #[test]
    fn test_insert_and_find_bunch_of_elements() {
        let mut tree: BPlus = BPlus::new(2);
        for i in 1..100 {
            tree = tree.insert(i, i);
        }
        
        for i in 1..100 {
            assert_eq!(tree.find(i), Some(i));
        }
    }

    #[test]
    fn test_insert_and_delete() {
        let mut tree: BPlus = BPlus::new(4);
        tree = tree.insert(1, 1);
        tree = tree.delete(1);
        assert!(tree.find(1).is_none());
    }

    #[test]
    fn test_insert_and_delete_bunch_of_elements() {
        let mut tree: BPlus = BPlus::new(2);
        for i in 1..100 {
            tree = tree.insert(i, i);
        }

        for i in 20..=30 {
            tree = tree.delete(i);
        }
        
        for i in 1..20 {
            assert_eq!(tree.find(i).unwrap(), i);
        }
        for i in 20..=30 {
            assert!(tree.find(i).is_none());
        }
        for i in 31..100 {
            assert_eq!(tree.find(i).unwrap(), i);
        }
    }
}