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
    parent: Option<usize>,
    child: Vec<usize>,
    pointers: Vec<ValueType>,
    left: Option<usize>,
    right: Option<usize>,
}
#[allow(dead_code)]
impl BPlus {
    fn new(t: usize) -> Self {
        Self {t, root: None, nodes: Vec::new()}
    }

    fn find_leaf(&self, current_sub_tree: Option<usize>, key: usize) -> Option<usize> {
        match current_sub_tree {
            Some(x) => {
                let cur = &self.nodes[x];
                if !cur.leaf {
                    for i in 0..cur.key_num {
                        if i == cur.key_num || key < cur.keys[i as usize] {
                            Self::find_leaf(self, Some(i as usize), key)
                        } else {continue};
                    };
                };
                Some(x)
            },
            None => None,
        }
    }

    fn find(&self, key: usize) -> Option<ValueType> {
        let node = self.find_leaf(self.root, key);
        match node {
            Some(x)=>Some(self.nodes[x].pointers[self.find_key(x, key)]),
            None=>None
        }
    }

    fn insert(mut self, key: usize, value: ValueType) -> BPlus {
        let leaf = Self::find_leaf(&self, self.root, key).unwrap();
        let mut pos = 0;
        while pos < self.nodes[leaf].key_num && self.nodes[leaf].keys[pos] < key {
            pos += 1;
        }
        
        self.nodes[leaf].keys.push(0);
        self.nodes[leaf].pointers.push(0);

        for i in self.nodes[leaf].key_num..pos { 
            self.nodes[leaf].keys[i] = self.nodes[leaf].keys[i - 1];
            self.nodes[leaf].pointers[i] = self.nodes[leaf].pointers[i - 1];
        }
        self.nodes[leaf].keys[pos] = key;
        self.nodes[leaf].pointers[pos] = value;
        self.nodes[leaf].key_num += 1;
    
        if self.nodes[leaf].key_num == 2 * self.t {
            return Self::split(self, leaf);
        }
        self
    }
    
    fn split(mut self, node: usize) -> BPlus {
        let new_node = Box::new(Self::create_new_node());
        self.nodes.push(new_node);

        let new_node = self.nodes.len() - 1;
        
        self.nodes[new_node].right = self.nodes[node].right;
        if self.nodes[node].right.is_some() {
            let temp = self.nodes[node].right.unwrap();
            self.nodes[temp].left = Some(new_node);
        }

        self.nodes[node].right = Some(new_node);
        self.nodes[new_node].left = Some(node);

        let mid_key = self.nodes[node].keys[self.t];
        self.nodes[new_node].key_num = self.t - 1;
        self.nodes[node].key_num = self.t - 1;


        let temp = self.nodes[new_node].key_num;
        for i in 0..temp-1 {
            self.nodes[new_node].keys[i] = self.nodes[node].keys[i + self.t + 1];
            self.nodes[new_node].child[i] = self.nodes[node].child[i + self.t + 1];
            self.nodes[new_node].pointers[i] = self.nodes[node].pointers[i + self.t + 1];
        }
        
        self.nodes[new_node].child[temp + 1] = self.nodes[node].child[2 * self.t];

        if self.nodes[node].leaf {
            self.nodes[new_node].key_num += 1;
            self.nodes[new_node].keys.push(0);
            self.nodes[new_node].leaf = true;

            let temp = self.nodes[new_node].key_num;
            for i in temp - 1..1 {
                self.nodes[new_node].keys[i] = self.nodes[new_node].keys[i - 1];
                self.nodes[new_node].pointers[i] = self.nodes[new_node].pointers[i - 1];
            }
            self.nodes[new_node].keys[0] = self.nodes[node].keys[self.t];
            self.nodes[new_node].pointers[0] = self.nodes[node].pointers[self.t];
        }
        
        if self.root.unwrap() == node{
            let mut new_root = Self::create_new_node();
            new_root.keys[0] = mid_key;
            new_root.child.push(node);
            new_root.child.push(new_node);
            new_root.key_num = 1;
            self.nodes.push(Box::new(new_root));
            let new_root = self.nodes.len() - 1;
            self.root = Some(new_root);

            self.nodes[node].parent = Some(new_root);
            self.nodes[new_node].parent = Some(new_root);
        } else {
            self.nodes[new_node].parent = self.nodes[node].parent;

            let parent = self.nodes[node].parent.unwrap();

            let mut pos = 0;
            while pos < self.nodes[parent].key_num && self.nodes[parent].keys[pos] < mid_key {
                pos += 1;
            }

            self.nodes[parent].keys.push(0);
            self.nodes[parent].child.push(0);

            for i in self.nodes[parent].key_num..pos { 
                self.nodes[parent].keys[i] = self.nodes[parent].keys[i - 1];
            }
            for i in self.nodes[parent].key_num + 1..pos + 1 { 
                self.nodes[parent].child[i] = self.nodes[parent].child[i - 1];
            }
            self.nodes[parent].keys[pos] = mid_key;
            self.nodes[parent].child[pos + 1] = new_node;
            self.nodes[parent].key_num += 1;
            
            if self.nodes[parent].key_num == 2 * self.t {
                return Self::split(self, parent)
            }
        }
        self
    }

    fn create_new_node() -> Node { 
        let new_node = Node{leaf : false, key_num: 0, keys: Vec::new(), parent: None, child: Vec::new(), pointers: Vec::new(), left: None, right: None};
        new_node
    }

    fn get_predecessor(&self, node: usize, index: usize) -> usize
    {
        let mut curr = self.nodes[node].child[index];
        while !self.nodes[curr].leaf {
            curr = self.nodes[curr].child[self.nodes[curr].key_num];
        }
        self.nodes[self.nodes[curr].keys[curr] as usize].key_num - 1
    }

    fn find_key(&self, node: usize, key: usize) -> usize
    {
        let mut index = 0;
        while index < self.nodes[node].key_num && key > self.nodes[node].keys[index] {
            index += 1;
        }
        index
    }

    fn remove_from_leaf(mut self, node: usize, index: usize) -> BPlus
    {
        for i in index+1..self.nodes[node].key_num {
            self.nodes[node].keys[i - 1] = self.nodes[node].keys[i];
        }
        self.nodes[node].key_num -= 1;
        self
    }

    fn merge(mut self, node: usize, index: usize) -> BPlus
    {
        let child = self.nodes[node].child[index];
        let sibling = self.nodes[node].child[index + 1];

        let temp = self.nodes[child].key_num;
        self.nodes[child].keys[temp] = self.nodes[node].keys[index];

        if !self.nodes[child].leaf {
            let temp = self.nodes[child].key_num + 1;
            self.nodes[child].child[temp]
                = self.nodes[sibling].child[0];
        }

        for i in 0..self.nodes[sibling].key_num {
            let temp = i + self.nodes[child].key_num + 1;
            self.nodes[child].keys[temp] = self.nodes[sibling].keys[i];
        }
        for i in 0..self.nodes[sibling].key_num {
            let temp = i + self.nodes[child].key_num + 1;
            self.nodes[child].keys[temp] = self.nodes[sibling].keys[i];
        }

        if !self.nodes[child].leaf {
            for i in 0..self.nodes[sibling].key_num + 1 {
                let temp = i + self.nodes[child].key_num + 1;
                self.nodes[child].child[temp] = self.nodes[sibling].child[i];
            }
        }

        for i in index+1..self.nodes[node].key_num {
            self.nodes[node].keys[i - 1] = self.nodes[node].keys[i];
        }

        for i in index+2..self.nodes[node].key_num+1 {
            self.nodes[node].child[i - 1] = self.nodes[node].child[i];
        }

        self.nodes[child].key_num += self.nodes[sibling].key_num + 1;
        self.nodes[node].key_num -= 1;
        self
    }

    fn fill(self, node: usize, index: usize) -> BPlus
    {
        if index != 0 && self.nodes[self.nodes[node].child[index - 1]].key_num >= self.t {
            Self::borrow_from_prev(self, node, index)
        }
        else if index != self.nodes[node].key_num && self.nodes[self.nodes[node].child[index + 1]].key_num >= self.t {
            Self::borrow_from_next(self, node, index)
        }
        else {
            if index != self.nodes[node].key_num {
                Self::merge(self, node, index)
            }
            else {
                Self::merge(self, node, index - 1)
            }
        }
    }

    fn delete_key_helper(mut self, node: usize, key: usize) -> BPlus
    {
        let index = Self::find_key(&self, node, key);

        if index < self.nodes[node].key_num && self.nodes[node].keys[index] == key {
            if self.nodes[node].leaf {
                return Self::remove_from_leaf(self, node, index);
            }
            else {
                let predecessor = Self::get_predecessor(&self, node, index);
                self.nodes[node].keys[index] = predecessor;
                let temp = self.nodes[node].child[index];
                return Self::delete_key_helper(self, temp, predecessor);
            }
        } else {
            if self.nodes[node].leaf {
                return self;
            }

            let is_last_child = index == self.nodes[node].key_num;

            if self.nodes[self.nodes[node].child[index]].key_num < self.t {
                self = Self::fill(self, node, index);
            }
            let n = self.nodes[node].key_num;
            if is_last_child && index > n {
                let temp = self.nodes[node].child[index - 1];
                return Self::delete_key_helper(self, temp, key);
            }
            else {
                let temp = self.nodes[node].child[index];
                return Self::delete_key_helper(self, temp, key);
            }
        }
    }

    fn delete_key(mut self, key: usize)
    {
        let root = self.root.unwrap();
        self = Self::delete_key_helper(self, root, key);
        if self.nodes[root].key_num == 0 && !self.nodes[root].leaf {
            let new_root = self.nodes[root].child[0];
            self.root = Some(new_root);
        }
    }

    fn borrow_from_prev(mut self, node: usize, index: usize) -> BPlus {   
        let child = self.nodes[node].child[index];
        let sibling = self.nodes[node].child[index - 1];
        for i in self.nodes[child].key_num - 1..=0 {
            self.nodes[child].keys[i + 1] = self.nodes[child].keys[i];
        }

        if !self.nodes[child].leaf {
            for i in self.nodes[child].key_num..=0 {
                self.nodes[child].child[i + 1] = self.nodes[child].child[i];
            }
        }

        self.nodes[child].keys[0] = self.nodes[node].keys[index - 1];

        if !self.nodes[child].leaf {
            self.nodes[child].child[0] = self.nodes[sibling].child[self.nodes[sibling].key_num];
        }

        self.nodes[node].keys[index - 1] = self.nodes[sibling].keys[self.nodes[sibling].key_num - 1];

        self.nodes[child].key_num += 1;
        self.nodes[sibling].key_num -= 1;
        self
    }

    #[allow(dead_code)]
    fn borrow_from_next(mut self, node: usize, index: usize) -> BPlus
    {
        let child = self.nodes[node].child[index];
        let sibling = self.nodes[node].child[index + 1];

        let temp = self.nodes[child].key_num;
        self.nodes[child].keys[temp] = self.nodes[node].keys[index];

        if !self.nodes[child].leaf {
            let temp = self.nodes[child].key_num + 1;
            self.nodes[child].child[temp]
                = self.nodes[sibling].child[0];
        }

        self.nodes[node].keys[index] = self.nodes[sibling].keys[0];

        for i in 1..self.nodes[sibling].key_num {
            self.nodes[sibling].keys[i - 1] = self.nodes[sibling].keys[i];
        }

        if !self.nodes[sibling].leaf {
            for i in 1..=self.nodes[sibling].key_num {
                self.nodes[sibling].child[i - 1] = self.nodes[sibling].child[i];
            }
        }

        self.nodes[child].key_num += 1;
        self.nodes[sibling].key_num -= 1;
        self
    }
}

#[cfg(test)]
mod tests {
    // Note this useful idiom: importing names from outer (for mod tests) scope.
    use super::*;

    #[test]
    fn test_insert_and_find() {
        let mut tree: BPlus = BPlus::new(2);
        tree = tree.insert(1, 1);
        assert_eq!(tree.find(1), Some(1));
    }
}
