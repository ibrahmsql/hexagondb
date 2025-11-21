use std::collections::HashMap;

pub struct DB{
    items: HashMap<String,String>
}

impl DB {
    pub fn new() -> Self {
        DB {
            items: HashMap::new(),
        }
    }

    pub fn get(&self, item: String) -> Option<String> {
        self.items.get(&item).cloned()
    }

    pub fn set(&mut self,item: String,value: String) {
        self.items.insert(item,value);
    }

    pub fn del(&mut self,item: String) {
        self.items.remove(&item);
    }

    pub fn exists(&self, item: String) -> bool {
        self.items.contains_key(&item)
    }
}