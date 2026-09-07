use std::{collections::HashMap, sync::RwLock};

pub trait StateMachine: Send + Sync {
    fn apply_log(&self, key: String, value: String) -> anyhow::Result<()>;
    fn get_value(&self, key: String) -> Option<String>;
    fn delete_key(&self, key: String) -> anyhow::Result<()>;
}

pub struct SimpleStateMachine {
    data_store: RwLock<HashMap<String, String>>,
}

impl SimpleStateMachine {
    pub fn new() -> Self {
        Self {
            data_store: RwLock::new(HashMap::new()),
        }
    }
}

impl StateMachine for SimpleStateMachine {
    fn apply_log(&self, key: String, value: String) -> anyhow::Result<()> {
        self.data_store.write().unwrap().insert(key, value);
        Ok(())
    }
    fn get_value(&self, key: String) -> Option<String> {
        self.data_store.read().unwrap().get(&key).cloned()
    }

    fn delete_key(&self, key: String) -> anyhow::Result<()> {
        self.data_store.write().unwrap().remove(&key);
        Ok(())
    }
}
