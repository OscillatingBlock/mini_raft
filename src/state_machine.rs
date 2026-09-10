use std::{collections::HashMap, sync::RwLock};
use tracing::{debug, info, instrument};

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
        info!(target: "state_machine", "initializing new simple state machine data store");
        Self {
            data_store: RwLock::new(HashMap::new()),
        }
    }
}

impl StateMachine for SimpleStateMachine {
    #[instrument(skip(self), fields(%key, %value))]
    fn apply_log(&self, key: String, value: String) -> anyhow::Result<()> {
        debug!(target: "state_machine", "applying log entry to state machine store");
        self.data_store
            .write()
            .unwrap()
            .insert(key.clone(), value.clone());
        info!(target: "state_machine", %key, %value, "successfully applied log entry and updated state");
        Ok(())
    }

    #[instrument(skip(self), fields(%key))]
    fn get_value(&self, key: String) -> Option<String> {
        debug!(target: "state_machine", "looking up key in state machine store");
        let store = self.data_store.read().unwrap();
        let val = store.get(&key).cloned();
        if let Some(ref v) = val {
            debug!(target: "state_machine", %key, value = %v, "key found in state machine store");
        } else {
            debug!(target: "state_machine", %key, "key not found in state machine store");
        }
        val
    }

    #[instrument(skip(self), fields(%key))]
    fn delete_key(&self, key: String) -> anyhow::Result<()> {
        debug!(target: "state_machine", "deleting key from state machine store");
        let removed = self.data_store.write().unwrap().remove(&key);
        if removed.is_some() {
            info!(target: "state_machine", %key, "successfully deleted key from state machine");
        } else {
            debug!(target: "state_machine", %key, "key attempted for deletion did not exist");
        }
        Ok(())
    }
}
