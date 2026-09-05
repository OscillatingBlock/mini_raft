use crate::server::LogEntry;

pub trait StateMachine: Send + Sync {
    fn get_last_log_index(&self) -> u64;
    //TODO: make log entry type
    fn apply_log(&self, log_entry: LogEntry);
}

pub struct MockStateMachine {
    last_log_index: u64,
}

impl MockStateMachine {
    pub fn new() -> Self {
        Self { last_log_index: 0 }
    }
}

impl StateMachine for MockStateMachine {
    fn get_last_log_index(&self) -> u64 {
        self.last_log_index
    }

    fn apply_log(&self, log_entry: LogEntry) {
        //     self.last_log_index += 1;
        //     println!("Applying log entry: {:?}", log_entry);
    }
}

struct SimpleStateMachine {}
