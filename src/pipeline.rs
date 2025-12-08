//! Pipeline support for batched command execution.
//!
//! Allows clients to send multiple commands without waiting for responses.

use std::collections::VecDeque;

/// Pipeline state for a client
#[derive(Debug, Clone, Default)]
pub struct Pipeline {
    /// Queued commands
    commands: VecDeque<PipelineCommand>,
    /// Whether pipeline mode is active
    active: bool,
}

/// A command in the pipeline
#[derive(Debug, Clone)]
pub struct PipelineCommand {
    pub command: String,
    pub args: Vec<String>,
}

/// Result of a pipelined command
#[derive(Debug, Clone)]
pub enum PipelineResult {
    Success(String),
    Error(String),
    Integer(i64),
    Bulk(Option<String>),
    Array(Vec<PipelineResult>),
    Null,
}

impl Pipeline {
    pub fn new() -> Self {
        Pipeline {
            commands: VecDeque::new(),
            active: false,
        }
    }

    /// Add a command to the pipeline
    pub fn queue(&mut self, command: String, args: Vec<String>) {
        self.commands.push_back(PipelineCommand { command, args });
        self.active = true;
    }

    /// Get all queued commands for execution
    pub fn flush(&mut self) -> Vec<PipelineCommand> {
        self.active = false;
        self.commands.drain(..).collect()
    }

    /// Check if pipeline has queued commands
    pub fn has_commands(&self) -> bool {
        !self.commands.is_empty()
    }

    /// Get number of queued commands
    pub fn len(&self) -> usize {
        self.commands.len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.commands.is_empty()
    }

    /// Clear the pipeline
    pub fn clear(&mut self) {
        self.commands.clear();
        self.active = false;
    }

    /// Check if pipeline is active
    pub fn is_active(&self) -> bool {
        self.active
    }
}

/// Pipeline manager for multiple clients
pub struct PipelineManager {
    /// Client ID -> Pipeline state
    pipelines: parking_lot::RwLock<std::collections::HashMap<String, Pipeline>>,
}

impl PipelineManager {
    pub fn new() -> Self {
        PipelineManager {
            pipelines: parking_lot::RwLock::new(std::collections::HashMap::new()),
        }
    }

    /// Get or create pipeline for a client
    pub fn get_or_create(&self, client_id: &str) -> Pipeline {
        let pipelines = self.pipelines.read();
        pipelines.get(client_id).cloned().unwrap_or_default()
    }

    /// Update pipeline for a client
    pub fn update(&self, client_id: &str, pipeline: Pipeline) {
        self.pipelines.write().insert(client_id.to_string(), pipeline);
    }

    /// Remove pipeline state for a client
    pub fn remove(&self, client_id: &str) {
        self.pipelines.write().remove(client_id);
    }

    /// Queue a command for a client
    pub fn queue(&self, client_id: &str, command: String, args: Vec<String>) {
        let mut pipelines = self.pipelines.write();
        let pipeline = pipelines.entry(client_id.to_string()).or_default();
        pipeline.queue(command, args);
    }

    /// Flush and get all commands for a client
    pub fn flush(&self, client_id: &str) -> Vec<PipelineCommand> {
        let mut pipelines = self.pipelines.write();
        if let Some(pipeline) = pipelines.get_mut(client_id) {
            pipeline.flush()
        } else {
            vec![]
        }
    }
}

impl Default for PipelineManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pipeline() {
        let mut pipeline = Pipeline::new();
        
        pipeline.queue("SET".to_string(), vec!["key1".to_string(), "value1".to_string()]);
        pipeline.queue("SET".to_string(), vec!["key2".to_string(), "value2".to_string()]);
        pipeline.queue("GET".to_string(), vec!["key1".to_string()]);
        
        assert_eq!(pipeline.len(), 3);
        assert!(pipeline.is_active());
        
        let commands = pipeline.flush();
        assert_eq!(commands.len(), 3);
        assert!(pipeline.is_empty());
        assert!(!pipeline.is_active());
    }

    #[test]
    fn test_pipeline_manager() {
        let manager = PipelineManager::new();
        
        manager.queue("client1", "PING".to_string(), vec![]);
        manager.queue("client1", "GET".to_string(), vec!["key".to_string()]);
        
        let commands = manager.flush("client1");
        assert_eq!(commands.len(), 2);
    }
}
