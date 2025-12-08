//! Pub/Sub implementation.
//!
//! Provides publish/subscribe messaging between clients.
//! Supports both channel subscriptions and pattern-based subscriptions.

use std::collections::HashMap;
use tokio::sync::{broadcast, RwLock};

/// Pub/Sub manager
pub struct PubSub {
    /// Channel subscribers
    channels: RwLock<HashMap<String, broadcast::Sender<String>>>,
    /// Pattern subscribers (glob patterns)
    patterns: RwLock<HashMap<String, broadcast::Sender<(String, String)>>>,
}

impl PubSub {
    /// Create a new PubSub manager
    pub fn new() -> Self {
        PubSub {
            channels: RwLock::new(HashMap::new()),
            patterns: RwLock::new(HashMap::new()),
        }
    }

    /// Subscribe to a channel
    pub async fn subscribe(&self, channel: &str) -> broadcast::Receiver<String> {
        let mut channels = self.channels.write().await;
        
        let sender = channels.entry(channel.to_string()).or_insert_with(|| {
            let (tx, _) = broadcast::channel(1000);
            tx
        });
        
        sender.subscribe()
    }

    /// Subscribe to a pattern (glob-style: *, ?, [abc])
    pub async fn psubscribe(&self, pattern: &str) -> broadcast::Receiver<(String, String)> {
        let mut patterns = self.patterns.write().await;
        
        let sender = patterns.entry(pattern.to_string()).or_insert_with(|| {
            let (tx, _) = broadcast::channel(1000);
            tx
        });
        
        sender.subscribe()
    }

    /// Publish a message to a channel
    /// Returns total number of subscribers that received the message (including pattern subscribers)
    pub async fn publish(&self, channel: &str, message: &str) -> usize {
        let mut count = 0;
        
        // Send to direct channel subscribers
        {
            let channels = self.channels.read().await;
            if let Some(sender) = channels.get(channel) {
                count += sender.send(message.to_string()).unwrap_or(0);
            }
        }
        
        // Send to pattern subscribers
        {
            let patterns = self.patterns.read().await;
            for (pattern, sender) in patterns.iter() {
                if glob_match(pattern, channel) {
                    count += sender.send((channel.to_string(), message.to_string())).unwrap_or(0);
                }
            }
        }
        
        count
    }

    /// Unsubscribe from a channel (removes the channel if no subscribers remain)
    pub async fn unsubscribe(&self, channel: &str) {
        let mut channels = self.channels.write().await;
        if let Some(sender) = channels.get(channel) {
            // Only remove if no subscribers
            if sender.receiver_count() == 0 {
                channels.remove(channel);
            }
        }
    }

    /// Unsubscribe from a pattern
    pub async fn punsubscribe(&self, pattern: &str) {
        let mut patterns = self.patterns.write().await;
        if let Some(sender) = patterns.get(pattern) {
            if sender.receiver_count() == 0 {
                patterns.remove(pattern);
            }
        }
    }

    /// Get number of subscribers for a channel
    pub async fn numsub(&self, channel: &str) -> usize {
        let channels = self.channels.read().await;
        
        if let Some(sender) = channels.get(channel) {
            sender.receiver_count()
        } else {
            0
        }
    }

    /// Get list of active channels
    pub async fn channels(&self, pattern: Option<&str>) -> Vec<String> {
        let channels = self.channels.read().await;
        
        channels.keys()
            .filter(|ch| {
                if let Some(p) = pattern {
                    glob_match(p, ch)
                } else {
                    true
                }
            })
            .cloned()
            .collect()
    }

    /// Get number of pattern subscriptions
    pub async fn numpat(&self) -> usize {
        let patterns = self.patterns.read().await;
        patterns.values().map(|s| s.receiver_count()).sum()
    }

    /// Get list of active patterns
    pub async fn patterns(&self) -> Vec<String> {
        let patterns = self.patterns.read().await;
        patterns.keys().cloned().collect()
    }
}

impl Default for PubSub {
    fn default() -> Self {
        Self::new()
    }
}

/// Simple glob pattern matching for Pub/Sub patterns
fn glob_match(pattern: &str, text: &str) -> bool {
    let mut pattern_chars = pattern.chars().peekable();
    let mut text_chars = text.chars().peekable();

    while pattern_chars.peek().is_some() || text_chars.peek().is_some() {
        match pattern_chars.peek() {
            Some('*') => {
                pattern_chars.next();
                if pattern_chars.peek().is_none() {
                    return true;
                }
                while text_chars.peek().is_some() {
                    let remaining_pattern: String = pattern_chars.clone().collect();
                    let remaining_text: String = text_chars.clone().collect();
                    if glob_match(&remaining_pattern, &remaining_text) {
                        return true;
                    }
                    text_chars.next();
                }
                return false;
            }
            Some('?') => {
                pattern_chars.next();
                if text_chars.next().is_none() {
                    return false;
                }
            }
            Some('[') => {
                pattern_chars.next();
                let mut chars_in_bracket = Vec::new();
                let mut negated = false;
                
                if pattern_chars.peek() == Some(&'!') || pattern_chars.peek() == Some(&'^') {
                    negated = true;
                    pattern_chars.next();
                }
                
                while let Some(&c) = pattern_chars.peek() {
                    if c == ']' {
                        pattern_chars.next();
                        break;
                    }
                    chars_in_bracket.push(c);
                    pattern_chars.next();
                }
                
                let text_char = match text_chars.next() {
                    Some(c) => c,
                    None => return false,
                };
                
                let matches = chars_in_bracket.contains(&text_char);
                if (negated && matches) || (!negated && !matches) {
                    return false;
                }
            }
            Some(pc) => {
                if Some(*pc) != text_chars.next() {
                    return false;
                }
                pattern_chars.next();
            }
            None => {
                return text_chars.peek().is_none();
            }
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_glob_match_patterns() {
        assert!(glob_match("*", "anything"));
        assert!(glob_match("news.*", "news.sport"));
        assert!(glob_match("news.*", "news.weather"));
        assert!(!glob_match("news.*", "sport.news"));
        assert!(glob_match("user:*:messages", "user:123:messages"));
        assert!(glob_match("h?llo", "hello"));
        assert!(glob_match("h[ae]llo", "hello"));
        assert!(glob_match("h[ae]llo", "hallo"));
        assert!(!glob_match("h[ae]llo", "hillo"));
    }
}
