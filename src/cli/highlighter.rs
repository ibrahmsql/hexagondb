//! Syntax Highlighting
//!
//! Color-codes commands as the user types.

use rustyline::highlight::Highlighter;
use std::borrow::Cow;

use super::completer::COMMANDS;

/// Syntax highlighter for commands
pub struct CommandHighlighter {
    pub enabled: bool,
}

impl Default for CommandHighlighter {
    fn default() -> Self {
        CommandHighlighter { enabled: true }
    }
}

impl Highlighter for CommandHighlighter {
    fn highlight<'l>(&self, line: &'l str, _pos: usize) -> Cow<'l, str> {
        if !self.enabled || line.is_empty() {
            return Cow::Borrowed(line);
        }

        let words: Vec<&str> = line.splitn(2, ' ').collect();
        let cmd = words[0].to_uppercase();
        
        // Check if it's a valid command
        let is_valid = COMMANDS.iter().any(|(c, _, _)| *c == cmd);
        
        if is_valid {
            // Green for valid command
            let mut result = format!("\x1b[32m{}\x1b[0m", words[0]);
            if words.len() > 1 {
                result.push(' ');
                result.push_str(&highlight_args(words[1]));
            }
            Cow::Owned(result)
        } else if !cmd.is_empty() && COMMANDS.iter().any(|(c, _, _)| c.starts_with(&cmd)) {
            // Yellow for partial match
            Cow::Owned(format!("\x1b[33m{}\x1b[0m", line))
        } else if !cmd.is_empty() {
            // Red for invalid
            Cow::Owned(format!("\x1b[31m{}\x1b[0m", line))
        } else {
            Cow::Borrowed(line)
        }
    }

    fn highlight_hint<'h>(&self, hint: &'h str) -> Cow<'h, str> {
        if !self.enabled {
            return Cow::Borrowed(hint);
        }
        Cow::Owned(format!("\x1b[2m{}\x1b[0m", hint))
    }
}

fn highlight_args(args: &str) -> String {
    let mut result = String::new();
    let mut in_quotes = false;
    let mut current = String::new();

    for c in args.chars() {
        match c {
            '"' => {
                if in_quotes {
                    current.push(c);
                    result.push_str(&format!("\x1b[33m{}\x1b[0m", current));
                    current.clear();
                    in_quotes = false;
                } else {
                    if !current.is_empty() {
                        result.push_str(&format!("\x1b[36m{}\x1b[0m", current));
                        current.clear();
                    }
                    current.push(c);
                    in_quotes = true;
                }
            }
            ' ' if !in_quotes => {
                if !current.is_empty() {
                    result.push_str(&format!("\x1b[36m{}\x1b[0m", current));
                    current.clear();
                }
                result.push(c);
            }
            _ => {
                current.push(c);
            }
        }
    }

    if !current.is_empty() {
        if in_quotes {
            result.push_str(&format!("\x1b[33m{}\x1b[0m", current));
        } else {
            result.push_str(&format!("\x1b[36m{}\x1b[0m", current));
        }
    }

    result
}
