//! Command Hints
//!
//! Provides inline hints as the user types.

use rustyline::hint::{Hint, Hinter};
use rustyline::Context;

use super::completer::COMMANDS;

/// Command hinter - shows usage hints inline
pub struct CommandHinter;

impl Hinter for CommandHinter {
    type Hint = CommandHint;

    fn hint(&self, line: &str, pos: usize, _ctx: &Context<'_>) -> Option<Self::Hint> {
        if line.is_empty() || pos < line.len() {
            return None;
        }

        let line = line.trim();
        let words: Vec<&str> = line.split_whitespace().collect();
        
        if words.is_empty() {
            return None;
        }

        let cmd = words[0].to_uppercase();
        
        // Find matching command
        for (command, args, _) in COMMANDS {
            if *command == cmd {
                // Show remaining arguments
                let num_args_provided = words.len() - 1;
                let arg_parts: Vec<&str> = args.split_whitespace().collect();
                
                if num_args_provided < arg_parts.len() {
                    let remaining: Vec<&str> = arg_parts.into_iter().skip(num_args_provided).collect();
                    let hint = format!(" {}", remaining.join(" "));
                    return Some(CommandHint { 
                        text: hint,
                        complete_up_to: 0,
                    });
                }
                return None;
            }
        }

        // Partial command match for completion hint
        if words.len() == 1 && !line.ends_with(' ') {
            for (command, args, _) in COMMANDS {
                if command.starts_with(&cmd) && *command != cmd {
                    let hint = format!("{} {}", &command[cmd.len()..], args);
                    return Some(CommandHint { 
                        text: hint,
                        complete_up_to: command.len() - cmd.len(),
                    });
                }
            }
        }

        None
    }
}

/// A hint with display text
pub struct CommandHint {
    text: String,
    complete_up_to: usize,
}

impl Hint for CommandHint {
    fn display(&self) -> &str {
        &self.text
    }

    fn completion(&self) -> Option<&str> {
        if self.complete_up_to > 0 {
            Some(&self.text[..self.complete_up_to])
        } else {
            None
        }
    }
}
