//! Command Parser
//!
//! Parses command strings into structured commands.

/// Parse a command line into parts, respecting quotes
pub fn parse_command(input: &str) -> Vec<String> {
    let mut parts = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;
    let mut quote_char = '"';
    let mut escape_next = false;

    for c in input.chars() {
        if escape_next {
            current.push(c);
            escape_next = false;
            continue;
        }

        match c {
            '\\' => escape_next = true,
            '"' | '\'' if !in_quotes => {
                in_quotes = true;
                quote_char = c;
            }
            c if in_quotes && c == quote_char => {
                in_quotes = false;
            }
            ' ' | '\t' if !in_quotes => {
                if !current.is_empty() {
                    parts.push(current.clone());
                    current.clear();
                }
            }
            _ => current.push(c),
        }
    }

    if !current.is_empty() {
        parts.push(current);
    }

    parts
}

/// Check if a command is a special CLI command (not sent to server)
pub fn is_cli_command(cmd: &str) -> bool {
    matches!(
        cmd.to_lowercase().as_str(),
        "quit" | "exit" | "help" | "clear" | "history" | "?"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple() {
        let parts = parse_command("SET key value");
        assert_eq!(parts, vec!["SET", "key", "value"]);
    }

    #[test]
    fn test_parse_quoted() {
        let parts = parse_command(r#"SET key "hello world""#);
        assert_eq!(parts, vec!["SET", "key", "hello world"]);
    }

    #[test]
    fn test_parse_escaped() {
        let parts = parse_command(r#"SET key "hello\"world""#);
        assert_eq!(parts, vec!["SET", "key", "hello\"world"]);
    }
}
