//! Output Formatting
//!
//! Formats RESP responses for display.

use super::client::RespResponse;
use super::colors::Colors;

/// Format a RESP response for display
pub fn format_response(response: &RespResponse, colors: &Colors) -> String {
    match response {
        RespResponse::Simple(s) => s.clone(),
        RespResponse::Error(s) => {
            format!("{}(error) {}{}", colors.red(), s, colors.reset())
        }
        RespResponse::Integer(n) => {
            format!("{}(integer) {}{}", colors.magenta(), n, colors.reset())
        }
        RespResponse::Bulk(s) => {
            format!("{}\"{}\"{}",  colors.green(), s, colors.reset())
        }
        RespResponse::Array(items) => format_array(items, colors, 0),
        RespResponse::Null => {
            format!("{}(nil){}", colors.yellow(), colors.reset())
        }
    }
}

fn format_array(items: &[RespResponse], colors: &Colors, indent: usize) -> String {
    if items.is_empty() {
        return "(empty array)".to_string();
    }

    let prefix = " ".repeat(indent);
    let mut result = String::new();

    for (i, item) in items.iter().enumerate() {
        let formatted = match item {
            RespResponse::Array(nested) => format_array(nested, colors, indent + 3),
            other => format_response(other, colors),
        };
        result.push_str(&format!("{}{}) {}\n", prefix, i + 1, formatted));
    }

    result.trim_end().to_string()
}

/// Format raw output (no colors, no prefixes)
pub fn format_raw(response: &RespResponse) -> String {
    match response {
        RespResponse::Simple(s) => s.clone(),
        RespResponse::Error(s) => s.clone(),
        RespResponse::Integer(n) => n.to_string(),
        RespResponse::Bulk(s) => s.clone(),
        RespResponse::Array(items) => {
            items.iter()
                .map(format_raw)
                .collect::<Vec<_>>()
                .join("\n")
        }
        RespResponse::Null => String::new(),
    }
}
