//! Terminal Colors
//!
//! ANSI color codes for terminal output.

/// Color codes for terminal output
pub struct Colors {
    pub enabled: bool,
}

impl Colors {
    pub fn new(enabled: bool) -> Self {
        Colors { enabled }
    }

    pub fn reset(&self) -> &'static str {
        if self.enabled { "\x1b[0m" } else { "" }
    }

    pub fn red(&self) -> &'static str {
        if self.enabled { "\x1b[31m" } else { "" }
    }

    pub fn green(&self) -> &'static str {
        if self.enabled { "\x1b[32m" } else { "" }
    }

    pub fn yellow(&self) -> &'static str {
        if self.enabled { "\x1b[33m" } else { "" }
    }

    pub fn blue(&self) -> &'static str {
        if self.enabled { "\x1b[34m" } else { "" }
    }

    pub fn magenta(&self) -> &'static str {
        if self.enabled { "\x1b[35m" } else { "" }
    }

    pub fn cyan(&self) -> &'static str {
        if self.enabled { "\x1b[36m" } else { "" }
    }

    pub fn bold(&self) -> &'static str {
        if self.enabled { "\x1b[1m" } else { "" }
    }

    pub fn dim(&self) -> &'static str {
        if self.enabled { "\x1b[2m" } else { "" }
    }
}

impl Default for Colors {
    fn default() -> Self {
        Colors::new(true)
    }
}
