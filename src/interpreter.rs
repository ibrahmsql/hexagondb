use std::sync::Arc;
use parking_lot::Mutex;
use tracing::{debug, warn};
use crate::{database::DB, parse_query};

pub struct Interpreter {
    db: Arc<Mutex<DB>>
}
impl Interpreter {
    pub fn new(db: Arc<Mutex<DB>>) -> Self {
        Interpreter {
            db,
        }
    }

    pub fn exec(&mut self, query: String) -> String {
        let tokens: Vec<String> = parse_query::parse_query(query.to_string());
        debug!("Executing command: {:?}", tokens);

        if let Some(cmd) = tokens.get(0).cloned() {
            if let Some(item) = tokens.get(1).cloned() {
                if cmd.to_uppercase() == "GET" {
                    let db = self.db.lock();
                    return match db.get(item) {
                        Some(value) => value,
                        None => String::from("(nil)"),
                    };
                } else if cmd.to_uppercase() == "SET" {
                    if let Some(value) = tokens.get(2).cloned() {
                        let mut db = self.db.lock();
                        db.set(item, value);
                        return String::from("+OK");
                    } else {
                        return String::from("-ERR wrong number of arguments for 'SET' command");
                    }
                } else if cmd.to_uppercase() == "DEL" {
                    let mut db = self.db.lock();
                    db.del(item);
                    return String::from(":1");
                } else if cmd.to_uppercase() == "EXISTS" {
                    let db = self.db.lock();
                    let exists = db.exists(item);
                    return if exists { String::from(":1") } else { String::from(":0") };
                }
            }
        }

        warn!("Unknown command: {:?}", tokens);
        String::from("-ERR unknown command")
    }
}

