use std::net::TcpListener;
use std::sync::Arc;
use std::thread;
use parking_lot::Mutex;
use tracing::{info, error};
use tracing_subscriber;

use hexagondb::{database::DB, interpreter,connection};

fn main() -> std::io::Result<()>  {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_target(false)
        .compact()
        .init();
    
    let db: DB = DB::new();
    let db = Arc::new(Mutex::new(db));
    
    let listener = TcpListener::bind("127.0.0.1:2112")?;
    info!("HexagonDB server listening on 127.0.0.1:2112");
    
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let db_clone = Arc::clone(&db);
                
                // Spawn a new thread for each client connection
                thread::spawn(move || {
                    info!("New client connected");
                    let mut client = interpreter::Interpreter::new(db_clone);
                    connection::handle_client(stream, &mut client);
                    info!("Client disconnected");
                });
            }
            Err(e) => error!("Connection failed: {}", e),
        }
    }

    Ok(())
}
