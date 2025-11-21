use std::net::TcpListener;
use std::sync::Arc;
use std::thread;
use parking_lot::Mutex;

use hexagondb::{database::DB, interpreter,connection};

fn main() -> std::io::Result<()>  {
    let db: DB = DB::new();
    let db = Arc::new(Mutex::new(db));
    
    let listener = TcpListener::bind("127.0.0.1:2112")?;
    println!("HexagonDB server listening on 127.0.0.1:2112");
    
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let db_clone = Arc::clone(&db);
                
                // Spawn a new thread for each client connection
                thread::spawn(move || {
                    println!("New client connected");
                    let mut client = interpreter::Interpreter::new(db_clone);
                    connection::handle_client(stream, &mut client);
                    println!("Client disconnected");
                });
            }
            Err(e) => println!("Connection failed: {}", e),
        }
    }

    Ok(())
}
