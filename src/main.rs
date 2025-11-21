use std::net::TcpListener;
use std::sync::Arc;
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
                println!("New client connected");
                let db_clone = Arc::clone(&db);
                let mut client = interpreter::Interpreter::new(db_clone);
                connection::handle_client(stream, &mut client);
            }
            Err(e) => println!("Connection failed: {}", e),
        }
    }

    Ok(())
}
