//! HexagonDB CLI - Main Entry Point
//!
//! Professional CLI client for HexagonDB.

use clap::Parser;
use hexagondb::cli::{
    client::RespClient,
    colors::Colors,
    config::CliArgs,
    repl::{run_command, run_interactive, run_pipe},
};

fn main() {
    let args = CliArgs::parse();
    let colors = Colors::new(!args.no_color);

    // Connect to server
    let client = match RespClient::connect(&args.host, args.port, args.timeout) {
        Ok(c) => c,
        Err(e) => {
            eprintln!(
                "{}Could not connect to HexagonDB at {}:{}: {}{}",
                colors.red(),
                args.host,
                args.port,
                e,
                colors.reset()
            );
            std::process::exit(1);
        }
    };

    let result = if args.pipe {
        run_pipe(client, &args)
    } else if let Some(ref cmd) = args.command {
        run_command(client, cmd, &args)
    } else {
        run_interactive(client, &args)
    };

    if let Err(e) = result {
        eprintln!("{}Error: {}{}", colors.red(), e, colors.reset());
        std::process::exit(1);
    }
}
