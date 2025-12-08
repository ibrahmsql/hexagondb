//! Advanced REPL
//!
//! Interactive shell with vim mode, auto-complete, and hints.

use std::io::{self, BufRead};

use rustyline::error::ReadlineError;
use rustyline::history::DefaultHistory;
use rustyline::validate::MatchingBracketValidator;
use rustyline::{Completer, Editor, Helper, Highlighter, Hinter, Validator, Config, EditMode};

use super::client::RespClient;
use super::colors::Colors;
use super::commands::{clear_screen, print_help};
use super::completer::{CommandCompleter, get_command_help, COMMANDS};
use super::config::CliArgs;
use super::highlighter::CommandHighlighter;
use super::hinter::CommandHinter;
use super::output::{format_raw, format_response};
use super::parser::parse_command;

/// Combined helper for rustyline
#[derive(Completer, Helper, Highlighter, Hinter, Validator)]
pub struct CliHelper {
    #[rustyline(Completer)]
    completer: CommandCompleter,
    #[rustyline(Highlighter)]
    highlighter: CommandHighlighter,
    #[rustyline(Hinter)]
    hinter: CommandHinter,
    #[rustyline(Validator)]
    validator: MatchingBracketValidator,
}

impl CliHelper {
    pub fn new(colors_enabled: bool) -> Self {
        CliHelper {
            completer: CommandCompleter,
            highlighter: CommandHighlighter { enabled: colors_enabled },
            hinter: CommandHinter,
            validator: MatchingBracketValidator::new(),
        }
    }
}

/// Get history file path
fn history_path() -> Option<std::path::PathBuf> {
    dirs::data_dir().map(|p| p.join("hexagondb").join("cli_history"))
}

/// Run interactive REPL with vim mode
pub fn run_interactive(mut client: RespClient, args: &CliArgs) -> io::Result<()> {
    let colors = Colors::new(!args.no_color);

    // Configure rustyline
    let config = Config::builder()
        .history_ignore_space(true)
        .completion_type(rustyline::CompletionType::List)
        .edit_mode(EditMode::Vi)  // Vim mode!
        .auto_add_history(true)
        .max_history_size(10000).unwrap()
        .build();

    let helper = CliHelper::new(!args.no_color);
    let mut rl: Editor<CliHelper, DefaultHistory> = Editor::with_config(config).unwrap();
    rl.set_helper(Some(helper));

    // Load history
    if let Some(path) = history_path() {
        if let Some(parent) = path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        let _ = rl.load_history(&path);
    }

    // Authenticate if password provided
    if let Some(ref password) = args.password {
        let response = client.send_command(&["AUTH", password])?;
        if response.is_error() {
            println!(
                "{}Authentication failed: {:?}{}",
                colors.red(),
                response,
                colors.reset()
            );
            return Ok(());
        }
        println!("{}OK{}", colors.green(), colors.reset());
    }

    // Main REPL loop
    let prompt = format!("{}:{} > ", args.host, args.port);

    loop {
        match rl.readline(&prompt) {
            Ok(line) => {
                let input = line.trim();
                if input.is_empty() {
                    continue;
                }

                // Handle special CLI commands (vim-like hidden commands with :)
                if input.starts_with(':') {
                    if handle_vim_command(input, &colors) {
                        continue;
                    }
                }

                // Handle regular special commands
                match input.to_lowercase().as_str() {
                    "quit" | "exit" => break,
                    "help" | "?" => {
                        print_help(&colors);
                        continue;
                    }
                    "clear" => {
                        clear_screen();
                        continue;
                    }
                    _ if input.to_lowercase().starts_with("help ") => {
                        // Help for specific command
                        let cmd = &input[5..].trim();
                        if let Some(help) = get_command_help(cmd) {
                            println!("{}{}{}", colors.cyan(), help, colors.reset());
                        } else {
                            println!("{}Unknown command: {}{}", colors.red(), cmd, colors.reset());
                        }
                        continue;
                    }
                    _ => {}
                }

                // Parse and send command
                let parts = parse_command(input);
                if parts.is_empty() {
                    continue;
                }

                let refs: Vec<&str> = parts.iter().map(|s| s.as_str()).collect();

                match client.send_command(&refs) {
                    Ok(response) => {
                        let output = if args.raw {
                            format_raw(&response)
                        } else {
                            format_response(&response, &colors)
                        };
                        println!("{}", output);
                    }
                    Err(e) => {
                        println!("{}Error: {}{}", colors.red(), e, colors.reset());
                        // Try to reconnect
                        println!("{}Reconnecting...{}", colors.yellow(), colors.reset());
                        match RespClient::connect(&args.host, args.port, args.timeout) {
                            Ok(new_client) => {
                                client = new_client;
                                println!("{}OK{}", colors.green(), colors.reset());
                            }
                            Err(e) => {
                                println!(
                                    "{}Failed: {}{}",
                                    colors.red(),
                                    e,
                                    colors.reset()
                                );
                                break;
                            }
                        }
                    }
                }
            }
            Err(ReadlineError::Interrupted) => {
                // Ctrl-C
                println!("^C");
                continue;
            }
            Err(ReadlineError::Eof) => {
                // Ctrl-D
                break;
            }
            Err(err) => {
                println!("{}Error: {:?}{}", colors.red(), err, colors.reset());
                break;
            }
        }
    }

    // Save history
    if let Some(path) = history_path() {
        let _ = rl.save_history(&path);
    }

    Ok(())
}

/// Handle vim-like hidden commands
fn handle_vim_command(input: &str, colors: &Colors) -> bool {
    let cmd = &input[1..]; // Remove leading :
    
    match cmd.to_lowercase().as_str() {
        "q" | "q!" | "quit" | "exit" => {
            std::process::exit(0);
        }
        "w" | "write" => {
            println!("{}No buffer to save{}", colors.yellow(), colors.reset());
            true
        }
        "wq" => {
            std::process::exit(0);
        }
        "clear" | "cls" => {
            clear_screen();
            true
        }
        "help" | "h" => {
            print_vim_help(colors);
            true
        }
        "commands" | "cmds" => {
            print_commands(colors);
            true
        }
        "set" => {
            println!("{}Options: no_color, raw{}", colors.cyan(), colors.reset());
            true
        }
        "version" | "ver" => {
            println!("{}HexagonDB CLI v0.1.0{}", colors.cyan(), colors.reset());
            true
        }
        _ if cmd.starts_with("search ") || cmd.starts_with("s ") => {
            let query = cmd.split_whitespace().skip(1).collect::<Vec<_>>().join(" ");
            search_commands(&query, colors);
            true
        }
        _ => {
            println!("{}Unknown command: :{}{}", colors.red(), cmd, colors.reset());
            true
        }
    }
}

/// Print vim-like help
fn print_vim_help(colors: &Colors) {
    println!("{}Vim-like Commands:{}", colors.bold(), colors.reset());
    println!("  :q, :quit     - Exit CLI");
    println!("  :clear, :cls  - Clear screen");
    println!("  :help, :h     - Show this help");
    println!("  :commands     - List all commands");
    println!("  :search <q>   - Search commands");
    println!("  :version      - Show version");
    println!();
    println!("{}Editing:{}", colors.bold(), colors.reset());
    println!("  i             - Insert mode");
    println!("  ESC           - Normal mode");
    println!("  Ctrl+R        - Search history");
    println!("  Tab           - Auto-complete");
    println!("  Ctrl+C        - Cancel input");
    println!("  Ctrl+D        - Exit");
}

/// List all commands grouped by category
fn print_commands(colors: &Colors) {
    let categories = [
        "String", "List", "Hash", "Set", "Sorted Set", 
        "Bitmap", "Stream", "Geo", "HyperLogLog",
        "Key", "Server", "Transaction", "Pub/Sub", "Replication"
    ];
    
    for category in &categories {
        let cmds: Vec<_> = COMMANDS
            .iter()
            .filter(|(c, _, _)| {
                match *category {
                    "String" => ["GET", "SET", "APPEND", "INCR", "DECR", "MGET", "MSET"].contains(c),
                    "Key" => ["DEL", "EXISTS", "EXPIRE", "TTL", "KEYS", "SCAN", "TYPE"].contains(c),
                    "Transaction" => ["MULTI", "EXEC", "DISCARD", "WATCH"].contains(c),
                    _ => false
                }
            })
            .map(|(c, _, _)| *c)
            .collect();
        
        if !cmds.is_empty() {
            println!("{}{}:{}", colors.yellow(), category, colors.reset());
            for chunk in cmds.chunks(8) {
                println!("  {}", chunk.join(", "));
            }
            println!();
        }
    }
}

/// Search commands
fn search_commands(query: &str, colors: &Colors) {
    let query = query.to_lowercase();
    let matches: Vec<_> = COMMANDS
        .iter()
        .filter(|(cmd, args, desc)| {
            cmd.to_lowercase().contains(&query) 
            || args.to_lowercase().contains(&query)
            || desc.to_lowercase().contains(&query)
        })
        .collect();

    if matches.is_empty() {
        println!("{}No matching commands{}", colors.yellow(), colors.reset());
    } else {
        for (cmd, args, desc) in matches {
            println!(
                "{}{}{} {} - {}",
                colors.green(),
                cmd,
                colors.reset(),
                args,
                desc
            );
        }
    }
}

/// Run a single command
pub fn run_command(mut client: RespClient, command: &str, args: &CliArgs) -> io::Result<()> {
    let colors = Colors::new(!args.no_color);

    // Authenticate if password provided
    if let Some(ref password) = args.password {
        let response = client.send_command(&["AUTH", password])?;
        if response.is_error() {
            eprintln!("Authentication failed");
            std::process::exit(1);
        }
    }

    for i in 0..args.repeat {
        if i > 0 && args.interval > 0.0 {
            std::thread::sleep(std::time::Duration::from_secs_f64(args.interval));
        }

        let parts = parse_command(command);
        if parts.is_empty() {
            continue;
        }

        let refs: Vec<&str> = parts.iter().map(|s| s.as_str()).collect();

        match client.send_command(&refs) {
            Ok(response) => {
                let output = if args.raw {
                    format_raw(&response)
                } else {
                    format_response(&response, &colors)
                };
                println!("{}", output);
            }
            Err(e) => {
                eprintln!("Error: {}", e);
                std::process::exit(1);
            }
        }
    }

    Ok(())
}

/// Run in pipe mode (read commands from stdin)
pub fn run_pipe(mut client: RespClient, args: &CliArgs) -> io::Result<()> {
    let colors = Colors::new(!args.no_color);

    // Authenticate if password provided
    if let Some(ref password) = args.password {
        let response = client.send_command(&["AUTH", password])?;
        if response.is_error() {
            eprintln!("Authentication failed");
            std::process::exit(1);
        }
    }

    let stdin = io::stdin();
    for line in stdin.lock().lines() {
        let line = line?;
        let line = line.trim();

        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        let parts = parse_command(line);
        if parts.is_empty() {
            continue;
        }

        let refs: Vec<&str> = parts.iter().map(|s| s.as_str()).collect();

        match client.send_command(&refs) {
            Ok(response) => {
                if args.verbose {
                    println!("> {}", line);
                }
                let output = if args.raw {
                    format_raw(&response)
                } else {
                    format_response(&response, &colors)
                };
                println!("{}", output);
            }
            Err(e) => {
                eprintln!("Error: {}", e);
            }
        }
    }

    Ok(())
}
