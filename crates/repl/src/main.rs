use std::io::{stdin, Write};

use anyhow::anyhow;
use crunch_engine::engine::Engine;

enum Command {
    Set { key: String, value: String },
    Get { key: String },
    Delete { key: String },
    List,
    SegmentList,
    SegmentInspect { segment_file: String },
    Exit,
}

impl Command {
    /// Parse a [`Command`] from the given REPL input from the user.
    fn parse(input: &str) -> anyhow::Result<Self> {
        let input = input.trim().to_lowercase();
        let tokens: Vec<_> = input.split(" ").collect();
        match (tokens[0], tokens.len() - 1) {
            // TODO: It's probably best UX to have this parse from key=value, but this is just
            // easier for now.
            ("set", 2) => {
                Ok(Command::Set { key: tokens[1].to_owned(), value: tokens[2].to_owned() })
            },
            ("get", 1) => Ok(Command::Get { key: tokens[1].to_owned() }),
            ("del", 1) => Ok(Command::Delete { key: tokens[1].to_owned() }),
            ("list", 0) => Ok(Command::List),
            ("segment-list", 0) => Ok(Command::SegmentList),
            ("segment-inspect", 1) => {
                Ok(Command::SegmentInspect { segment_file: tokens[1].to_owned() })
            },
            ("exit", 0) => Ok(Command::Exit),
            _ => Err(anyhow!("invalid command")),
        }
    }

    /// Execute this command against the database `engine`.
    fn execute(&self, engine: &mut Engine) -> anyhow::Result<()> {
        match self {
            Self::Set { key, value } => engine.set(key, value)?,
            Self::Get { key } => match engine.get(key) {
                Ok(Some(value)) => println!("{value}"),
                Ok(None) => return Err(anyhow!("not found")),
                Err(error) => return Err(error.into()),
            },
            Self::Delete { key } => engine.delete(key)?,
            Self::List => engine.list()?.into_iter().for_each(|key| println!("{key}")),
            Self::SegmentList => engine
                .store()
                .list_segments()?
                .into_iter()
                .for_each(|segment| println!("{segment:?}")),
            Self::SegmentInspect { segment_file } => {
                engine.store().inspect_segment(segment_file)?;
            },
            // Exit will be handled by caller due to `Engine` ownership requirement.
            Self::Exit => {},
        }
        Ok(())
    }
}

fn main() {
    env_logger::init();
    let mut engine = Engine::new("test-db".into()).unwrap();

    println!("Crunch");
    println!("The worst key-value store on the planet!");
    println!();
    println!("Here is how to use:");
    println!("SET key value");
    println!("GET key");
    println!("DEL key");
    println!("LIST");
    println!("SEGMENT-LIST");
    println!("SEGMENT-INSPECT segment");
    println!("EXIT");
    println!();
    println!("That's it - Have fun!");

    loop {
        let mut command = String::new();
        print!("> ");
        std::io::stdout().flush().unwrap();
        stdin().read_line(&mut command).expect("failed to read command");
        let command = match Command::parse(&command) {
            Ok(command) => command,
            Err(error) => {
                println!("error: {error}");
                continue;
            },
        };
        if let Err(error) = command.execute(&mut engine) {
            println!("error: {error}");
        }
        if matches!(command, Command::Exit) {
            engine.stop().unwrap();
            break;
        }
    }
}
