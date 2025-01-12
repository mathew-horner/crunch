use std::io::stdin;

use crunch_engine::engine::{Engine, EngineArgs};
use crunch_engine::error::Error;
use crunch_engine::memtable::MemtableArgs;
use crunch_engine::store::StoreArgs;
use crunch_engine::util::Assignment;

fn main() {
    env_logger::init();
    let mut engine = Engine::new("test-db".into(), EngineArgs {
        memtable: MemtableArgs::from_env(),
        store: StoreArgs::from_env(),
    })
    .unwrap();

    println!("Crunch");
    println!("The worst key-value store on the planet!");
    println!();
    println!("Here is how to use:");
    println!("SET key=value");
    println!("GET key");
    println!("DEL key");
    println!("INSPECT-SEGMENT segment");
    println!("EXIT");
    println!();
    println!("That's it - Have fun!");

    loop {
        let mut command = String::new();
        stdin().read_line(&mut command).expect("Error: Failed to read command");

        let command = command.trim().to_lowercase();
        match command.as_str() {
            "exit" => {
                engine.stop().unwrap();
                break;
            },
            _ => {
                let tokens = command.split(" ").map(|t| t.to_string()).collect::<Vec<String>>();

                if tokens.len() < 2 {
                    println!("Error: Invalid command format!");
                    continue;
                }

                let command = tokens[0].clone();
                let argument = tokens[1..].join(" ");

                fn exec(f: impl FnOnce() -> Result<(), Error>) {
                    if let Err(err) = f() {
                        error(err.to_string());
                    }
                }

                fn error(message: impl AsRef<str>) {
                    println!("Error: {}", message.as_ref());
                }

                match command.as_str() {
                    "set" => {
                        exec(|| {
                            let assignment = Assignment::parse(argument.as_str())?;
                            engine.set(assignment.key, assignment.value)
                        });
                    },
                    "get" => match engine.get(argument.as_ref()) {
                        Ok(Some(value)) => println!("{}", value),
                        Ok(None) => error("Not found"),
                        Err(err) => error(err.to_string()),
                    },
                    "del" => exec(|| engine.delete(argument.as_ref())),
                    "inspect-segment" => exec(|| engine.store().inspect_segment(argument.as_ref())),
                    _ => error("Invalid command"),
                };
            },
        };
    }
}
