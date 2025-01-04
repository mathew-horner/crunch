mod compaction;
mod engine;
mod env;
mod memtable;
mod segment;
mod sparse_index;
mod store;
#[cfg(test)]
mod test;
mod util;

use std::io::stdin;

use store::StoreArgs;

use crate::engine::{Engine, EngineArgs};
use crate::memtable::MemtableArgs;
use crate::util::Assignment;

fn main() {
    env_logger::init();
    let engine = Engine::new("test-db".into(), EngineArgs {
        memtable: MemtableArgs::from_env(),
        store: StoreArgs::from_env(),
    });
    db_client(engine);
}

fn db_client(mut engine: Engine) {
    println!("Kayvee");
    println!("The worst key-value store on the planet!");
    println!();
    println!("Here is how to use:");
    println!("SET key=value");
    println!("GET key");
    println!("DEL key");
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

                match command.as_str() {
                    "set" => match Assignment::parse(argument.as_str()) {
                        Ok(Assignment { key, value }) => engine.set(key, value),
                        Err(error) => println!("Error: {}", error),
                    },
                    "get" => match engine.get(argument.as_ref()) {
                        Some(value) => println!("{}", value),
                        None => println!("Error: Not found!"),
                    },
                    "del" => {
                        engine.delete(argument.as_ref());
                    },
                    _ => {
                        println!("Error: Invalid command!");
                    },
                };
            },
        };
    }
}
