mod compaction;
mod database;
mod memtable;
mod segment;
mod sparse_index;
mod store;
mod util;

use store::StoreArgs;

use crate::{
    database::{Database, DatabaseArgs},
    memtable::MemtableArgs,
    util::{parse_assignment, Assignment},
};

use std::io::stdin;

fn main() {
    let database = Database::new(
        "~/.log-kv/mydb".into(),
        DatabaseArgs {
            memtable: MemtableArgs { capacity: 16 },
            store: StoreArgs {
                compaction_enabled: false,
                compaction_interval_seconds: 5,
            },
        },
    );

    db_client(database);
}

fn db_client(mut database: Database) {
    println!("Log-KV");
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
        stdin()
            .read_line(&mut command)
            .expect("Error: Failed to read command");

        let command = command.trim().to_lowercase();
        match command.as_str() {
            "exit" => {
                database.stop().unwrap();
                break;
            }
            _ => {
                let tokens = command
                    .split(" ")
                    .map(|t| t.to_string())
                    .collect::<Vec<String>>();

                if tokens.len() < 2 {
                    println!("Error: Invalid command format!");
                    continue;
                }

                let command = tokens[0].clone();
                let argument = tokens[1..].join(" ");

                match command.as_str() {
                    "set" => match parse_assignment(argument.as_str()) {
                        Ok(Assignment { key, value }) => database.set(key.as_str(), value.as_str()),
                        Err(error) => println!("Error: {}", error),
                    },
                    "get" => match database.get(argument.as_ref()) {
                        Some(value) => println!("{}", value),
                        None => println!("Error: Not found!"),
                    },
                    "del" => {
                        database.delete(argument.as_ref());
                    }
                    _ => {
                        println!("Error: Invalid command!");
                    }
                };
            }
        };
    }
}
