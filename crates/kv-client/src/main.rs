use std::fmt::Display;
use std::io::Write;
use std::net::TcpStream;

use clap::Parser;
use nom::branch::alt;
use nom::bytes::complete::{is_not, tag, tag_no_case};
use nom::character::complete::space1;
use nom::sequence::separated_pair;
use nom::IResult;

mod protocol;

/// Command line client for CrunchKV
#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    /// The server port
    #[arg(short, long)]
    port: Option<u16>,
}

enum Command<'a> {
    Get { key: &'a str },
    Set { key: &'a str, value: &'a str },
    Delete { key: &'a str },
    Exit,
}

impl<'a> Command<'a> {
    fn parse(input: &'a str) -> Self {
        alt((parse_get, parse_set, parse_delete, parse_exit))(input).unwrap().1
    }
}

fn parse_get(input: &str) -> IResult<&str, Command> {
    let (rest, _) = tag_no_case("get")(input)?;
    let (rest, _) = space1(rest)?;
    Ok(("", Command::Get { key: rest.trim() }))
}

fn parse_set(input: &str) -> IResult<&str, Command> {
    let (rest, _) = tag_no_case("set")(input)?;
    let (rest, _) = space1(rest)?;
    let (_, (key, value)) = separated_pair(is_not("="), tag("="), is_not("="))(rest)?;
    log::trace!("key={key} value={value}");
    Ok(("", Command::Set { key: key.trim(), value: value.trim() }))
}

fn parse_delete(input: &str) -> IResult<&str, Command> {
    let (rest, _) = tag_no_case("delete")(input)?;
    let (rest, _) = space1(rest)?;
    Ok(("", Command::Delete { key: rest.trim() }))
}

fn parse_exit(input: &str) -> IResult<&str, Command> {
    _ = tag_no_case("exit")(input)?;
    Ok(("", Command::Exit))
}

fn error(message: impl Display) {
    println!("Error: {message}");
}

fn main() {
    env_logger::init();
    let args = Cli::parse();
    let port = args.port.unwrap_or(6210);
    let mut stream = protocol::Stream(TcpStream::connect(("127.0.0.1", port)).unwrap());
    let stdin = std::io::stdin();
    loop {
        print!("> ");
        std::io::stdout().flush().unwrap();
        let mut line = String::new();
        stdin.read_line(&mut line).unwrap();
        match Command::parse(&line) {
            Command::Get { key } => {
                let Some(value) = stream.get(key.as_bytes()).unwrap() else {
                    error("not found");
                    continue;
                };
                match std::str::from_utf8(&value) {
                    Ok(value) => {
                        println!("{value}");
                        std::io::stdout().flush().unwrap();
                    },
                    Err(err) => error(err),
                }
            },
            Command::Set { key, value } => {
                if let Err(err) = stream.set(key.as_bytes(), value.as_bytes()) {
                    error(err);
                }
            },
            Command::Delete { key } => {
                if let Err(err) = stream.delete(key.as_bytes()) {
                    error(err);
                }
            },
            Command::Exit => {
                return;
            },
        }
    }
}
