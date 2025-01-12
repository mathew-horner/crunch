# Crunch

Crunch is an LSM-tree storage engine, and will eventually also have an actual NoSQL database built on top of it.

*This is a hobbyist project; it is not meant for serious use.*

## Environment Variables

### Types

These type placeholders are used throughout the environment variable table:

|Type|Accepted Value(s)|
|-|-|
|`bool`|`true \| 1 \| false \| 0`|
|`uint`|Integer value >= 0|

### Variables

|Variable Name|Description|Accepted Value(s)|
|-|-|-|
|`CRUNCH_ENGINE_MEMTABLE__CAPACITY`|The number of key-value pairs that the memtable can hold before it flushes to disk|`<number>`|
|`CRUNCH_ENGINE_STORE__COMPACTION_ENABLED`|Whether the background thread to perform compaction should run.|`<bool>`|
|`CRUNCH_ENGINE_STORE__COMPACTION_INTERVAL_SECONDS`|The number of seconds between compaction runs.|`<number>`|

## Usage

Right now, if you run `cargo run` you will get a REPL type interface for setting key-value pairs directly in the engine.
This is useful for development, but eventually the database will run as its own server and allow arbitrary clients to
communicate with it over the network.
