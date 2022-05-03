use cli::run_cli;
use error::DBResult;
use init::{init, write_back};

#[macro_use]
extern crate serde;

mod cli;
mod config;
mod dbms;
mod defines;
mod error;
mod filesystem;
mod index;
mod init;
mod page;
mod record;
mod repl;
mod utils;

fn main() -> DBResult<()> {
    init();
    run_cli()?;
    write_back().map_err(|e| format!("Failed to exit correctly because of {:?}", e))?;
    Ok(())
}
