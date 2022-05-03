use std::{borrow::Borrow, error::Error};

use rustyline::{error::ReadlineError, Cmd, Editor, KeyCode, KeyEvent, Modifiers, Movement};

use naive_sql_parser::{ParseError, SingleSqlParser};

use crate::{config::REPL_HISTORY, dbms::exec::Exec};

pub fn main_loop() {
    let parser = SingleSqlParser::new();
    let mut rl = Editor::<()>::new();
    if let Err(e) = rl.load_history(REPL_HISTORY.as_path()) {
        eprintln!("Failed to load history because of error: {}", e);
    }

    rl.bind_sequence(
        KeyEvent(KeyCode::Tab, Modifiers::NONE),
        Cmd::Indent(Movement::ForwardChar(4)),
    );

    'main: loop {
        let mut sql = String::new();
        let mut prompt = "naive > ";
        let mut extra_line = false;

        'single: loop {
            match rl.readline(prompt) {
                Ok(line) => {
                    if sql.is_empty() && line.is_empty() {
                        break 'single;
                    } else {
                        sql.push_str(&line)
                    }
                }
                Err(ReadlineError::Interrupted) => break 'single,
                Err(ReadlineError::Eof) => break 'main,
                Err(err) => {
                    eprintln!("wtf... {:?}", err);
                    break 'main;
                }
            }

            use ParseError::*;
            match parser.parse(&sql) {
                Ok(ast) if !extra_line => {
                    if let Err(err) = ast.exec() {
                        handle_err(&sql, err.borrow());
                    }

                    rl.add_history_entry(sql);
                    break 'single;
                }
                Err(UnrecognizedEOF { .. }) | Ok(_) => {
                    extra_line = !extra_line;
                    sql.push_str("\n        ");
                    prompt = "    ... ";
                }
                Err(User { error }) => {
                    eprintln!("User error: {}", error);
                    break 'single;
                }
                Err(e) => {
                    let location = match e {
                        InvalidToken { location } => location,
                        UnrecognizedToken { token, .. } => token.0,
                        ExtraToken { token } => token.0,
                        _ => unreachable!(),
                    };
                    let (prev, rest) = sql.split_at(location);
                    eprintln!(
                        "Syntax error near '{}' at line {}",
                        rest,
                        prev.lines().count(),
                    );
                    rl.add_history_entry(&sql);
                    break 'single;
                }
            } // match parse
        } // single
    } // main

    if let Err(e) = rl.append_history(REPL_HISTORY.as_path()) {
        eprintln!("Failed to store history because of error: {}", e);
    }
}

pub fn handle_err(sql: &str, err: &dyn Error) {
    eprintln!("Error: {}", err);
    if let Some(source) = err.source() {
        eprintln!("...originated from: {}", source);
    }
    eprintln!("...while executing sql: {}", sql);
}
