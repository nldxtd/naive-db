use std::{
    fs::File,
    io::{BufReader, Read},
    path::{Path, PathBuf},
};

use naive_sql_parser::SqlStmtsParser;
use structopt::StructOpt;

use crate::{
    dbms::{
        database::{change_database, ensure_table, get_table_id, modify_table},
        exec::Exec,
    },
    error::DBResult,
    repl,
    utils::table::parse_colval,
};

#[derive(Debug, StructOpt)]
enum Sub {
    /// Load data from csv file into table
    Load {
        /// CSV file to load data from
        #[structopt(long, parse(from_os_str), name = "csv_file")]
        from: PathBuf,
        /// Database where the table resides
        #[structopt(long, name = "database")]
        to: String,
        /// Table name to insert data into
        #[structopt(long, name = "table_name")]
        table: String,
    },
    /// Exec all statements in an SQL file
    Exec {
        /// Execute SQL file
        #[structopt(long = "path", parse(from_os_str))]
        sql_path: PathBuf,
    },
    /// Run in REPL mode (default)
    Repl,
}

#[derive(Debug, StructOpt)]
#[structopt(name = "Naive Database", about = "Duck this course")]
struct Opt {
    #[structopt(subcommand)]
    cmd: Option<Sub>,
}

fn load_csv(from: &Path, database: &str, table: &str) -> DBResult<()> {
    change_database(database);
    let id = match get_table_id(table) {
        Some(id) => id,
        None => {
            return Err(format!("table {} does not exist in database {}", table, database).into())
        }
    };

    let (coltype, slot_size) = ensure_table(id, move |table| {
        (
            table
                .meta
                .columns
                .iter()
                .map(|col| col.coltype)
                .collect::<Vec<_>>(),
            table.meta.slot_size() as u64,
        )
    });

    let file = File::open(from)?;
    let n_slots = (file.metadata()?.len() as f32 * 1.3) as u64 / slot_size;
    let mut rdr = csv::Reader::from_reader(BufReader::new(file));

    modify_table(id, |table| -> DBResult<()> {
        table.reserve_for(n_slots as _)?;
        let mut row = Vec::with_capacity(15);
        let headers = rdr.headers();
        row.extend(
            headers?
                .into_iter()
                .enumerate()
                .map(|(i, val)| parse_colval(coltype[i], val).unwrap()),
        );
        table.insert(&row)?;
        row.clear();
        for val in rdr.records() {
            row.extend(
                val?.into_iter()
                    .enumerate()
                    .map(|(i, val)| parse_colval(coltype[i], val).unwrap()),
            );
            table.insert(&row)?;
            row.clear();
        }
        Ok(())
    })?;
    Ok(())
}

fn exec_sql(path: &Path) -> DBResult<()> {
    let mut sqls = String::new();
    File::open(path)?.read_to_string(&mut sqls)?;

    let parser = SqlStmtsParser::new();
    match parser.parse(&sqls) {
        Ok(stmts) => stmts.exec()?,
        Err(e) => eprintln!("Error while parsing sql: {:?}", e),
    }
    Ok(())
}

pub fn run_cli() -> DBResult<()> {
    let cli = Opt::from_args();

    match cli.cmd {
        Some(cmd) => match cmd {
            Sub::Exec { sql_path } => exec_sql(&sql_path)?,
            Sub::Load { from, to, table } => load_csv(&from, &to, &table)?,
            Sub::Repl => repl::main_loop(),
        },
        None => repl::main_loop(),
    }

    Ok(())
}
