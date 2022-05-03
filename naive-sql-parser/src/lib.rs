#![allow(unused)]

mod ast;
mod defs;
mod sql;

pub use ast::*;
pub use defs::*;
pub use lalrpop_util::ParseError;
pub use sql::{SingleSqlParser, SqlStmtsParser};

#[cfg(test)]
mod tests {
    use super::sql;
    use std::{
        fs::{self, File},
        io::{Read, Result},
    };

    #[test]
    fn test_all_sql_files() -> Result<()> {
        let parser = sql::SqlStmtsParser::new();
        let files = fs::read_dir("tests")?.filter_map(|r| {
            let f = r.ok()?;
            if f.file_type().ok()?.is_file() {
                let path = f.path();
                File::open(&path).ok().map(|f| (path, f))
            } else {
                None
            }
        });
        for (path, mut file) in files {
            let mut buf = String::new();
            file.read_to_string(&mut buf);
            let result = parser.parse(&buf);
            println!("{:?}", &result);
            assert!(
                result.is_ok(),
                "error when running {}",
                path.to_string_lossy()
            );
        }
        Ok(())
    }
}
