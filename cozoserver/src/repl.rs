/*
 * Copyright 2022, The Cozo Project Authors.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file,
 * You can obtain one at https://mozilla.org/MPL/2.0/.
 */

// This file is based on code contributed by https://github.com/rhn

use std::error::Error;

use prettytable;
use rustyline;

use cozo;
use cozo::DbInstance;

struct Indented;

impl rustyline::hint::Hinter for Indented {
    type Hint = String;
}

impl rustyline::highlight::Highlighter for Indented {}
impl rustyline::completion::Completer for Indented {
    type Candidate = String;

    fn update(
        &self,
        _line: &mut rustyline::line_buffer::LineBuffer,
        _start: usize,
        _elected: &str,
    ) {
        unreachable!();
    }
}

impl rustyline::Helper for Indented {}

impl rustyline::validate::Validator for Indented {
    fn validate(
        &self,
        ctx: &mut rustyline::validate::ValidationContext<'_>,
    ) -> rustyline::Result<rustyline::validate::ValidationResult> {
        Ok(if ctx.input().starts_with(" ") {
            if ctx.input().ends_with("\n") {
                rustyline::validate::ValidationResult::Valid(None)
            } else {
                rustyline::validate::ValidationResult::Incomplete
            }
        } else {
            rustyline::validate::ValidationResult::Valid(None)
        })
    }
}

pub(crate) fn repl_main(db: DbInstance) -> Result<(), Box<dyn Error>> {
    let mut exit = false;
    let mut rl = rustyline::Editor::<Indented>::new()?;
    rl.set_helper(Some(Indented));

    loop {
        let readline = rl.readline("=> ");
        match readline {
            Ok(line) => {
                match db.run_script(&line, Default::default()) {
                    Ok(out) => {
                        use prettytable::format;
                        let mut table = prettytable::Table::new();
                        let headers = out
                            .headers
                            .iter()
                            .map(prettytable::Cell::from)
                            .collect::<Vec<_>>();
                        table.set_titles(prettytable::Row::new(headers));
                        let rows = out
                            .rows
                            .iter()
                            .map(|r| r.iter().map(|c| format!("{}", c)).collect::<Vec<_>>())
                            .collect::<Vec<_>>();
                        let rows = rows
                            .iter()
                            .map(|r| r.iter().map(prettytable::Cell::from).collect::<Vec<_>>());
                        for row in rows {
                            table.add_row(prettytable::Row::new(row));
                        }
                        table.set_format(*format::consts::FORMAT_NO_BORDER_LINE_SEPARATOR);
                        table.printstd();
                    }
                    Err(mut err) => {
                        if err.source_code().is_none() {
                            err = err.with_source_code(line.to_string());
                        }
                        eprintln!("{:?}", err);
                    }
                };
                rl.add_history_entry(line);
            }
            Err(rustyline::error::ReadlineError::Interrupted) => {
                if exit {
                    break;
                } else {
                    println!("Again to exit");
                    exit = true;
                }
            }
            Err(rustyline::error::ReadlineError::Eof) => break,
            Err(e) => eprintln!("{:?}", e),
        }
    }
    Ok(())
}
