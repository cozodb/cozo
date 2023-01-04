/*
 * Copyright 2022, The Cozo Project Authors.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file,
 * You can obtain one at https://mozilla.org/MPL/2.0/.
 */

// This file is based on code contributed by https://github.com/rhn

use std::collections::BTreeMap;
use std::error::Error;
use std::io::Write;

use prettytable;
use rustyline;
use serde_json::{json, Value};

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
    println!("Welcome to the Cozo REPL.");
    println!("Type a space followed by newline to enter multiline mode.");

    let mut exit = false;
    let mut rl = rustyline::Editor::<Indented>::new()?;
    let mut params = BTreeMap::new();
    let mut save_next: Option<String> = None;
    rl.set_helper(Some(Indented));

    loop {
        let readline = rl.readline("=> ");
        match readline {
            Ok(line) => {
                let line = line.trim();
                if line.is_empty() {
                    continue;
                }
                if let Some(remaining) = line.strip_prefix("%") {
                    let remaining = remaining.trim();
                    let (op, payload) = remaining
                        .split_once(|c: char| c.is_whitespace())
                        .unwrap_or((remaining, ""));
                    match op {
                        "set" => {
                            if let Some((key, v_str)) =
                                payload.trim().split_once(|c: char| c.is_whitespace())
                            {
                                match serde_json::from_str(v_str) {
                                    Ok(val) => {
                                        params.insert(key.to_string(), val);
                                    }
                                    Err(e) => {
                                        eprintln!("{:?}", e)
                                    }
                                }
                            } else {
                                eprintln!("Bad set syntax. Should be '%set <KEY> <VALUE>'.")
                            }
                        }
                        "unset" => {
                            let key = remaining.trim();
                            if params.remove(key).is_none() {
                                eprintln!("Key not found: '{}'", key)
                            }
                        }
                        "clear" => {
                            params.clear();
                        }
                        "params" => match serde_json::to_string_pretty(&json!(&params)) {
                            Ok(display) => {
                                println!("{}", display)
                            }
                            Err(err) => {
                                eprintln!("{:?}", err)
                            }
                        },
                        "backup" => {
                            let path = remaining.trim();
                            if path.is_empty() {
                                eprintln!("Backup requires a path");
                            } else {
                                match db.backup_db(path.to_string()) {
                                    Ok(_) => {
                                        println!("Backup written successfully to {}", path)
                                    }
                                    Err(err) => {
                                        eprintln!("{:?}", err)
                                    }
                                }
                            }
                        }
                        "restore" => {
                            let path = remaining.trim();
                            if path.is_empty() {
                                eprintln!("Restore requires a path");
                            } else {
                                match db.restore_backup(path) {
                                    Ok(_) => {
                                        println!("Backup successfully loaded from {}", path)
                                    }
                                    Err(err) => {
                                        eprintln!("{:?}", err)
                                    }
                                }
                            }
                        }
                        "save" => {
                            let next_path = remaining.trim();
                            if next_path.is_empty() {
                                eprintln!("Next result will NOT be saved to file");
                            } else {
                                eprintln!("Next result will be saved to file: {}", next_path);
                                save_next = Some(next_path.to_string())
                            }
                        }
                        op => eprintln!("Unknown op: {}", op),
                    }
                } else {
                    match db.run_script(&line, params.clone()) {
                        Ok(out) => {
                            if let Some(path) = save_next.as_ref() {
                                println!(
                                    "Query has returned {} rows, saving to file {}",
                                    out.rows.len(),
                                    path
                                );

                                let to_save = out
                                    .rows
                                    .iter()
                                    .map(|row| -> Value {
                                        row.iter()
                                            .zip(out.headers.iter())
                                            .map(|(v, k)| (k.to_string(), v.clone()))
                                            .collect()
                                    })
                                    .collect();

                                let j_payload = Value::Array(to_save);

                                match std::fs::File::create(path) {
                                    Ok(mut file) => {
                                        match file.write_all(j_payload.to_string().as_bytes()) {
                                            Ok(_) => {
                                                save_next = None;
                                            }
                                            Err(err) => {
                                                eprintln!("{:?}", err);
                                            }
                                        }
                                    }
                                    Err(err) => {
                                        eprintln!("{:?}", err);
                                    }
                                }
                            } else {
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
                                let rows = rows.iter().map(|r| {
                                    r.iter().map(prettytable::Cell::from).collect::<Vec<_>>()
                                });
                                for row in rows {
                                    table.add_row(prettytable::Row::new(row));
                                }
                                table.set_format(*format::consts::FORMAT_NO_BORDER_LINE_SEPARATOR);
                                table.printstd();
                            }
                        }
                        Err(mut err) => {
                            if err.source_code().is_none() {
                                err = err.with_source_code(line.to_string());
                            }
                            eprintln!("{:?}", err);
                        }
                    };
                }
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
