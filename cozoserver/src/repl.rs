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
use std::fs::File;
use std::io::{Read, Write};

use miette::{bail, miette, IntoDiagnostic};
use serde_json::{json, Value};

use cozo::{DataValue, DbInstance};

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
        Ok(if ctx.input().starts_with(' ') {
            if ctx.input().ends_with('\n') {
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

    let history_file = ".cozo_repl_history";
    if rl.load_history(history_file).is_ok() {
        println!("Loaded history from {history_file}");
    }

    loop {
        let readline = rl.readline("=> ");
        match readline {
            Ok(line) => {
                if let Err(err) = process_line(&line, &db, &mut params, &mut save_next) {
                    eprintln!("{err:?}");
                }
                rl.add_history_entry(line);
                exit = false;
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
            Err(e) => eprintln!("{e:?}"),
        }
    }

    if rl.save_history(history_file).is_ok() {
        eprintln!("Query history saved in {history_file}");
    }
    Ok(())
}

fn process_line(
    line: &str,
    db: &DbInstance,
    params: &mut BTreeMap<String, DataValue>,
    save_next: &mut Option<String>,
) -> miette::Result<()> {
    let line = line.trim();
    if line.is_empty() {
        return Ok(());
    }
    if let Some(remaining) = line.strip_prefix('%') {
        let remaining = remaining.trim();
        let (op, payload) = remaining
            .split_once(|c: char| c.is_whitespace())
            .unwrap_or((remaining, ""));
        match op {
            "set" => {
                let (key, v_str) = payload
                    .trim()
                    .split_once(|c: char| c.is_whitespace())
                    .ok_or_else(|| miette!("Bad set syntax. Should be '%set <KEY> <VALUE>'."))?;
                let val = serde_json::from_str(v_str).into_diagnostic()?;
                params.insert(key.to_string(), val);
            }
            "unset" => {
                let key = payload.trim();
                if params.remove(key).is_none() {
                    bail!("Key not found: '{}'", key)
                }
            }
            "clear" => {
                params.clear();
            }
            "params" => {
                let display = serde_json::to_string_pretty(&json!(&params)).into_diagnostic()?;
                println!("{display}");
            }
            "backup" => {
                let path = payload.trim();
                if path.is_empty() {
                    bail!("Backup requires a path");
                };
                db.backup_db(path)?;
                println!("Backup written successfully to {path}")
            }
            "restore" => {
                let path = payload.trim();
                if path.is_empty() {
                    bail!("Restore requires a path");
                };
                db.restore_backup(path)?;
                println!("Backup successfully loaded from {path}")
            }
            "save" => {
                let next_path = payload.trim();
                if next_path.is_empty() {
                    println!("Next result will NOT be saved to file");
                } else {
                    println!("Next result will be saved to file: {next_path}");
                    *save_next = Some(next_path.to_string())
                }
            }
            "import" => {
                let url = payload.trim();
                if url.starts_with("http://") || url.starts_with("https://") {
                    let data = minreq::get(url).send().into_diagnostic()?;
                    let data = data.as_str().into_diagnostic()?;
                    db.import_relations_str_with_err(data)?;
                    println!("Imported data from {url}")
                } else {
                    let file_path = url.strip_prefix("file://").unwrap_or(url);
                    let mut file = File::open(file_path).into_diagnostic()?;
                    let mut content = String::new();
                    file.read_to_string(&mut content).into_diagnostic()?;
                    db.import_relations_str_with_err(&content)?;
                    println!("Imported data from {url}");
                }
            }
            op => bail!("Unknown op: {}", op),
        }
    } else {
        let out = db.run_script(line, params.clone())?;
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

            let mut file = File::create(path).into_diagnostic()?;
            file.write_all(j_payload.to_string().as_bytes())
                .into_diagnostic()?;
            *save_next = None;
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
                .map(|r| r.iter().map(|c| format!("{c}")).collect::<Vec<_>>())
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
    }
    Ok(())
}
