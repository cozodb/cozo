/*
 * Copyright 2022, The Cozo Project Authors.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file,
 * You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::fmt::Debug;
use std::fs;
use std::process::exit;

use clap::{Args, Parser, Subcommand};
use env_logger::Env;
use log::{error, info};

use crate::repl::repl_main;
use crate::server::{server_main, ServerArgs};

// use rand::Rng;
// use serde_json::json;

// use cozo::*;

mod client;
mod repl;
mod server;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
#[command(propagate_version = true)]
struct AppArgs {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    ///
    Server(ServerArgs),
    Client(ClientArgs),
    Repl(ReplArgs),
    Restore(RestoreArgs),
    Stream(StreamArgs),
}

#[derive(Args, Debug)]
struct ReplArgs {
    /// Database engine, can be `mem`, `sqlite`, `rocksdb` and others.
    #[clap(short, long, default_value_t = String::from("mem"))]
    engine: String,

    /// Path to the directory to store the database
    #[clap(short, long, default_value_t = String::from("cozo.db"))]
    path: String,

    /// Extra config in JSON format
    #[clap(short, long, default_value_t = String::from("{}"))]
    config: String,
}

#[derive(Args, Debug)]
struct ClientArgs {
    #[clap(default_value_t = String::from("http://127.0.0.1:9070"))]
    address: String,
    #[clap(short, long, default_value_t = String::from(""))]
    auth: String,
}

#[derive(Args, Debug)]
struct StreamArgs {}

#[derive(Args, Debug)]
struct RestoreArgs {
    /// Path of the backup file to restore from, must be a SQLite-backed backup file.
    from: String,
    /// Path of the database to restore into
    to: String,
    /// Database engine for the database to restore into, can be `mem`, `sqlite`, `rocksdb` and others.
    #[clap(short, long)]
    engine: String,
}

// macro_rules! check_auth {
//     ($request:expr, $auth_guard:expr) => {
//         match $request.header("x-cozo-auth") {
//             None => return Response::text("Unauthorized").with_status_code(401),
//             Some(code) => {
//                 if $auth_guard != code {
//                     return Response::text("Unauthorized").with_status_code(401);
//                 }
//             }
//         }
//     };
// }

fn main() {
    let args = match AppArgs::parse().command {
        Commands::Server(s) => s,
        Commands::Client(_) => {
            todo!()
        }
        Commands::Repl(_) => {
            todo!()
        }
        Commands::Restore(_) => {
            todo!()
        }
        Commands::Stream(_) => {
            todo!()
        }
    };

    // if let Some(restore_path) = &args.restore {
    //     db.restore_backup(restore_path).unwrap();
    // }

    // if args.repl {
    //     let db_copy = db.clone();
    //     ctrlc::set_handler(move || {
    //         let running = db_copy
    //             .run_script("::running", Default::default())
    //             .expect("Cannot determine running queries");
    //         for row in running.rows {
    //             let id = row.into_iter().next().unwrap();
    //             eprintln!("Killing running query {id}");
    //             db_copy
    //                 .run_script("::kill $id", BTreeMap::from([("id".to_string(), id)]))
    //                 .expect("Cannot kill process");
    //         }
    //     })
    //     .expect("Error setting Ctrl-C handler");
    //
    //     if let Err(e) = repl_main(db) {
    //         eprintln!("{e}");
    //         exit(-1);
    //     }
    // } else {
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(server_main(args))

    // server_main(args, db)
    // }
}

// fn server_main(args: Server, db: DbInstance) {
//     let conf_path = format!("{}.{}.cozo_auth", args.path, args.engine);
//     let auth_guard = match fs::read_to_string(&conf_path) {
//         Ok(s) => s.trim().to_string(),
//         Err(_) => {
//             let s = rand::thread_rng()
//                 .sample_iter(&rand::distributions::Alphanumeric)
//                 .take(64)
//                 .map(char::from)
//                 .collect();
//             fs::write(&conf_path, &s).unwrap();
//             s
//         }
//     };
//
//     let addr = if Ipv6Addr::from_str(&args.bind).is_ok() {
//         format!("[{}]:{}", args.bind, args.port)
//     } else {
//         format!("{}:{}", args.bind, args.port)
//     };
//     println!(
//         "Database ({} backend) web API running at http://{}",
//         args.engine, addr
//     );
//     println!("The auth file is at {conf_path}");
//     rouille::start_server(addr, move |request| {
//         let now = chrono::Utc::now().format("%Y-%m-%d %H:%M:%S%.6f");
//         let log_ok = |req: &Request, _resp: &Response, elap: std::time::Duration| {
//             info!("{} {} {} {:?}", now, req.method(), req.raw_url(), elap);
//         };
//         let log_err = |req: &Request, elap: std::time::Duration| {
//             error!(
//                 "{} Handler panicked: {} {} {:?}",
//                 now,
//                 req.method(),
//                 req.raw_url(),
//                 elap
//             );
//         };
//     });
// }
