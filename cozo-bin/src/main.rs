/*
 * Copyright 2022, The Cozo Project Authors.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file,
 * You can obtain one at https://mozilla.org/MPL/2.0/.
 */

extern crate core;

use std::process::exit;

use clap::{Parser, Subcommand};
use env_logger::Env;

use crate::repl::{repl_main, ReplArgs};
use crate::server::{server_main, ServerArgs};

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
    Server(ServerArgs),
    Repl(ReplArgs),
}

fn main() {
    match AppArgs::parse().command {
        Commands::Server(args) => {
            env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(server_main(args))
        }
        Commands::Repl(args) => {
            if let Err(e) = repl_main(args) {
                eprintln!("{e}");
                exit(-1);
            }
        }
    };

    // if args.repl {

    // } else {

    // server_main(args, db)
    // }
}

// fn server_main(args: Server, db: DbInstance) {
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
