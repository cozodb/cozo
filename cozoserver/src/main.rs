use std::collections::BTreeMap;
use std::env;
use std::fmt::Debug;
use std::process::exit;
use std::time::Instant;

use clap::Parser;
use env_logger::Env;
use rouille::{router, try_or_400, Response};
use serde_json::json;

use cozo::{Db, DbBuilder};

#[derive(Parser, Debug)]
#[clap(version, about, long_about = None)]
struct Args {
    /// Path to the directory to store the database
    #[clap(value_parser)]
    path: String,

    /// Address to bind the service to
    #[clap(short, long, default_value_t = String::from("127.0.0.1"))]
    bind: String,

    /// Port to use
    #[clap(short, long, default_value_t = 9070)]
    port: u16,
}

fn main() {
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();
    let args = Args::parse();
    let auth_str = env::var("COZO_AUTH").ok();
    if args.bind != "127.0.0.1" && auth_str.is_none() {
        eprintln!(
            r#"You instructed Cozo to bind to address {}, which can potentially be accessed from
external networks. Please note that Cozo is designed to be accessed by trusted clients inside
trusted environments only. If you are absolutely sure that exposing Cozo to the address is OK,
set the environment variable COZO_AUTH and configure clients appropriately."#,
            args.bind
        );
        exit(1);
    }

    let builder = DbBuilder::default()
        .path(&args.path)
        .create_if_missing(true);
    let db = Db::build(builder).unwrap();

    let addr = format!("{}:{}", args.bind, args.port);
    println!("Service running at http://{}", addr);
    rouille::start_server(addr, move |request| {
        if let Some(auth) = &auth_str {
            match request.header("x-cozo-auth") {
                None => return Response::text("Unauthorized").with_status_code(401),
                Some(code) => {
                    if auth != code {
                        return Response::text("Unauthorized").with_status_code(401);
                    }
                }
            }
        }

        router!(request,
            (POST) (/text-query) => {
                #[derive(serde_derive::Serialize, serde_derive::Deserialize)]
                struct QueryPayload {
                    script: String,
                    params: BTreeMap<String, serde_json::Value>,
                }

                let payload: QueryPayload = try_or_400!(rouille::input::json_input(request));
                let start = Instant::now();

                match db.run_script(&payload.script, &payload.params) {
                    Ok(mut result) => {
                        if let Some(obj) = result.as_object_mut() {
                            obj.insert(
                                "time_taken".to_string(),
                                json!(start.elapsed().as_millis() as u64),
                            );
                        }
                        Response::json(&result)
                    }
                    Err(e) => Response::text(format!("{:?}", e)).with_status_code(400),
                }
            },
            _ => Response::empty_404()
        )
    });
}
