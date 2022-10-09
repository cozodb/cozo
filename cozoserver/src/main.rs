use std::collections::BTreeMap;
use std::fmt::Debug;
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

    let builder = DbBuilder::default()
        .path(&args.path)
        .create_if_missing(true);
    let db = Db::build(builder).unwrap();

    let addr = format!("{}:{}", args.bind, args.port);
    println!("Service running at http://{}", addr);
    rouille::start_server(addr, move |request| {
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
