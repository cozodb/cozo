/*
 * Copyright 2022, The Cozo Project Authors. Licensed under MPL-2.0.
 */

use std::fmt::Debug;
use std::fs;
use std::net::Ipv6Addr;
use std::path::PathBuf;
use std::str::FromStr;

use clap::Parser;
use env_logger::Env;
use log::{error, info};
use rand::Rng;
use rouille::{router, try_or_400, Request, Response};

use cozo::Db;

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
    if args.bind != "127.0.0.1" {
        eprintln!("{}", SECURITY_WARNING);
    }

    let db = Db::new(args.path.as_str()).unwrap();

    let mut path_buf = PathBuf::from(&args.path);
    path_buf.push("auth.txt");
    let auth_guard = match fs::read_to_string(&path_buf) {
        Ok(s) => s.trim().to_string(),
        Err(_) => {
            let s = rand::thread_rng()
                .sample_iter(&rand::distributions::Alphanumeric)
                .take(64)
                .map(char::from)
                .collect();
            fs::write(&path_buf, &s).unwrap();
            s
        }
    };

    let addr = if Ipv6Addr::from_str(&args.bind).is_ok() {
        format!("[{}]:{}", args.bind, args.port)
    } else {
        format!("{}:{}", args.bind, args.port)
    };
    println!("Database web API running at http://{}", addr);
    rouille::start_server(addr, move |request| {
        let now = chrono::Utc::now().format("%Y-%m-%d %H:%M:%S%.6f");
        let log_ok = |req: &Request, _resp: &Response, elap: std::time::Duration| {
            info!("{} {} {} {:?}", now, req.method(), req.raw_url(), elap);
        };
        let log_err = |req: &Request, elap: std::time::Duration| {
            error!(
                "{} Handler panicked: {} {} {:?}",
                now,
                req.method(),
                req.raw_url(),
                elap
            );
        };
        rouille::log_custom(request, log_ok, log_err, || {
            router!(request,
                (POST) (/text-query) => {
                    if !request.remote_addr().ip().is_loopback() {
                        match request.header("x-cozo-auth") {
                            None => return Response::text("Unauthorized").with_status_code(401),
                            Some(code) => {
                                if auth_guard != code {
                                    return Response::text("Unauthorized").with_status_code(401);
                                }
                            }
                        }
                    }

                    #[derive(serde_derive::Serialize, serde_derive::Deserialize)]
                    struct QueryPayload {
                        script: String,
                        params: serde_json::Map<String, serde_json::Value>,
                    }

                    let payload: QueryPayload = try_or_400!(rouille::input::json_input(request));
                    let result = db.run_script_fold_err(&payload.script, &payload.params);
                    let response = Response::json(&result);
                    if let Some(serde_json::Value::Bool(true)) = result.get("ok") {
                        response
                    } else {
                        response.with_status_code(400)
                    }
                    // {
                    //
                    //     Ok(mut result) => {
                    //         if let Some(obj) = result.as_object_mut() {
                    //             obj.insert(
                    //                 "time_taken".to_string(),
                    //                 json!(start.elapsed().as_millis() as u64),
                    //             );
                    //         }
                    //         Response::json(&result)
                    //     }
                    //     _ => Response::json(&result).with_status_code(400)
                    // }
                },
                (GET) (/) => {
                    Response::html(HTML_CONTENT)
                },
                _ => Response::empty_404()
            )
        })
    });
}

const HTML_CONTENT: &str = r##"
<!DOCTYPE html>
<html lang="en">
<head>
<link rel="icon" href="data:;base64,iVBORw0KGgo=">
<title>Cozo database</title>
</head>
<body>
<p>Cozo HTTP server is running.</p>
<script>
    let COZO_AUTH = '';
    let LAST_RESP = null;

    async function run(script, params) {
        const resp = await fetch('/text-query', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'x-cozo-auth': COZO_AUTH
            },
            body: JSON.stringify({
                script,
                params: params || {}
            })
        });
        if (resp.ok) {
            const json_resp = await resp.json();
            LAST_RESP = json_resp;
            if (json_resp) {
                json_resp.headers ||= [];
                console.table(json_resp.rows.map(row => {
                    let ret = {};
                    for (let i = 0; i < row.length; ++i) {
                        ret[json_resp.headers[i] || `(${i})`] = row[i];
                    }
                    return ret
                }))
            }
        } else {
            console.error((await resp.json()).display)
        }
    }
    console.log(
`Welcome to the Cozo Makeshift Javascript Console!
You can run your query like this:

await run("YOUR QUERY HERE", {param: value})

The global variables 'COZO_AUTH' and 'LAST_RESP' are available.`);
</script>
</body>
</html>
"##;

const SECURITY_WARNING: &str = r#"
====================================================================================
                      !! SECURITY NOTICE, PLEASE READ !!
====================================================================================
You instructed Cozo to bind to a non-default address.
Cozo is designed to be accessed by trusted clients in a trusted network.
As a last defense against unauthorized access when everything else fails,
any requests from non-loopback addresses require the HTTP request header
`x-cozo-auth` to be set to the content of auth.txt in your database directory.
This is not a sufficient protection against attacks, and you must set up
proper authentication schemes, encryptions, etc. by firewalls and/or proxies.
====================================================================================
"#;
