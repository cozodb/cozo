use std::{env, thread};
use std::collections::{BTreeMap, HashMap};
use std::fmt::{Debug, Display, Formatter};
use std::io::{stdin, stdout, Write};
use std::str::from_utf8;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use actix_cors::Cors;
use actix_web::{App, HttpRequest, HttpResponse, HttpServer, post, Responder, ResponseError, web};
use actix_web::body::BoxBody;
use actix_web::http::header::HeaderName;
use actix_web::rt::task::spawn_blocking;
use actix_web_static_files::ResourceFiles;
use ansi_to_html::convert_escaped;
use clap::Parser;
use env_logger::Env;
use log::debug;
use miette::{bail, IntoDiagnostic, miette};
use rand::Rng;
use serde_json::json;
use sha3::Digest;

use cozo::{Db, DbBuilder};

type Result<T> = std::result::Result<T, RespError>;

struct RespError {
    err: miette::Error,
}

impl Debug for RespError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.err)
    }
}

impl Display for RespError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.err)
    }
}

impl From<cozo::Error> for RespError {
    fn from(err: cozo::Error) -> RespError {
        RespError { err }
    }
}

impl ResponseError for RespError {
    fn error_response(&self) -> HttpResponse<BoxBody> {
        let formatted = format!("{:?}", self.err);
        let converted = convert_escaped(&formatted).unwrap();
        HttpResponse::BadRequest().body(converted)
    }
}

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

    /// Open playground in browser
    #[clap(long, action)]
    playground: bool,
}

struct AppStateWithDb {
    db: Db,
    pass_cache: Arc<RwLock<HashMap<String, Box<[u8]>>>>,
    seed: Box<[u8]>,
    playground: bool,
}

const PASSWORD_KEY: &str = "WEB_USER_PASSWORD";

impl AppStateWithDb {
    async fn verify_password(&self, req: &HttpRequest) -> miette::Result<()> {
        if self.playground {
            return Ok(());
        }
        let username = req
            .headers()
            .get(&HeaderName::from_static("x-cozo-username"))
            .ok_or_else(|| miette!("not authenticated"))?
            .to_str()
            .into_diagnostic()?;
        let password = req
            .headers()
            .get(&HeaderName::from_static("x-cozo-password"))
            .ok_or_else(|| miette!("not authenticated"))?
            .to_str()
            .into_diagnostic()?;
        if let Some(stored) = self.pass_cache.read().unwrap().get(username).cloned() {
            let mut seed = self.seed.to_vec();
            seed.extend_from_slice(password.as_bytes());
            let digest: &[u8] = &sha3::Sha3_256::digest(&seed);
            if *stored == *digest {
                return Ok(());
            } else {
                self.pass_cache.write().unwrap().remove(username);
                bail!("invalid password")
            }
        }
        let pass_cache = self.pass_cache.clone();
        let mut seed = self.seed.to_vec();
        let db = self.db.new_session()?;
        let password = password.to_string();
        let username = username.to_string();
        spawn_blocking(move || -> miette::Result<()> {
            if let Some(hashed) = db.get_meta_kv(&[PASSWORD_KEY, &username])? {
                let hashed = from_utf8(&hashed).into_diagnostic()?;
                if argon2::verify_encoded(&hashed, password.as_bytes()).into_diagnostic()? {
                    seed.extend_from_slice(password.as_bytes());
                    let easy_digest: &[u8] = &sha3::Sha3_256::digest(&seed);
                    pass_cache
                        .write()
                        .unwrap()
                        .insert(username, easy_digest.into());
                    return Ok(());
                }
            }
            thread::sleep(Duration::from_millis(1234));
            bail!("invalid password")
        })
            .await
            .into_diagnostic()?
    }

    async fn reset_password(&self, user: &str, new_pass: &str) -> miette::Result<()> {
        let pass_cache = self.pass_cache.clone();
        let db = self.db.new_session()?;
        let username = user.to_string();
        let new_pass = new_pass.to_string();
        spawn_blocking(move || -> miette::Result<()> {
            pass_cache.write().unwrap().remove(&username);
            let salt = rand::thread_rng().gen::<[u8; 32]>();
            let config = argon2config();
            let hash =
                argon2::hash_encoded(new_pass.as_bytes(), &salt, &config).into_diagnostic()?;
            db.put_meta_kv(&[PASSWORD_KEY, &username], hash.as_bytes())?;
            Ok(())
        })
            .await
            .into_diagnostic()?
    }

    async fn remove_user(&self, user: &str) -> miette::Result<()> {
        self.pass_cache.write().unwrap().remove(user);
        self.db.remove_meta_kv(&[PASSWORD_KEY, &user])?;
        Ok(())
    }
}

fn argon2config() -> argon2::Config<'static> {
    argon2::Config {
        variant: argon2::Variant::Argon2id,
        mem_cost: 65536,
        time_cost: 10,
        ..argon2::Config::default()
    }
}

#[derive(serde_derive::Serialize, serde_derive::Deserialize)]
struct QueryPayload {
    script: String,
    params: BTreeMap<String, serde_json::Value>,
}

#[post("/text-query")]
async fn query(
    body: web::Json<QueryPayload>,
    data: web::Data<AppStateWithDb>,
    req: HttpRequest,
) -> Result<impl Responder> {
    data.verify_password(&req).await?;
    let db = data.db.new_session()?;
    let start = Instant::now();
    let task = spawn_blocking(move || db.run_script(&body.script, &body.params, data.playground));
    let mut result = task.await.map_err(|e| miette!(e))??;
    if let Some(obj) = result.as_object_mut() {
        obj.insert(
            "time_taken".to_string(),
            json!(start.elapsed().as_millis() as u64),
        );
    }
    Ok(HttpResponse::Ok().json(result))
}

#[derive(serde_derive::Serialize, serde_derive::Deserialize)]
struct ChangePassPayload {
    new_pass: String,
}

#[post("/change-password")]
async fn change_password(
    body: web::Json<ChangePassPayload>,
    data: web::Data<AppStateWithDb>,
    req: HttpRequest,
) -> Result<impl Responder> {
    data.verify_password(&req).await?;
    let username = req
        .headers()
        .get(&HeaderName::from_static("x-cozo-username"))
        .ok_or_else(|| miette!("not authenticated"))?
        .to_str()
        .map_err(|e| miette!(e))?;
    data.reset_password(username, &body.new_pass).await?;
    Ok(HttpResponse::Ok().json(json!({"status": "OK"})))
}

#[derive(serde_derive::Serialize, serde_derive::Deserialize)]
struct AssertUserPayload {
    username: String,
    new_pass: String,
}

#[post("/assert-user")]
async fn assert_user(
    body: web::Json<AssertUserPayload>,
    data: web::Data<AppStateWithDb>,
    req: HttpRequest,
) -> Result<impl Responder> {
    data.verify_password(&req).await?;
    data.reset_password(&body.username, &body.new_pass).await?;
    Ok(HttpResponse::Ok().json(json!({"status": "OK"})))
}

#[derive(serde_derive::Serialize, serde_derive::Deserialize)]
struct RemoveUserPayload {
    username: String,
}

#[post("/remove-user")]
async fn remove_user(
    body: web::Json<RemoveUserPayload>,
    data: web::Data<AppStateWithDb>,
    req: HttpRequest,
) -> Result<impl Responder> {
    data.verify_password(&req).await?;
    data.remove_user(&body.username).await?;
    Ok(HttpResponse::Ok().json(json!({"status": "OK"})))
}

include!(concat!(env!("OUT_DIR"), "/generated.rs"));

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();
    let args = Args::parse();

    let builder = DbBuilder::default()
        .path(&args.path)
        .create_if_missing(true);
    let db = Db::build(builder).unwrap();

    if !args.playground {
        match db.meta_range_scan(&[PASSWORD_KEY]).next() {
            None => {
                let (username, password) = match (
                    env::var("COZO_INITIAL_WEB_USERNAME"),
                    env::var("COZO_INITIAL_WEB_PASSWORD"),
                ) {
                    (Ok(username), Ok(password)) => (username, password),
                    _ => {
                        println!("Welcome to Cozo!");
                        println!();
                        println!(
                            "This is the first time you are running this database at {},",
                            args.path
                        );
                        println!("so let's create a username and password.");

                        loop {
                            println!();
                            print!("Enter a username: ");

                            let _ = stdout().flush();
                            let mut username = String::new();
                            stdin().read_line(&mut username).unwrap();
                            let username = username.trim().to_string();
                            if username.is_empty() {
                                continue;
                            }
                            let password = rpassword::prompt_password("Enter your password: ").unwrap();
                            let confpass = rpassword::prompt_password("Again to confirm it: ").unwrap();

                            if password.trim() != confpass.trim() {
                                println!("Password mismatch. Try again.");
                                continue;
                            }
                            println!("Done, you can now log in with your new username/password in the WebUI!");
                            break (username, password.trim().to_string());
                        }
                    }
                };

                let salt = rand::thread_rng().gen::<[u8; 32]>();
                let config = argon2config();
                let hash = argon2::hash_encoded(password.trim().as_bytes(), &salt, &config).unwrap();
                db.put_meta_kv(&[PASSWORD_KEY, &username], hash.as_bytes())
                    .unwrap();
            }
            Some(Err(err)) => panic!("{}", err),
            Some(Ok((user, _))) => {
                debug!("User {:?}", user[1]);
            }
        }
    }

    let app_state = web::Data::new(AppStateWithDb {
        db,
        pass_cache: Arc::new(Default::default()),
        seed: Box::new(rand::thread_rng().gen::<[u8; 32]>()),
        playground: args.playground,
    });

    let addr = (&args.bind as &str, args.port);
    let url_to_open = if args.playground {
        let url = format!("http://{}:{}", addr.0, addr.1);
        println!("Access playground at {}", url);
        println!("DO NOT run the playground in production!");
        Some(url)
    } else {
        println!("Service running at http://{}:{}", addr.0, addr.1);
        None
    };

    let server = HttpServer::new(move || {
        let cors = Cors::permissive();
        let generated = generate();

        let mut app = App::new()
            .app_data(app_state.clone())
            .wrap(cors)
            .service(query)
            .service(change_password)
            .service(assert_user)
            .service(remove_user);
        if args.playground {
            app = app.service(ResourceFiles::new("/", generated));
        }
        app
    })
        .bind(addr)?
        .run();

    if args.playground {
        let (server_res, _) = futures::join!(server, open_url(url_to_open));
        server_res?;
    } else {
        server.await?;
    }
    Ok(())
}


async fn open_url(url: Option<String>) {
    if let Some(url) = url {
        if webbrowser::open(&url).is_err() {
            println!("Cannot open the browser for you. You have to do it manually.")
        }
    }
}