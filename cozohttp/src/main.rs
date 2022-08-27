use std::fmt::{Debug, Display, Formatter};
use std::path::Path;
use std::time::Instant;

use actix_cors::Cors;
use actix_web::rt::task::spawn_blocking;
use actix_web::{post, web, App, HttpResponse, HttpServer, Responder};
use clap::Parser;
use env_logger::Env;
use log::info;

use actix_web_static_files::ResourceFiles;
use anyhow::anyhow;
use cozo::{Db, DbBuilder};

type Result<T> = std::result::Result<T, RespError>;

struct RespError {
    err: anyhow::Error,
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

impl actix_web::error::ResponseError for RespError {}

impl From<cozo::Error> for RespError {
    fn from(err: cozo::Error) -> RespError {
        RespError { err }
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

    /// Temporary database, i.e. will be deleted when the program exits
    #[clap(short, long, default_value_t = false, action)]
    temp: bool,
}

struct AppStateWithDb {
    db: Db,
}

#[post("/text-query")]
async fn query(body: web::Bytes, data: web::Data<AppStateWithDb>) -> Result<impl Responder> {
    let text = std::str::from_utf8(&body)
        .map_err(|e| anyhow!(e))?
        .to_string();
    let db = data.db.new_session()?;
    let start = Instant::now();
    let task = spawn_blocking(move || db.run_script(&text));
    let result = task.await.map_err(|e| anyhow!(e))??;
    info!("finished query in {:?}", start.elapsed());
    Ok(HttpResponse::Ok().json(result))
}

include!(concat!(env!("OUT_DIR"), "/generated.rs"));

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();
    let args = Args::parse();
    if args.temp && Path::new(&args.path).exists() {
        panic!(
            "cannot open database at '{}' as temporary since it already exists",
            args.path
        );
    }

    let builder = DbBuilder::default()
        .path(&args.path)
        .create_if_missing(true)
        .destroy_on_exit(args.temp);
    let db = Db::build(builder).unwrap();

    let app_state = web::Data::new(AppStateWithDb { db });

    let addr = (&args.bind as &str, args.port);
    info!(
        "Serving database {} at http://{}:{}",
        args.path, addr.0, addr.1
    );

    HttpServer::new(move || {
        let cors = Cors::permissive();
        let generated = generate();

        App::new()
            .app_data(app_state.clone())
            .wrap(cors)
            .service(query)
            .service(ResourceFiles::new("/", generated))
    })
    .bind(addr)?
    .run()
    .await
}
