use actix_web::{post, web, App, HttpResponse, HttpServer, Responder};
use cozo::DbInstance;

struct AppStateWithDb {
    db: DbInstance,
}

#[post("/")]
async fn query(body: web::Bytes, data: web::Data<AppStateWithDb>) -> impl Responder {
    let text = String::from_utf8_lossy(body.as_ref());
    let mut sess = data.db.session().unwrap().start().unwrap();
    match sess.run_script(text, true) {
        Ok(res) => HttpResponse::Ok().body(res.to_string()),
        Err(e) => HttpResponse::BadRequest().body(format!("{:?}", e)),
    }
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let mut db = DbInstance::new("_test_rest", false).unwrap();
    db.set_destroy_on_close(true);
    let db = web::Data::new(AppStateWithDb { db });

    HttpServer::new(move || App::new().app_data(db.clone()).service(query))
        .bind(("127.0.0.1", 8080))?
        .run()
        .await
}
