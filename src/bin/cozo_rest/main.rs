use actix_web::{get, post, web, App, HttpResponse, HttpServer, Responder};
use cozo::DbInstance;
use std::sync::Arc;

struct AppStateWithDb {
    db: DbInstance,
}

#[get("/")]
async fn hello(data: web::Data<AppStateWithDb>) -> impl Responder {
    // let sess = data.db.session().unwrap().start().unwrap();
    // let res = sess.get_next_main_table_id();
    HttpResponse::Ok().body(format!("Hello world! {:?}", None))
}

#[post("/echo")]
async fn echo(req_body: String) -> impl Responder {
    HttpResponse::Ok().body(req_body)
}

async fn manual_hello() -> impl Responder {
    HttpResponse::Ok().body("Hey there!")
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let mut db = DbInstance::new("_test_rest", false).unwrap();
    db.set_destroy_on_close(true);
    let db = web::Data::new(AppStateWithDb {
        db
    });


    HttpServer::new(move || {
        App::new()
            .app_data(db.clone())
            .service(hello)
            .service(echo)
            .route("/hey", web::get().to(manual_hello))
    })
    .bind(("127.0.0.1", 8080))?
    .run()
    .await
}
