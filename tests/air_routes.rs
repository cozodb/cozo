use std::fs::read_to_string;
use std::time::Instant;
use anyhow::Result;

use cozo::Db;
use cozorocks::DbBuilder;

fn create_db(name: &str, destroy_on_exit: bool) -> Db {
    let builder = DbBuilder::default()
        .path(name)
        .create_if_missing(true)
        .destroy_on_exit(destroy_on_exit);
    Db::build(builder).unwrap()
}

fn init_logger() {
    let _ = env_logger::builder().is_test(true).try_init();
}


#[test]
fn air_routes() -> Result<()> {
    init_logger();
    let db = create_db("_test_air_routes", false);
    let attr_res = db.run_tx_attributes(r#"
        put country {
            code: string identity,
            desc: string
        }
        put continent {
            code: string identity,
            desc: string
        }
        put airport {
            iata: string identity,
            icao: string index,
            city: string index,
            desc: string,
            region: string index,
            country: ref,
            runways: int,
            longest: int,
            altitude: int,
            lat: float,
            lon: float
        }
        put route {
            src: ref,
            dst: ref,
            distance: int
        }
        put geo {
            contains: ref
        }
    "#);

    if attr_res.is_ok() {
        let insertions = read_to_string("tests/air-routes-data.json")?;
        let triple_insertion_time = Instant::now();
        db.run_tx_triples(&insertions)?;
        dbg!(triple_insertion_time.elapsed());

    }

    let simple_query_time = Instant::now();
    let res = db.run_script(r#"
        ?[?c, ?code, ?desc] := [?c country.code 'CU'] or ?c is 10000239, [?c country.code ?code], [?c country.desc ?desc];
    "#)?;
    dbg!(simple_query_time.elapsed());
    println!("{}", res);

    let no_airports_time = Instant::now();
    let res = db.run_script(r#"
        ?[?desc] := [?c country.desc ?desc], not [?a airport.country ?c];
    "#)?;
    dbg!(no_airports_time.elapsed());
    println!("{}", res);

    let no_routes_airport_time = Instant::now();
    let res = db.run_script(r#"
        ?[?code] := [?a airport.iata ?code], not [?_ route.src ?a], not [?_ route.dst ?a];
    "#)?;
    dbg!(no_routes_airport_time.elapsed());
    println!("{}", res);

    Ok(())
}