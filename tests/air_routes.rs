use std::fs::read_to_string;
use std::time::Instant;

use anyhow::Result;
use serde_json::json;

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
    let attr_res = db.run_tx_attributes(
        r#"
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
    "#,
    );

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
    assert_eq!(
        res,
        json!([[10000060, "CU", "Cuba"], [10000239, "VN", "Viet Nam"]])
    );

    let no_airports_time = Instant::now();
    let res = db.run_script(
        r#"
        ?[?desc] := [?c country.desc ?desc], not [?a airport.country ?c];
    "#,
    )?;
    dbg!(no_airports_time.elapsed());
    assert_eq!(
        res,
        json!([
            ["Andorra"],
            ["Liechtenstein"],
            ["Monaco"],
            ["Pitcairn"],
            ["San Marino"]
        ])
    );

    let no_routes_airport_time = Instant::now();
    let res = db.run_script(
        r#"
        ?[?code] := [?a airport.iata ?code], not [?_ route.src ?a], not [?_ route.dst ?a];
    "#,
    )?;
    dbg!(no_routes_airport_time.elapsed());
    assert_eq!(
        res,
        json!([
            ["AFW"],
            ["APA"],
            ["APK"],
            ["BID"],
            ["BVS"],
            ["BWU"],
            ["CRC"],
            ["CVT"],
            ["EKA"],
            ["GYZ"],
            ["HFN"],
            ["HZK"],
            ["ILG"],
            ["INT"],
            ["ISL"],
            ["KGG"],
            ["NBW"],
            ["NFO"],
            ["PSY"],
            ["RIG"],
            ["SFD"],
            ["SFH"],
            ["SXF"],
            ["TUA"],
            ["TWB"],
            ["TXL"],
            ["VCV"],
            ["YEI"]
        ])
    );

    let runway_distribution_time = Instant::now();
    let res = db.run_script(
        r#"
        ?[?runways, count(?a)] := [?a airport.runways ?runways];
    "#,
    )?;
    dbg!(runway_distribution_time.elapsed());
    assert_eq!(
        res,
        json!([
            [1, 2429],
            [2, 775],
            [3, 227],
            [4, 53],
            [5, 14],
            [6, 4],
            [7, 2]
        ])
    );

    let most_out_routes_time = Instant::now();
    let res = db.run_script(
        r#"
        route_count[?a, count(?r)] := [?r route.src ?a];
        ?[?code, ?n] := route_count[?a, ?n], ?n > 180, [?a airport.iata ?code];
        :sort -?n;
    "#,
    )?;
    dbg!(most_out_routes_time.elapsed());
    assert_eq!(
        res,
        json!([
            ["IST", 307],
            ["CDG", 293],
            ["AMS", 282],
            ["MUC", 270],
            ["ORD", 264],
            ["DFW", 251],
            ["PEK", 248],
            ["DXB", 247],
            ["ATL", 242],
            ["LGW", 232],
            ["LHR", 221],
            ["MAN", 216],
            ["LAX", 213],
            ["PVG", 212],
            ["STN", 211],
            ["VIE", 206],
            ["BCN", 203],
            ["BER", 202],
            ["JFK", 201],
            ["IAH", 199],
            ["EWR", 197],
            ["YYZ", 195],
            ["CPH", 194],
            ["DOH", 186],
            ["DUB", 185],
            ["CLT", 184],
            ["SVO", 181]
        ])
    );

    let most_routes_time = Instant::now();
    let res = db.run_script(
        r#"
        route_count[?a, count(?r)] := [?r route.src ?a] or [?r route.dst ?a];
        ?[?code, ?n] := route_count[?a, ?n], ?n > 400, [?a airport.iata ?code];
        :sort -?n;
    "#,
    )?;
    dbg!(most_routes_time.elapsed());
    assert_eq!(
        res,
        json!([
            ["IST", 614],
            ["CDG", 587],
            ["AMS", 566],
            ["MUC", 541],
            ["ORD", 527],
            ["DFW", 502],
            ["PEK", 497],
            ["DXB", 494],
            ["ATL", 484],
            ["DME", 465],
            ["LGW", 464],
            ["LHR", 442],
            ["DEN", 432],
            ["MAN", 431],
            ["LAX", 426],
            ["PVG", 424],
            ["STN", 423],
            ["VIE", 412],
            ["BCN", 406],
            ["BER", 404],
            ["FCO", 402],
            ["JFK", 401]
        ])
    );

    let airport_with_one_route_time = Instant::now();
    let res = db.run_script(
        r#"
        route_count[?a, count(?r)] := [?r route.src ?a];
        ?[count(?a)] := route_count[?a, ?n], ?n == 1;
    "#,
    )?;
    dbg!(airport_with_one_route_time.elapsed());
    assert_eq!(res, json!([[777]]));

    let single_runway_with_most_routes_time = Instant::now();
    let res = db.run_script(r#"
        single_or_lgw[?a] := [?a airport.iata 'LGW'] or [?a airport.runways 1];
        out_counts[?a, count(?r)] := single_or_lgw[?a], [?r route.src ?a];
        ?[?code, ?city, ?out_n] := out_counts[?a, ?out_n], [?a airport.city ?city], [?a airport.iata ?code];

        :order -?out_n;
        :limit 10;
    "#)?;
    dbg!(single_runway_with_most_routes_time.elapsed());
    assert_eq!(
        res,
        json!([
            ["LGW", "London", 232],
            ["STN", "London", 211],
            ["LIS", "Lisbon", 139],
            ["LTN", "London", 130],
            ["SZX", "Shenzhen", 129],
            ["CKG", "Chongqing", 122],
            ["STR", "Stuttgart", 121],
            ["XIY", "Xianyang", 117],
            ["KMG", "Kunming", 116],
            ["SAW", "Istanbul", 115]
        ])
    );

    let most_routes_in_canada_time = Instant::now();
    let res = db.run_script(r#"
        ca_airports[?a, count(?r)] := [?c country.code 'CA'], [?a airport.country ?c], [?r route.src ?a];
        ?[?code, ?city, ?n_routes] := ca_airports[?a, ?n_routes], [?a airport.iata ?code], [?a airport.city ?city];

        :order -?n_routes;
        :limit 10;
    "#)?;
    dbg!(most_routes_in_canada_time.elapsed());
    assert_eq!(
        res,
        json!([
            ["YYZ", "Toronto", 195],
            ["YUL", "Montreal", 121],
            ["YVR", "Vancouver", 105],
            ["YYC", "Calgary", 74],
            ["YEG", "Edmonton", 47],
            ["YHZ", "Halifax", 45],
            ["YWG", "Winnipeg", 38],
            ["YOW", "Ottawa", 36],
            ["YZF", "Yellowknife", 21],
            ["YQB", "Quebec City", 20]
        ])
    );

    let uk_count_time = Instant::now();
    let res =db.run_script(r"
        ?[?region, count(?a)] := [?c country.code 'UK'], [?a airport.country ?c], [?a airport.region ?region];
    ")?;
    dbg!(uk_count_time.elapsed());
    assert_eq!(
        res,
        json!([["GB-ENG", 27], ["GB-NIR", 3], ["GB-SCT", 25], ["GB-WLS", 3]])
    );

    Ok(())
}
