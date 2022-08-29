use std::fs::read_to_string;
use std::str::FromStr;
use std::time::Instant;

use anyhow::Result;
use num_traits::abs;
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
    let attr_res = db.run_script(
        r#"
        :schema

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
            contains: ref many,
        }
    "#,
    );

    if attr_res.is_ok() {
        let insertions = read_to_string("tests/air-routes-data.json")?;
        let triple_insertion_time = Instant::now();
        db.run_script(&insertions)?;
        dbg!(triple_insertion_time.elapsed());
    }

    let view_time = Instant::now();
    db.run_script(r#"
        ?[?src, ?dst, ?distance] := [?r route.src ?src], [?r route.dst ?dst], [?r route.distance ?distance];
        :view rederive flies_to;
    "#)?;

    dbg!(view_time.elapsed());

    let view_time2 = Instant::now();
    db.run_script(
        r#"
        ?[?src_c, ?dst_c, ?distance] := [?r route.src ?src], [?r route.dst ?dst],
                                        [?r route.distance ?distance],
                                        [?src airport.iata ?src_c], [?dst airport.iata ?dst_c];
        :view rederive flies_to_code;
    "#,
    )?;
    dbg!(view_time2.elapsed());

    let view_time3 = Instant::now();
    db.run_script(
        r#"
            ?[?code, ?lat, ?lon] := [?n airport.iata ?code], [?n airport.lat ?lat], [?n airport.lon ?lon];
            :view rederive code_lat_lon;
        "#
    )?;
    dbg!(view_time3.elapsed());

    println!("views: {}", db.list_views()?);

    let compact_main_time = Instant::now();
    db.compact_main()?;
    dbg!(compact_main_time.elapsed());

    let compact_view_time = Instant::now();
    db.compact_view()?;
    dbg!(compact_view_time.elapsed());

    let dfs_time = Instant::now();
    let res = db.run_script(r#"
        starting <- [['PEK']];
        ? <- dfs!(:flies_to_code[], [?id <airport.iata ?code], starting[], condition: (?code == 'LHR'));
    "#)?;
    dbg!(dfs_time.elapsed());
    println!("{}", res);

    let bfs_time = Instant::now();
    let res = db.run_script(r#"
        starting <- [['PEK']];
        ? <- bfs!(:flies_to_code[], [?id <airport.iata ?code], starting[], condition: ?code == 'SOU');
    "#)?;
    dbg!(bfs_time.elapsed());
    println!("{}", res);

    let scc_time = Instant::now();
    let res = db.run_script(r#"
        res <- strongly_connected_components!(:flies_to_code[], [?id <airport.iata ?code], mode: 'group_first');
        ?[?grp, ?code] := res[?grp, ?code], ?grp != 0;
    "#)?;
    println!("{}", res);
    dbg!(scc_time.elapsed());

    let cc_time = Instant::now();
    let res = db.run_script(r#"
        res <- connected_components!(:flies_to_code[], [?id <airport.iata ?code], mode: 'group_first');
        ?[?grp, ?code] := res[?grp, ?code], ?grp != 0;
    "#)?;
    println!("{}", res);
    dbg!(cc_time.elapsed());

    let astar_time = Instant::now();
    let res = db.run_script(r#"
        starting[?code, ?lat, ?lon] := ?code <- 'HFE', :code_lat_lon[?code, ?lat, ?lon];
        goal[?code, ?lat, ?lon] := ?code <- 'LHR', :code_lat_lon[?code, ?lat, ?lon];
        ? <- shortest_path_astar!(:flies_to_code[], :code_lat_lon[?node, ?lat1, ?lon1], starting[], goal[?goal, ?lat2, ?lon2], heuristic: haversine_deg_input(?lat1, ?lon1, ?lat2, ?lon2) * 3963);
    "#)?;
    println!("{}", res);
    dbg!(astar_time.elapsed());

    let deg_centrality_time = Instant::now();
    let res = db.run_script(
        r#"
        deg_centrality <- degree_centrality!(:flies_to[?a, ?b]);
        ?[?total, ?out, ?in] := deg_centrality[?node, ?total, ?out, ?in];
        :order -?total;
        :limit 10;
    "#,
    )?;

    dbg!(deg_centrality_time.elapsed());
    assert_eq!(
        *res.get("rows").unwrap(),
        serde_json::Value::from_str(
            r#"[
        [614,307,307],[587,293,294],[566,282,284],[541,270,271],[527,264,263],[502,251,251],
        [497,248,249],[494,247,247],[484,242,242],[465,232,233]]"#
        )?
    );

    let deg_centrality_ad_hoc_time = Instant::now();
    let res = db.run_script(
        r#"
        flies_to[?a, ?b] := [?r route.src ?ac], [?r route.dst ?bc],
                            [?ac airport.iata ?a], [?bc airport.iata ?b];
        deg_centrality <- degree_centrality!(flies_to[?a, ?b]);
        ?[?node, ?total, ?out, ?in] := deg_centrality[?node, ?total, ?out, ?in];
        :order -?total;
        :limit 10;
    "#,
    )?;

    dbg!(deg_centrality_ad_hoc_time.elapsed());
    assert_eq!(
        *res.get("rows").unwrap(),
        serde_json::Value::from_str(
            r#"[
            ["FRA",614,307,307],["IST",614,307,307],["CDG",587,293,294],["AMS",566,282,284],
            ["MUC",541,270,271],["ORD",527,264,263],["DFW",502,251,251],["PEK",497,248,249],
            ["DXB",494,247,247],["ATL",484,242,242]
            ]"#
        )?
    );

    let dijkstra_time = Instant::now();
    let res = db.run_script(
        r#"
        starting <- [['JFK']];
        ending <- [['KUL']];
        res <- shortest_path_dijkstra!(:flies_to_code[], starting[], ending[]);
        ?[?path] := res[?src, ?dst, ?cost, ?path];
    "#,
    )?;

    dbg!(dijkstra_time.elapsed());
    assert_eq!(*res.get("rows").unwrap(), json!([[["JFK", "CTU", "KUL"]]]));

    let yen_time = Instant::now();
    let res = db.run_script(
        r#"
        starting <- [['PEK']];
        ending <- [['SIN']];
        ? <- k_shortest_path_yen!(:flies_to_code[], starting[], ending[], k: 5);
    "#,
    )?;

    dbg!(yen_time.elapsed());
    println!("{}", res);

    let starts_with_time = Instant::now();
    let res = db.run_script(
        r#"
        ?[?code] := [?_ airport.iata ?code], starts_with(?code, 'US');
    "#,
    )?;
    dbg!(starts_with_time.elapsed());
    assert_eq!(
        *res.get("rows").unwrap(),
        json!([
            ["USA"],
            ["USH"],
            ["USJ"],
            ["USK"],
            ["USM"],
            ["USN"],
            ["USQ"],
            ["UST"],
            ["USU"]
        ])
    );

    let range_check_time = Instant::now();
    let res = db.run_script(
        r#"
        r[?code, ?dist] := [?a airport.iata ?code], [?r route.src ?a], [?r route.distance ?dist];
        ?[?dist] := r['PEK', ?dist], ?dist > 7000, ?dist <= 7722;
    "#,
    )?;
    dbg!(range_check_time.elapsed());
    assert_eq!(
        *res.get("rows").unwrap(),
        json!([[7176], [7270], [7311], [7722]])
    );

    let range_check_with_view_time = Instant::now();
    let res = db.run_script(
        r#"
        ?[?dist] := [?src airport.iata 'PEK'], :flies_to[?src, ?_, ?dist], ?dist > 7000, ?dist <= 7722;
    "#,
    )?;
    dbg!(range_check_with_view_time.elapsed());
    assert_eq!(
        *res.get("rows").unwrap(),
        json!([[7176], [7270], [7311], [7722]])
    );

    let simple_query_time = Instant::now();
    let res = db.run_script(r#"
        ?[?c, ?code, ?desc] := [?c country.code 'CU'] or ?c <- 10000239, [?c country.code ?code], [?c country.desc ?desc];
    "#)?;
    dbg!(simple_query_time.elapsed());
    assert_eq!(
        *res.get("rows").unwrap(),
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
        *res.get("rows").unwrap(),
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
        *res.get("rows").unwrap(),
        serde_json::Value::from_str(
            r#"[
            ["AFW"],["APA"],["APK"],["BID"],["BVS"],["BWU"],["CRC"],["CVT"],["EKA"],["GYZ"],
            ["HFN"],["HZK"],["ILG"],["INT"],["ISL"],["KGG"],["NBW"],["NFO"],["PSY"],["RIG"],
            ["SFD"],["SFH"],["SXF"],["TUA"],["TWB"],["TXL"],["VCV"],["YEI"]
        ]"#
        )
        .unwrap()
    );

    let runway_distribution_time = Instant::now();
    let res = db.run_script(
        r#"
        ?[?runways, count(?a)] := [?a airport.runways ?runways];
    "#,
    )?;
    dbg!(runway_distribution_time.elapsed());
    assert_eq!(
        *res.get("rows").unwrap(),
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
        *res.get("rows").unwrap(),
        serde_json::Value::from_str(
            r#"[
        ["FRA",307],["IST",307],["CDG",293],["AMS",282],["MUC",270],["ORD",264],["DFW",251],
        ["PEK",248],["DXB",247],["ATL",242],["DME",232],["LGW",232],["LHR",221],["DEN",216],
        ["MAN",216],["LAX",213],["PVG",212],["STN",211],["MAD",206],["VIE",206],["BCN",203],
        ["BER",202],["FCO",201],["JFK",201],["DUS",199],["IAH",199],["EWR",197],["MIA",195],
        ["YYZ",195],["BRU",194],["CPH",194],["DOH",186],["DUB",185],["CLT",184],["SVO",181]
        ]"#
        )
        .unwrap()
    );

    let most_out_routes_again_time = Instant::now();
    let res = db.run_script(
        r#"
        route_count[count(?r), ?a] := [?r route.src ?a];
        ?[?code, ?n] := route_count[?n, ?a], ?n > 180, [?a airport.iata ?code];
        :sort -?n;
    "#,
    )?;
    dbg!(most_out_routes_again_time.elapsed());
    assert_eq!(
        *res.get("rows").unwrap(),
        serde_json::Value::from_str(
            r#"[
        ["FRA",307],["IST",307],["CDG",293],["AMS",282],["MUC",270],["ORD",264],["DFW",251],
        ["PEK",248],["DXB",247],["ATL",242],["DME",232],["LGW",232],["LHR",221],["DEN",216],
        ["MAN",216],["LAX",213],["PVG",212],["STN",211],["MAD",206],["VIE",206],["BCN",203],
        ["BER",202],["FCO",201],["JFK",201],["DUS",199],["IAH",199],["EWR",197],["MIA",195],
        ["YYZ",195],["BRU",194],["CPH",194],["DOH",186],["DUB",185],["CLT",184],["SVO",181]
        ]"#
        )
        .unwrap()
    );

    let most_out_routes_time_inv = Instant::now();
    let res = db.run_script(
        r#"
        route_count[count(?r), ?a, ?x] := [?r route.src ?a], ?x <- 1;
        ?[?code, ?n] := route_count[?n, ?a, ?_], ?n > 180, [?a airport.iata ?code];
        :sort -?n;
    "#,
    )?;
    dbg!(most_out_routes_time_inv.elapsed());
    assert_eq!(
        *res.get("rows").unwrap(),
        serde_json::Value::from_str(
            r#"[
        ["FRA",307],["IST",307],["CDG",293],["AMS",282],["MUC",270],["ORD",264],["DFW",251],
        ["PEK",248],["DXB",247],["ATL",242],["DME",232],["LGW",232],["LHR",221],["DEN",216],
        ["MAN",216],["LAX",213],["PVG",212],["STN",211],["MAD",206],["VIE",206],["BCN",203],
        ["BER",202],["FCO",201],["JFK",201],["DUS",199],["IAH",199],["EWR",197],["MIA",195],
        ["YYZ",195],["BRU",194],["CPH",194],["DOH",186],["DUB",185],["CLT",184],["SVO",181]
        ]"#
        )
        .unwrap()
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
        *res.get("rows").unwrap(),
        serde_json::Value::from_str(
            r#"[
        ["FRA",614],["IST",614],["CDG",587],["AMS",566],["MUC",541],["ORD",527],["DFW",502],
        ["PEK",497],["DXB",494],["ATL",484],["DME",465],["LGW",464],["LHR",442],["DEN",432],
        ["MAN",431],["LAX",426],["PVG",424],["STN",423],["MAD",412],["VIE",412],["BCN",406],
        ["BER",404],["FCO",402],["JFK",401]]"#
        )
        .unwrap()
    );

    let airport_with_one_route_time = Instant::now();
    let res = db.run_script(
        r#"
        route_count[?a, count(?r)] := [?r route.src ?a];
        ?[count(?a)] := route_count[?a, ?n], ?n == 1;
    "#,
    )?;
    dbg!(airport_with_one_route_time.elapsed());
    assert_eq!(*res.get("rows").unwrap(), json!([[777]]));

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
        *res.get("rows").unwrap(),
        serde_json::Value::from_str(
            r#"[
        ["LGW","London",232],["STN","London",211],["CTU","Chengdu",139],["LIS","Lisbon",139],
        ["BHX","Birmingham",130],["LTN","London",130],["SZX","Shenzhen",129],
        ["CKG","Chongqing",122],["STR","Stuttgart",121],["CRL","Brussels",117]]"#
        )
        .unwrap()
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
        *res.get("rows").unwrap(),
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
    let res = db.run_script(r"
        ?[?region, count(?a)] := [?c country.code 'UK'], [?a airport.country ?c], [?a airport.region ?region];
    ")?;
    dbg!(uk_count_time.elapsed());
    assert_eq!(
        *res.get("rows").unwrap(),
        json!([["GB-ENG", 27], ["GB-NIR", 3], ["GB-SCT", 25], ["GB-WLS", 3]])
    );

    let airports_by_country = Instant::now();
    let res = db.run_script(
        r"
        airports_by_country[?c, count(?a)] := [?a airport.country ?c];
        country_count[?c, max(?count)] := airports_by_country[?c, ?count];
        ?[?code, ?count] := [?c country.code ?code], country_count[?c, ?count];
        ?[?code, ?count] := [?c country.code ?code], not country_count[?c, ?_], ?count <- 0;

        :order ?count;
    ",
    )?;
    dbg!(airports_by_country.elapsed());
    assert_eq!(
        *res.get("rows").unwrap(),
        serde_json::Value::from_str(
            r#"[
    ["AD",0],["LI",0],["MC",0],["PN",0],["SM",0],["AG",1],["AI",1],["AL",1],["AS",1],["AW",1],
    ["BB",1],["BH",1],["BI",1],["BJ",1],["BL",1],["BM",1],["BN",1],["BT",1],["CC",1],["CF",1],
    ["CW",1],["CX",1],["DJ",1],["DM",1],["ER",1],["FO",1],["GD",1],["GF",1],["GI",1],["GM",1],
    ["GN",1],["GP",1],["GU",1],["GW",1],["HK",1],["IM",1],["JE",1],["KM",1],["KP",1],["KS",1],
    ["KW",1],["LB",1],["LS",1],["LU",1],["LV",1],["MD",1],["MF",1],["ML",1],["MO",1],["MQ",1],
    ["MS",1],["MT",1],["NC",1],["NE",1],["NF",1],["NI",1],["NR",1],["PM",1],["PW",1],["QA",1],
    ["SL",1],["SR",1],["SS",1],["ST",1],["SV",1],["SX",1],["SZ",1],["TG",1],["TL",1],["TM",1],
    ["TV",1],["VC",1],["WS",1],["YT",1],["AM",2],["BF",2],["CI",2],["EH",2],["FK",2],["GA",2],
    ["GG",2],["GQ",2],["GT",2],["GY",2],["HT",2],["HU",2],["JM",2],["JO",2],["KG",2],["KI",2],
    ["KN",2],["LC",2],["LR",2],["ME",2],["MH",2],["MK",2],["MP",2],["MU",2],["PY",2],["RE",2],
    ["RW",2],["SC",2],["SG",2],["SH",2],["SI",2],["SK",2],["SY",2],["TT",2],["UY",2],["VG",2],
    ["VI",2],["WF",2],["BQ",3],["BY",3],["CG",3],["CY",3],["EE",3],["GE",3],["KH",3],["KY",3],
    ["LT",3],["MR",3],["RS",3],["ZW",3],["BA",4],["BG",4],["BW",4],["FM",4],["OM",4],["SN",4],
    ["TC",4],["TJ",4],["UG",4],["AF",5],["AZ",5],["BE",5],["CM",5],["CZ",5],["NL",5],["PA",5],
    ["SD",5],["TD",5],["TO",5],["AT",6],["CH",6],["CK",6],["GH",6],["HN",6],["IL",6],["IQ",6],
    ["LK",6],["SO",6],["BD",7],["CV",7],["DO",7],["IE",7],["IS",7],["MW",7],["PR",7],["DK",8],
    ["HR",8],["LA",8],["MV",8],["TN",8],["TW",9],["YE",9],["ZM",9],["AE",10],["FJ",10],["MN",10],
    ["CD",11],["EG",11],["LY",11],["MZ",11],["NP",11],["TZ",11],["UZ",11],["CU",12],["BZ",13],
    ["CR",13],["MG",13],["PL",13],["AO",14],["GL",14],["KE",14],["RO",14],["BO",15],["EC",15],
    ["KR",15],["UA",15],["ET",16],["MA",16],["CL",17],["MM",17],["SB",17],["BS",18],["NG",19],
    ["PT",19],["FI",20],["ZA",20],["KZ",21],["PK",21],["PE",22],["VN",22],["NZ",25],["PG",26],
    ["SA",26],["VU",26],["VE",27],["DZ",30],["TH",33],["DE",34],["MY",35],["AR",38],["IT",38],
    ["GR",39],["PF",39],["SE",39],["PH",40],["ES",43],["IR",45],["NO",49],["CO",51],["TR",52],
    ["UK",58],["FR",59],["MX",60],["JP",65],["ID",70],["IN",77],["BR",117],["RU",129],["AU",132],
    ["CA",205],["CN",217],["US",586]]"#
        )
        .unwrap()
    );

    let n_airports_by_continent_time = Instant::now();
    let res = db.run_script(
        r#"
        airports_by_continent[?c, count(?a)] := [?a airport.iata ?_], [?c geo.contains ?a];
        ?[?cont, max(?count)] := airports_by_continent[?c, ?count], [?c continent.code ?cont];
        ?[?cont, max(?count)] := [?_ continent.code ?cont], ?count <- 0;
    "#,
    )?;
    dbg!(n_airports_by_continent_time.elapsed());
    assert_eq!(
        *res.get("rows").unwrap(),
        serde_json::Value::from_str(
            r#"[["AF",321],["AN",0],["AS",971],["EU",605],["NA",989],["OC",305],["SA",313]]"#
        )
        .unwrap()
    );

    let routes_per_airport_time = Instant::now();
    let res = db.run_script(
        r#"
        routes_count[?a, count(?r)] := given[?code], [?a airport.iata ?code], [?r route.src ?a];
        ?[?code, ?n] := routes_count[?a, ?n], [?a airport.iata ?code];

        given <- [['A' ++ 'U' ++ 'S'],['AMS'],['JFK'],['DUB'],['MEX']];
        "#,
    )?;
    dbg!(routes_per_airport_time.elapsed());
    assert_eq!(
        *res.get("rows").unwrap(),
        serde_json::Value::from_str(
            r#"[["AMS",282],["AUS",95],["DUB",185],["JFK",201],["MEX",116]]"#
        )
        .unwrap()
    );

    let airports_by_route_number_time = Instant::now();
    let res = db.run_script(
        r#"
        route_count[?a, count(?r)] := [?r route.src ?a];
        ?[?n, collect(?code)] := route_count[?a, ?n], [?a airport.iata ?code], ?n = 105;
    "#,
    )?;
    dbg!(airports_by_route_number_time.elapsed());
    assert_eq!(*res.get("rows").unwrap(), json!([[105, ["TFS", "YVR"]]]));

    let out_from_aus_time = Instant::now();
    let res = db.run_script(r#"
        out_by_runways[?n_runways, count(?a)] := [?aus airport.iata 'AUS'],
                                                 [?r1 route.src ?aus],
                                                 [?r1 route.dst ?a],
                                                 [?a airport.runways ?n_runways];
        two_hops[count(?a)] := [?aus airport.iata 'AUS'],
                               [?r1 route.src ?aus],
                               [?r1 route.dst ?a],
                               [?r route.src ?a];
        ?[max(?total), collect(?coll)] := two_hops[?total], out_by_runways[?n, ?ct], ?coll <- [?n, ?ct];
    "#)?;
    dbg!(out_from_aus_time.elapsed());
    assert_eq!(
        *res.get("rows").unwrap(),
        serde_json::Value::from_str(r#"[[7909,[[1,9],[2,23],[3,29],[4,24],[5,5],[6,3],[7,2]]]]"#)
            .unwrap()
    );

    let const_return_time = Instant::now();
    let res = db.run_script(
        r#"
        ?[?name, count(?a)] := [?a airport.region 'US-OK'], ?name <- 'OK';
    "#,
    )?;
    dbg!(const_return_time.elapsed());
    assert_eq!(*res.get("rows").unwrap(), json!([["OK", 4]]));

    let multi_res_time = Instant::now();
    let res = db.run_script(
        r#"
        total[count(?a)] := [?a airport.iata ?_];
        high[count(?a)] := [?a airport.runways ?n], ?n >= 6;
        low[count(?a)] := [?a airport.runways ?n], ?n <= 2;
        four[count(?a)] := [?a airport.runways ?n], ?n = 4;
        france[count(?a)] := [?fr country.code 'FR'], [?a airport.country ?fr];

        ?[?total, ?high, ?low, ?four, ?france] := total[?total], high[?high], low[?low],
                                                  four[?four], france[?france];
    "#,
    )?;
    dbg!(multi_res_time.elapsed());
    assert_eq!(
        *res.get("rows").unwrap(),
        serde_json::Value::from_str(r#"[[3504,6,3204,53,59]]"#).unwrap()
    );

    let multi_unification_time = Instant::now();
    let res = db.run_script(r#"
        target_airports[collect(?a, 5)] := [?a airport.iata ?_];
        ?[?code, count(?r)] := target_airports[?targets], ?a <- ..?targets, [?a airport.iata ?code], [?r route.src ?a];
    "#)?;
    dbg!(multi_unification_time.elapsed());
    assert_eq!(
        *res.get("rows").unwrap(),
        serde_json::Value::from_str(
            r#"[["ANC",41],["ATL",242],["AUS",95],["BNA",74],["BOS",141]]"#
        )
        .unwrap()
    );

    let num_routes_from_eu_to_us_time = Instant::now();
    let res = db.run_script(
        r#"
        routes[unique(?r)] := [?eu continent.code 'EU'],
                              [?us country.code 'US'],
                              [?eu geo.contains ?a],
                              [?r route.src ?a],
                              [?r route.dst ?a2],
                              [?a2 airport.country ?us];
        ?[?n] := routes[?rs], ?n <- length(?rs);
    "#,
    )?;
    dbg!(num_routes_from_eu_to_us_time.elapsed());
    assert_eq!(*res.get("rows").unwrap(), json!([[417]]));

    let num_airports_in_us_with_routes_from_eu_time = Instant::now();
    let res = db.run_script(
        r#"
        ?[count_unique(?a2)] := [?eu continent.code 'EU'],
                                [?us country.code 'US'],
                                [?eu geo.contains ?a],
                                [?r route.src ?a],
                                [?r route.dst ?a2],
                                [?a2 airport.country ?us];
    "#,
    )?;
    dbg!(num_airports_in_us_with_routes_from_eu_time.elapsed());
    assert_eq!(*res.get("rows").unwrap(), json!([[45]]));

    let num_routes_in_us_airports_from_eu_time = Instant::now();
    let res = db.run_script(
        r#"
        ?[?code, count(?r)] := [?eu continent.code 'EU'],
                               [?us country.code 'US'],
                               [?eu geo.contains ?a],
                               [?r route.src ?a],
                               [?r route.dst ?a2],
                               [?a2 airport.country ?us],
                               [?a2 airport.iata ?code];
        :order ?r;
    "#,
    )?;
    dbg!(num_routes_in_us_airports_from_eu_time.elapsed());
    assert_eq!(
        *res.get("rows").unwrap(),
        serde_json::Value::from_str(
            r#"
    [["ANC",1],["BNA",1],["CHS",1],["CLE",1],["IND",1],["MCI",1],["STL",1],["BDL",2],["BWI",2],
     ["CVG",2],["MSY",2],["PHX",2],["RDU",2],["SJC",2],["AUS",3],["PDX",3],["RSW",3],["SAN",3],
     ["SLC",3],["PIT",4],["SFB",5],["SWF",5],["TPA",5],["DTW",6],["MSP",6],["OAK",6],["DEN",7],
     ["FLL",7],["PVD",7],["CLT",8],["IAH",8],["DFW",10],["SEA",10],["LAS",11],["MCO",13],["ATL",15],
     ["SFO",20],["IAD",21],["PHL",22],["BOS",25],["LAX",25],["ORD",27],["MIA",28],["EWR",38],
     ["JFK",42]]"#
        )
        .unwrap()
    );

    let routes_from_eu_to_us_starting_with_l_time = Instant::now();
    let res = db.run_script(
        r#"
        ?[?eu_code, ?us_code] := [?eu continent.code 'EU'],
                               [?us country.code 'US'],
                               [?eu geo.contains ?a],
                               [?a airport.iata ?eu_code],
                               starts_with(?eu_code, 'L'),
                               [?r route.src ?a],
                               [?r route.dst ?a2],
                               [?a2 airport.country ?us],
                               [?a2 airport.iata ?us_code];
    "#,
    )?;
    dbg!(routes_from_eu_to_us_starting_with_l_time.elapsed());
    assert_eq!(
        *res.get("rows").unwrap(),
        serde_json::Value::from_str(
            r#"[
    ["LGW","AUS"],["LGW","BOS"],["LGW","DEN"],["LGW","FLL"],["LGW","JFK"],["LGW","LAS"],
    ["LGW","LAX"],["LGW","MCO"],["LGW","MIA"],["LGW","OAK"],["LGW","ORD"],["LGW","SEA"],
    ["LGW","SFO"],["LGW","TPA"],["LHR","ATL"],["LHR","AUS"],["LHR","BNA"],["LHR","BOS"],
    ["LHR","BWI"],["LHR","CHS"],["LHR","CLT"],["LHR","DEN"],["LHR","DFW"],["LHR","DTW"],
    ["LHR","EWR"],["LHR","IAD"],["LHR","IAH"],["LHR","JFK"],["LHR","LAS"],["LHR","LAX"],
    ["LHR","MIA"],["LHR","MSP"],["LHR","MSY"],["LHR","ORD"],["LHR","PDX"],["LHR","PHL"],
    ["LHR","PHX"],["LHR","PIT"],["LHR","RDU"],["LHR","SAN"],["LHR","SEA"],["LHR","SFO"],
    ["LHR","SJC"],["LHR","SLC"],["LIS","ATL"],["LIS","BOS"],["LIS","EWR"],["LIS","IAD"],
    ["LIS","JFK"],["LIS","MIA"],["LIS","ORD"],["LIS","PHL"],["LIS","SFO"]]"#
        )
        .unwrap()
    );

    let len_of_names_count_time = Instant::now();
    let res = db.run_script(
        r#"
        ?[sum(?n)] := [?a airport.iata 'AUS'],
                      [?r route.src ?a],
                      [?r route.dst ?a2],
                      [?a2 airport.city ?city_name],
                      ?n <- length(?city_name);
    "#,
    )?;
    dbg!(len_of_names_count_time.elapsed());
    assert_eq!(*res.get("rows").unwrap(), json!([[866.0]]));

    let group_count_by_out_time = Instant::now();
    let res = db.run_script(
        r#"
        route_count[count(?r), ?a] := [?r route.src ?a];
        rc[max(?n), ?a] := route_count[?n, ?a];
        rc[max(?n), ?a] := [?a airport.iata ?_], ?n <- 0;
        ?[?n, count(?a)] := rc[?n, ?a];
        :order ?n;
        :limit 10;
    "#,
    )?;
    dbg!(group_count_by_out_time.elapsed());
    assert_eq!(
        *res.get("rows").unwrap(),
        serde_json::Value::from_str(
            r#"[[0,29],[1,777],[2,649],[3,359],[4,232],[5,150],[6,139],[7,100],[8,74],[9,63]]"#
        )
        .unwrap()
    );

    let mean_group_count_time = Instant::now();
    let res = db.run_script(
        r#"
        route_count[count(?r), ?a] := [?r route.src ?a];
        rc[max(?n), ?a] := route_count[?n, ?a] or ([?a airport.iata ?_], ?n <- 0);
        ?[mean(?n)] := rc[?n, ?_];
    "#,
    )?;
    dbg!(mean_group_count_time.elapsed());
    let v = res
        .get("rows")
        .unwrap()
        .as_array()
        .unwrap()
        .get(0)
        .unwrap()
        .as_array()
        .unwrap()
        .get(0)
        .unwrap()
        .as_f64()
        .unwrap();
    let expected = 14.425513698630137;
    assert!(abs(v - expected) < 1e-8);

    let n_routes_from_london_uk_time = Instant::now();
    let res = db.run_script(r#"
        ?[?code, count(?r)] := [?a airport.city 'London'], [?a airport.region 'GB-ENG'], [?r route.src ?a], [?a airport.iata ?code];
    "#)?;
    dbg!(n_routes_from_london_uk_time.elapsed());
    assert_eq!(
        *res.get("rows").unwrap(),
        serde_json::Value::from_str(
            r#"[["LCY",51],["LGW",232],["LHR",221],["LTN",130],["STN",211]]"#
        )
        .unwrap()
    );

    let reachable_from_london_uk_in_two_hops_time = Instant::now();
    let res = db.run_script(r#"
        lon_uk_airports[?a] := [?a airport.city 'London'], [?a airport.region 'GB-ENG'];
        one_hop[?a2] := lon_uk_airports[?a], [?r route.src ?a], [?r route.dst ?a2], not lon_uk_airports[?a2];
        ?[count_unique(?a3)] := one_hop[?a2], [?r2 route.src ?a2], [?r2 route.dst ?a3], not lon_uk_airports[?a3];
    "#)?;
    dbg!(reachable_from_london_uk_in_two_hops_time.elapsed());
    assert_eq!(*res.get("rows").unwrap(), json!([[2353]]));

    let routes_within_england_time = Instant::now();
    let res = db.run_script(
        r#"
        eng_aps[?a] := [?a airport.region 'GB-ENG'];
        ?[?src, ?dst] := eng_aps[?a1], [?r route.src ?a1], [?r route.dst ?a2], eng_aps[?a2],
                         [?a1 airport.iata ?src], [?a2 airport.iata ?dst];
    "#,
    )?;
    dbg!(routes_within_england_time.elapsed());
    assert_eq!(
        *res.get("rows").unwrap(),
        serde_json::Value::from_str(
            r#"[
    ["BHX","NCL"],["BRS","NCL"],["EMA","SOU"],["EXT","ISC"],["EXT","MAN"],["EXT","NQY"],
    ["HUY","NWI"],["ISC","EXT"],["ISC","LEQ"],["ISC","NQY"],["LBA","LHR"],["LBA","NQY"],
    ["LBA","SOU"],["LCY","MAN"],["LCY","NCL"],["LEQ","ISC"],["LGW","NCL"],["LGW","NQY"],
    ["LHR","LBA"],["LHR","MAN"],["LHR","NCL"],["LHR","NQY"],["LPL","NQY"],["MAN","EXT"],
    ["MAN","LCY"],["MAN","LHR"],["MAN","NQY"],["MAN","NWI"],["MAN","SEN"],["MAN","SOU"],
    ["MME","NWI"],["NCL","BHX"],["NCL","BRS"],["NCL","LCY"],["NCL","LGW"],["NCL","LHR"],
    ["NCL","SOU"],["NQY","EXT"],["NQY","ISC"],["NQY","LBA"],["NQY","LGW"],["NQY","LHR"],
    ["NQY","LPL"],["NQY","MAN"],["NQY","SEN"],["NWI","HUY"],["NWI","MAN"],["NWI","MME"],
    ["SEN","MAN"],["SEN","NQY"],["SOU","EMA"],["SOU","LBA"],["SOU","MAN"],["SOU","NCL"]]"#
        )
        .unwrap()
    );

    let routes_within_england_time_no_dup = Instant::now();
    let res = db.run_script(
        r#"
        eng_aps[?a] := [?a airport.region 'GB-ENG'];
        ?[?pair] := eng_aps[?a1], [?r route.src ?a1], [?r route.dst ?a2], eng_aps[?a2],
                         [?a1 airport.iata ?src], [?a2 airport.iata ?dst],
                         ?pair <- sort([?src, ?dst]);
    "#,
    )?;
    dbg!(routes_within_england_time_no_dup.elapsed());
    assert_eq!(
        *res.get("rows").unwrap(),
        serde_json::Value::from_str(
            r#"[
    [["BHX","NCL"]],[["BRS","NCL"]],[["EMA","SOU"]],[["EXT","ISC"]],[["EXT","MAN"]],[["EXT","NQY"]],
    [["HUY","NWI"]],[["ISC","LEQ"]],[["ISC","NQY"]],[["LBA","LHR"]],[["LBA","NQY"]],[["LBA","SOU"]],
    [["LCY","MAN"]],[["LCY","NCL"]],[["LGW","NCL"]],[["LGW","NQY"]],[["LHR","MAN"]],[["LHR","NCL"]],
    [["LHR","NQY"]],[["LPL","NQY"]],[["MAN","NQY"]],[["MAN","NWI"]],[["MAN","SEN"]],[["MAN","SOU"]],
    [["MME","NWI"]],[["NCL","SOU"]],[["NQY","SEN"]]]"#
        )
        .unwrap()
    );

    let hard_route_finding_time = Instant::now();
    let res = db.run_script(
        r#"
        reachable[?a, choice(?p)] := [?s airport.iata 'AUS'],
                                     [?r route.src ?s], [?r route.dst ?a],
                                     [?a airport.iata ?code], ?code != 'YYZ', ?p <- ['AUS', ?code];
        reachable[?a, choice(?p)] := reachable[?b, ?prev],
                                     [?r route.src ?b], [?r route.dst ?a], [?a airport.iata ?code],
                                     ?code != 'YYZ', ?p <- append(?prev, ?code);
        ?[?p] := reachable[?a, ?p], [?a airport.iata 'YPO'];

        :limit 1;
    "#,
    )?;
    dbg!(hard_route_finding_time.elapsed());
    assert_eq!(
        *res.get("rows").unwrap(),
        serde_json::Value::from_str(
            r#"[[["AUS","BOS","YTZ","YTS","YMO","YFA","ZKE","YAT","YPO"]]]"#
        )
        .unwrap()
    );

    let na_from_india_time = Instant::now();
    let res = db.run_script(
        r#"
        ?[?ind_c, ?na_c] := [?india country.code 'IN'], [?ind_a airport.country ?india],
                            [?r route.src ?ind_a], [?r route.dst ?na_a],
                            [?na_a airport.country ?dst_country],
                            [?dst_country country.code ?dst_country_name],
                            ?dst_country_name <- ..['US', 'CA'],
                            [?ind_a airport.iata ?ind_c], [?na_a airport.iata ?na_c];

    "#,
    )?;
    dbg!(na_from_india_time.elapsed());
    assert_eq!(
        *res.get("rows").unwrap(),
        serde_json::Value::from_str(
            r#"[
    ["BOM","EWR"],["BOM","JFK"],["BOM","YYZ"],["DEL","EWR"],["DEL","IAD"],["DEL","JFK"],
    ["DEL","ORD"],["DEL","SFO"],["DEL","YVR"],["DEL","YYZ"]]"#
        )
        .unwrap()
    );

    let eu_cities_reachable_from_fll_time = Instant::now();
    let res = db.run_script(
        r#"
        ?[?city_name] := [?a airport.iata 'FLL'],
                         [?r route.src ?a],
                         [?r route.dst ?a2],
                         [?cont geo.contains ?a2],
                         [?cont continent.code 'EU'],
                         [?a2 airport.city ?city_name];
    "#,
    )?;
    dbg!(eu_cities_reachable_from_fll_time.elapsed());
    assert_eq!(
        *res.get("rows").unwrap(),
        serde_json::Value::from_str(
            r#"[
    ["Barcelona"],["Copenhagen"],["London"],["Madrid"],["Oslo"],["Paris"],["Stockholm"]]"#
        )
        .unwrap()
    );

    let clt_to_eu_or_sa_time = Instant::now();
    let res = db.run_script(
        r#"
        ?[?code] := [?a airport.iata 'CLT'], [?r route.src ?a], [?r route.dst ?a2],
                    [?cont geo.contains ?a2], [?cont continent.code ?c_name],
                    ?c_name <- ..['EU', 'SA'],
                    [?a2 airport.iata ?code];
    "#,
    )?;
    dbg!(clt_to_eu_or_sa_time.elapsed());
    assert_eq!(
        *res.get("rows").unwrap(),
        serde_json::Value::from_str(
            r#"[["BCN"],["CDG"],["DUB"],["FCO"],["FRA"],["GIG"],["GRU"],["LHR"],["MAD"],["MUC"]]"#
        )
        .unwrap()
    );

    let london_to_us_time = Instant::now();
    let res = db.run_script(
        r#"
        ?[?l_code, ?us_code] := ?l_code <- ..['LHR', 'LCY', 'LGW', 'LTN', 'STN'],
                                [?a airport.iata ?l_code],
                                [?r route.src ?a], [?r route.dst ?a2],
                                [?us country.code 'US'],
                                [?a2 airport.country ?us],
                                [?a2 airport.iata ?us_code];
    "#,
    )?;
    dbg!(london_to_us_time.elapsed());
    assert_eq!(
        *res.get("rows").unwrap(),
        serde_json::Value::from_str(
            r#"
    [["LGW","AUS"],["LGW","BOS"],["LGW","DEN"],["LGW","FLL"],["LGW","JFK"],["LGW","LAS"],
     ["LGW","LAX"],["LGW","MCO"],["LGW","MIA"],["LGW","OAK"],["LGW","ORD"],["LGW","SEA"],
     ["LGW","SFO"],["LGW","TPA"],["LHR","ATL"],["LHR","AUS"],["LHR","BNA"],["LHR","BOS"],
     ["LHR","BWI"],["LHR","CHS"],["LHR","CLT"],["LHR","DEN"],["LHR","DFW"],["LHR","DTW"],
     ["LHR","EWR"],["LHR","IAD"],["LHR","IAH"],["LHR","JFK"],["LHR","LAS"],["LHR","LAX"],
     ["LHR","MIA"],["LHR","MSP"],["LHR","MSY"],["LHR","ORD"],["LHR","PDX"],["LHR","PHL"],
     ["LHR","PHX"],["LHR","PIT"],["LHR","RDU"],["LHR","SAN"],["LHR","SEA"],["LHR","SFO"],
     ["LHR","SJC"],["LHR","SLC"],["STN","BOS"],["STN","EWR"],["STN","IAD"],["STN","SFB"]]
    "#
        )
        .unwrap()
    );

    let tx_to_ny_time = Instant::now();
    let res = db.run_script(
        r#"
        ?[?tx_code, ?ny_code] := [?a airport.region 'US-TX'],
                                 [?r route.src ?a],
                                 [?r route.dst ?a2],
                                 [?a2 airport.region 'US-NY'],
                                 [?a airport.iata ?tx_code],
                                 [?a2 airport.iata ?ny_code];
    "#,
    )?;
    dbg!(tx_to_ny_time.elapsed());
    assert_eq!(
        *res.get("rows").unwrap(),
        serde_json::Value::from_str(
            r#"
    [["AUS","BUF"],["AUS","EWR"],["AUS","JFK"],["DAL","LGA"],["DFW","BUF"],["DFW","EWR"],
     ["DFW","JFK"],["DFW","LGA"],["HOU","EWR"],["HOU","JFK"],["HOU","LGA"],["IAH","EWR"],
     ["IAH","JFK"],["IAH","LGA"],["SAT","EWR"],["SAT","JFK"]]
    "#
        )
        .unwrap()
    );

    let denver_to_mexico_time = Instant::now();
    let res = db.run_script(
        r#"
        ?[?city_name] := [?a airport.iata 'DEN'], [?r route.src ?a], [?r route.dst ?a2],
                         [?a2 airport.country ?ct],
                         [?ct country.code 'MX'],
                         [?a2 airport.city ?city_name];
    "#,
    )?;
    dbg!(denver_to_mexico_time.elapsed());
    assert_eq!(
        *res.get("rows").unwrap(),
        serde_json::Value::from_str(
            r#"[
    ["Cancun"],["Cozumel"],["Guadalajara"],["Mexico City"],["Monterrey"],
    ["Puerto Vallarta"],["San José del Cabo"]]"#
        )
        .unwrap()
    );

    let three_cities_time = Instant::now();
    let res = db.run_script(
        r#"
        three[?a] := ?city <- ..['London', 'Munich', 'Paris'], [?a airport.city ?city];
        ?[?src, ?dst] := three[?s], [?r route.src ?s], [?r route.dst ?d], three[?d],
                         [?s airport.iata ?src], [?d airport.iata ?dst];
    "#,
    )?;
    dbg!(three_cities_time.elapsed());
    assert_eq!(
        *res.get("rows").unwrap(),
        serde_json::Value::from_str(
            r#"[
    ["CDG","LCY"],["CDG","LGW"],["CDG","LHR"],["CDG","LTN"],["CDG","MUC"],["LCY","CDG"],
    ["LCY","MUC"],["LCY","ORY"],["LGW","CDG"],["LGW","MUC"],["LHR","CDG"],["LHR","MUC"],
    ["LHR","ORY"],["LTN","CDG"],["LTN","MUC"],["LTN","ORY"],["MUC","CDG"],["MUC","LCY"],
    ["MUC","LGW"],["MUC","LHR"],["MUC","LTN"],["MUC","ORY"],["MUC","STN"],["ORY","LCY"],
    ["ORY","LHR"],["ORY","MUC"],["STN","MUC"]]"#
        )
        .unwrap()
    );

    let long_distance_from_lgw_time = Instant::now();
    let res = db.run_script(
        r#"
        ?[?city, ?dist] := [?a airport.iata 'LGW'], [?r route.src ?a], [?r route.dst ?a2],
                           [?r route.distance ?dist], ?dist > 4000, [?a2 airport.city ?city];
    "#,
    )?;
    dbg!(long_distance_from_lgw_time.elapsed());
    assert_eq!(
        *res.get("rows").unwrap(),
        serde_json::Value::from_str(
            r#"
    [["Austin",4921],["Beijing",5070],["Bridgetown",4197],["Buenos Aires",6908],["Calgary",4380],
    ["Cancun",4953],["Cape Town",5987],["Chengdu",5156],["Chongqing",5303],["Colombo",5399],
    ["Denver",4678],["Duong Dong",6264],["Fort Lauderdale",4410],["Havana",4662],["Hong Kong",5982],
    ["Kigali",4077],["Kingston",4680],["Langkawi",6299],["Las Vegas",5236],["Los Angeles",5463],
    ["Malé",5287],["Miami",4429],["Montego Bay",4699],["Oakland",5364],["Orlando",4341],
    ["Port Louis",6053],["Port of Spain",4408],["Punta Cana",4283],["Rayong",6008],
    ["Rio de Janeiro",5736],["San Francisco",5374],["San Jose",5419],["Seattle",4807],
    ["Shanghai",5745],["Singapore",6751],["St. George",4076],["Taipei",6080],["Tampa",4416],
    ["Tianjin",5147],["Vancouver",4731],["Varadero",4618],["Vieux Fort",4222]]"#
        )
        .unwrap()
    );

    let long_routes_one_dir_time = Instant::now();
    let res = db.run_script(
        r#"
        ?[?src, ?dist, ?dst] := [?r route.distance ?dist], ?dist > 8000, [?r route.src ?s],
                                [?r route.dst ?d], [?s airport.iata ?src], [?d airport.iata ?dst],
                                ?src < ?dst;
    "#,
    )?;
    dbg!(long_routes_one_dir_time.elapsed());
    assert_eq!(
        *res.get("rows").unwrap(),
        serde_json::Value::from_str(
            r#"
    [["AKL",8186,"ORD"],["AKL",8818,"DXB"],["AKL",9025,"DOH"],["ATL",8434,"JNB"],["AUH",8053,"DFW"],
    ["AUH",8139,"SFO"],["AUH",8372,"LAX"],["CAN",8754,"MEX"],["DFW",8022,"DXB"],["DFW",8105,"HKG"],
    ["DFW",8574,"SYD"],["DOH",8030,"IAH"],["DOH",8287,"LAX"],["DXB",8085,"SFO"],["DXB",8150,"IAH"],
    ["DXB",8321,"LAX"],["EWR",8047,"HKG"],["EWR",9523,"SIN"],["HKG",8054,"JFK"],["HKG",8135,"IAD"],
    ["IAH",8591,"SYD"],["JED",8314,"LAX"],["JFK",8504,"MNL"],["LAX",8246,"RUH"],["LAX",8756,"SIN"],
    ["LHR",9009,"PER"],["MEL",8197,"YVR"],["PEK",8884,"PTY"],["SCL",8208,"TLV"],["SEA",8059,"SIN"],
    ["SFO",8433,"SIN"]]"#
        )
        .unwrap()
    );

    let longest_routes_time = Instant::now();
    let res = db.run_script(
        r#"
        ?[?src, ?dist, ?dst] := [?r route.distance ?dist], ?dist > 4000, [?r route.src ?s],
                                [?r route.dst ?d], [?s airport.iata ?src], [?d airport.iata ?dst],
                                ?src < ?dst;
        :sort -?dist;
        :limit 20;
    "#,
    )?;
    dbg!(longest_routes_time.elapsed());
    assert_eq!(*res.get("rows").unwrap(), serde_json::Value::from_str(r#"
    [["EWR",9523,"SIN"],["AKL",9025,"DOH"],["LHR",9009,"PER"],["PEK",8884,"PTY"],["AKL",8818,"DXB"],
    ["LAX",8756,"SIN"],["CAN",8754,"MEX"],["IAH",8591,"SYD"],["DFW",8574,"SYD"],["JFK",8504,"MNL"],
    ["ATL",8434,"JNB"],["SFO",8433,"SIN"],["AUH",8372,"LAX"],["DXB",8321,"LAX"],["JED",8314,"LAX"],
    ["DOH",8287,"LAX"],["LAX",8246,"RUH"],["SCL",8208,"TLV"],["MEL",8197,"YVR"],["AKL",8186,"ORD"]]"#).unwrap());

    let longest_routes_from_each_airports = Instant::now();
    let res = db.run_script(r#"
        ap[?a, max(?dist)] := [?r route.src ?a], [?r route.distance ?dist];
        ?[?src, ?dist, ?dst] := ap[?a, ?dist], [?r route.src ?a], [?r route.distance ?dist], [?r route.dst ?d],
                                [?a airport.iata ?src], [?d airport.iata ?dst];
        :limit 10;
    "#)?;
    dbg!(longest_routes_from_each_airports.elapsed());
    assert_eq!(*res.get("rows").unwrap(), serde_json::Value::from_str(r#"
    [["ANC",3368,"KEF"],["ATL",8434,"JNB"],["AUS",5294,"FRA"],["BNA",4168,"LHR"],["BOS",7952,"HKG"],
    ["BWI",3622,"LHR"],["DCA",2434,"SFO"],["DFW",8574,"SYD"],["FLL",7808,"DXB"],["IAD",8135,"HKG"]]"#).unwrap());

    let total_distance_from_three_cities_time = Instant::now();
    let res = db.run_script(
        r#"
        three[?a] := ?city <- ..['London', 'Munich', 'Paris'], [?a airport.city ?city];
        ?[sum(?dist)] := three[?a], [?r route.src ?a], [?r route.distance ?dist];
    "#,
    )?;
    dbg!(total_distance_from_three_cities_time.elapsed());
    assert_eq!(*res.get("rows").unwrap(), json!([[2733379.0]]));

    let total_distance_within_three_cities_time = Instant::now();
    let res = db.run_script(
        r#"
        three[?a] := ?city <- ..['London', 'Munich', 'Paris'], [?a airport.city ?city];
        ?[sum(?dist)] := three[?a], [?r route.src ?a], [?r route.dst ?a2], three[?a2],
                         [?r route.distance ?dist];
    "#,
    )?;
    dbg!(total_distance_within_three_cities_time.elapsed());
    assert_eq!(*res.get("rows").unwrap(), json!([[10282.0]]));

    let specific_distance_time = Instant::now();
    let res = db.run_script(
        r#"
        ?[?dist] := [?a airport.iata 'AUS'], [?a2 airport.iata 'MEX'], [?r route.src ?a],
                    [?r route.dst ?a2], [?r route.distance ?dist];
    "#,
    )?;
    dbg!(specific_distance_time.elapsed());
    assert_eq!(*res.get("rows").unwrap(), json!([[748]]));

    let n_routes_between_time = Instant::now();
    let res = db.run_script(
        r#"
        us_a[?a] := [?us country.code 'US'], [?us geo.contains ?a];
        ?[count(?r)] := [?r route.distance ?dist], ?dist >= 100, ?dist <= 200,
                        [?r route.src ?s], us_a[?s],
                        [?r route.dst ?d], us_a[?d];
    "#,
    )?;
    dbg!(n_routes_between_time.elapsed());
    assert_eq!(*res.get("rows").unwrap(), json!([[597]]));

    let one_stop_distance_time = Instant::now();
    let res = db.run_script(
        r#"
        ?[?code, ?dist] := [?s airport.iata 'AUS'], [?r1 route.src ?s], [?r1 route.dst ?a],
           [?r2 route.src ?a], [?r2 route.dst ?d], [?d airport.iata 'LHR'],
           [?r1 route.distance ?dis1], [?r2 route.distance ?dis2], ?dist <- ?dis1 + ?dis2,
           [?a airport.iata ?code];
        :order ?dist;
        :limit 10;
    "#,
    )?;
    dbg!(one_stop_distance_time.elapsed());
    assert_eq!(
        *res.get("rows").unwrap(),
        serde_json::Value::from_str(
            r#"
    [["DTW",4893],["YYZ",4901],["ORD",4912],["PIT",4916],["BNA",4923],["DFW",4926],["BOS",4944],
    ["EWR",4953],["IAD",4959],["JFK",4960]]"#
        )
        .unwrap()
    );

    let airport_most_routes_time = Instant::now();
    let res = db.run_script(
        r#"
        ac[?a, count(?r)] := [?r route.src ?a];
        ?[?code, ?n] := ac[?a, ?n], [?a airport.iata ?code];
        :order -?n;
        :limit 10;
    "#,
    )?;
    dbg!(airport_most_routes_time.elapsed());
    assert_eq!(
        *res.get("rows").unwrap(),
        serde_json::Value::from_str(
            r#"
    [["FRA",307],["IST",307],["CDG",293],["AMS",282],["MUC",270],
    ["ORD",264],["DFW",251],["PEK",248],["DXB",247],["ATL",242]]
    "#
        )
        .unwrap()
    );

    let north_of_77_time = Instant::now();
    let res = db.run_script(r#"
        ?[?city, ?latitude] := [?a airport.lat ?lat], ?lat > 77, [?a airport.city ?city], ?latitude <- round(?lat);
    "#)?;
    dbg!(north_of_77_time.elapsed());
    assert_eq!(
        *res.get("rows").unwrap(),
        serde_json::Value::from_str(r#"[["Longyearbyen",78.0],["Qaanaaq",77.0]]"#).unwrap()
    );

    let greenwich_meridian_time = Instant::now();
    let res = db.run_script(
        r#"
        ?[?code] := [?a airport.lon ?lon], ?lon > -0.1, ?lon < 0.1, [?a airport.iata ?code];
    "#,
    )?;
    dbg!(greenwich_meridian_time.elapsed());
    assert_eq!(
        *res.get("rows").unwrap(),
        json!([["CDT"], ["LCY"], ["LDE"], ["LEH"]])
    );

    let box_around_heathrow_time = Instant::now();
    let res = db.run_script(
        r#"
        h_box[?lhr_lon, ?lhr_lat] := [?lhr airport.iata 'LHR'],
                                     [?lhr airport.lon ?lhr_lon],
                                     [?lhr airport.lat ?lhr_lat];
        ?[?code] := h_box[?lhr_lon, ?lhr_lat], [?a airport.lon ?lon], [?a airport.lat ?lat],
                    abs(?lhr_lon - ?lon) < 1, abs(?lhr_lat - ?lat) < 1, [?a airport.iata ?code];
    "#,
    )?;
    dbg!(box_around_heathrow_time.elapsed());
    assert_eq!(
        *res.get("rows").unwrap(),
        json!([["LCY"], ["LGW"], ["LHR"], ["LTN"], ["SOU"], ["STN"]])
    );

    let dfw_by_region_time = Instant::now();
    let res = db.run_script(
        r#"
        ?[?region, collect(?code)] := [?dfw airport.iata 'DFW'],
                                      [?us country.code 'US'],
                                      [?r route.src ?dfw],
                                      [?r route.dst ?a], [?a airport.country ?us],
                                      ?region <- ..['US-CA', 'US-TX', 'US-FL', 'US-CO', 'US-IL'],
                                      [?a airport.region ?region],
                                      [?a airport.iata ?code];
    "#,
    )?;
    dbg!(dfw_by_region_time.elapsed());
    assert_eq!(*res.get("rows").unwrap(), serde_json::Value::from_str(r#"
    [["US-CA",["BFL","BUR","FAT","LAX","MRY","OAK","ONT","PSP","SAN","SBA","SFO","SJC","SMF","SNA"]],
    ["US-CO",["ASE","COS","DEN","DRO","EGE","GJT","GUC","HDN","MTJ"]],
    ["US-FL",["ECP","EYW","FLL","GNV","JAX","MCO","MIA","PBI","PNS","RSW","SRQ","TLH","TPA","VPS"]],
    ["US-IL",["BMI","CMI","MLI","ORD","PIA","SPI"]],
    ["US-TX",["ABI","ACT","AMA","AUS","BPT","BRO","CLL","CRP","DRT","ELP","GGG","GRK","HOU","HRL",
              "IAH","LBB","LRD","MAF","MFE","SAT","SJT","SPS","TYR"]]]
    "#).unwrap());

    let great_circle_distance = Instant::now();
    let res = db.run_script(
        r#"
        ?[?deg_diff] := [?a airport.iata 'SFO'], [?a airport.lat ?a_lat], [?a airport.lon ?a_lon],
                        [?b airport.iata 'NRT'], [?b airport.lat ?b_lat], [?b airport.lon ?b_lon],
                        ?deg_diff <- round(haversine_deg_input(?a_lat, ?a_lon, ?b_lat, ?b_lon));
    "#,
    )?;
    dbg!(great_circle_distance.elapsed());
    assert_eq!(*res.get("rows").unwrap(), json!([[1.0]]));

    let aus_to_edi_time = Instant::now();
    let res = db.run_script(
        r#"
        us_uk_airports[?a] := [?c country.code 'UK'], [?a airport.country ?c];
        us_uk_airports[?a] := [?c country.code 'US'], [?a airport.country ?c];
        routes[?a2, shortest(?path)] := [?a airport.iata 'AUS'], [?r route.src ?a],
                                        [?r route.dst ?a2], us_uk_airports[?a2],
                                        [?a2 airport.iata ?dst],
                                        ?path <- ['AUS', ?dst];
        routes[?a2, shortest(?path)] := routes[?a, ?prev], [?r route.src ?a],
                                        [?r route.dst ?a2], us_uk_airports[?a2],
                                        [?a2 airport.iata ?dst],
                                        ?path <- append(?prev, ?dst);
        ?[?path] := [?edi airport.iata 'EDI'], routes[?edi, ?path];
    "#,
    )?;
    dbg!(aus_to_edi_time.elapsed());
    assert_eq!(*res.get("rows").unwrap(), json!([[["AUS", "BOS", "EDI"]]]));

    let reachable_from_lhr = Instant::now();
    let res = db.run_script(
        r#"
        routes[?a2, shortest(?path)] := [?a airport.iata 'LHR'], [?r route.src ?a],
                                        [?r route.dst ?a2],
                                        [?a2 airport.iata ?dst],
                                        ?path <- ['LHR', ?dst];
        routes[?a2, shortest(?path)] := routes[?a, ?prev], [?r route.src ?a],
                                        [?r route.dst ?a2],
                                        [?a2 airport.iata ?dst],
                                        ?path <- append(?prev, ?dst);
        ?[?len, ?path] := routes[?_, ?path], ?len <- length(?path);

        :order -?len;
        :limit 10;
    "#,
    )?;
    dbg!(reachable_from_lhr.elapsed());
    assert_eq!(
        *res.get("rows").unwrap(),
        serde_json::Value::from_str(
            r#"
    [[8,["LHR","YYZ","YTS","YMO","YFA","ZKE","YAT","YPO"]],
    [7,["LHR","DFW","ANC","AKN","PIP","UGB","PTH"]],[7,["LHR","DFW","ANC","ANI","CHU","CKD","RDV"]],
    [7,["LHR","DFW","ANC","ANI","CHU","CKD","SLQ"]],[7,["LHR","DFW","ANC","BET","OOK","TNK","WWT"]],
    [7,["LHR","DFW","SYD","AYQ","MEB","WMB","PTJ"]],[7,["LHR","DFW","SYD","WTB","SGO","CMA","XTG"]],
    [7,["LHR","KEF","GOH","JAV","JUV","NAQ","THU"]],[7,["LHR","LAX","BNE","ISA","BQL","BEU","BVI"]],
    [7,["LHR","YUL","YGL","YPX","AKV","YIK","YZG"]]]
    "#
        )
        .unwrap()
    );

    let furthest_from_lhr = Instant::now();
    let res = db.run_script(
        r#"
        routes[?a2, min_cost(?cost_pair)] := [?a airport.iata 'LHR'], [?r route.src ?a],
                                             [?r route.dst ?a2],
                                             [?r route.distance ?dist],
                                             [?a2 airport.iata ?dst],
                                             ?path <- ['LHR', ?dst],
                                             ?cost_pair <- [?path, ?dist];
        routes[?a2, min_cost(?cost_pair)] := routes[?a, ?prev], [?r route.src ?a],
                                             [?r route.dst ?a2],
                                             [?r route.distance ?dist],
                                             [?a2 airport.iata ?dst],
                                             ?path <- append(first(?prev), ?dst),
                                             ?cost_pair <- [?path, last(?prev) + ?dist];
        ?[?cost, ?path] := routes[?dst, ?cost_pair], ?cost <- last(?cost_pair), ?path <- first(?cost_pair);

        :order -?cost;
        :limit 10;
    "#,
    )?;
    dbg!(furthest_from_lhr.elapsed());
    assert_eq!(
        *res.get("rows").unwrap(),
        serde_json::Value::from_str(
            r#"
    [[12922,["LHR","JNB","HLE","ASI","BZZ"]],[12114,["LHR","PVG","BNE","CHC","IVC"]],
     [12030,["LHR","PVG","BNE","CHC","DUD"]],[12015,["LHR","NRT","AKL","WLG","TIU"]],
     [11921,["LHR","PVG","BNE","CHC","HKK"]],[11910,["LHR","NRT","AKL","WLG","WSZ"]],
     [11826,["LHR","PVG","BNE","CHC"]],[11766,["LHR","PVG","BNE","ZQN"]],
     [11758,["LHR","NRT","AKL","BHE"]],[11751,["LHR","NRT","AKL","NSN"]]]
    "#
        )
        .unwrap()
    );

    let furthest_from_lhr_view = Instant::now();
    let res = db.run_script(
        r#"
        routes[?a2, min_cost(?cost_pair)] := [?a airport.iata 'LHR'], :flies_to[?a, ?a2, ?dist],
                                             [?a2 airport.iata ?dst],
                                             ?path <- ['LHR', ?dst],
                                             ?cost_pair <- [?path, ?dist];
        routes[?a2, min_cost(?cost_pair)] := routes[?a, ?prev], :flies_to[?a, ?a2, ?dist],
                                             [?a2 airport.iata ?dst],
                                             ?path <- append(first(?prev), ?dst),
                                             ?cost_pair <- [?path, last(?prev) + ?dist];
        ?[?cost, ?path] := routes[?dst, ?cost_pair], ?cost <- last(?cost_pair), ?path <- first(?cost_pair);

        :order -?cost;
        :limit 10;
    "#,
    )?;
    dbg!(furthest_from_lhr_view.elapsed());
    assert_eq!(
        *res.get("rows").unwrap(),
        serde_json::Value::from_str(
            r#"
    [[12922,["LHR","JNB","HLE","ASI","BZZ"]],[12114,["LHR","PVG","BNE","CHC","IVC"]],
     [12030,["LHR","PVG","BNE","CHC","DUD"]],[12015,["LHR","NRT","AKL","WLG","TIU"]],
     [11921,["LHR","PVG","BNE","CHC","HKK"]],[11910,["LHR","NRT","AKL","WLG","WSZ"]],
     [11826,["LHR","PVG","BNE","CHC"]],[11766,["LHR","PVG","BNE","ZQN"]],
     [11758,["LHR","NRT","AKL","BHE"]],[11751,["LHR","NRT","AKL","NSN"]]]
    "#
        )
        .unwrap()
    );

    Ok(())
}
