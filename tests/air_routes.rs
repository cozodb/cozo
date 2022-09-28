use std::str::FromStr;
use std::time::Instant;

use lazy_static::lazy_static;
use serde_json::json;

use cozo::Db;
use cozorocks::DbBuilder;

lazy_static! {
    static ref TEST_DB: Db = {
        let path = "_test_air_routes";
        _ = std::fs::remove_dir_all(path);
        let builder = DbBuilder::default().path(path).create_if_missing(true);
        let db = Db::build(builder).unwrap();

        let init = Instant::now();
        db.run_script(r##"
{
    res[idx, label, typ, code, icao, desc, region, runways, longest, elev, country, city, lat, lon] <~
        CsvReader(types: ['Int', 'Any', 'Any', 'Any', 'Any', 'Any', 'Any', 'Int?', 'Float?', 'Float?', 'Any', 'Any', 'Float?', 'Float?'],
                  url: 'file://./tests/air-routes-latest-nodes.csv',
                  has_headers: true)

    ?[code, icao, desc, region, runways, longest, elev, country, city, lat, lon] :=
        res[idx, label, typ, code, icao, desc, region, runways, longest, elev, country, city, lat, lon],
        label == 'airport'

    :replace airport {
        code: String
        =>
        icao: String,
        desc: String,
        region: String,
        runways: Int,
        longest: Float,
        elev: Float,
        country: String,
        city: String,
        lat: Float,
        lon: Float
    }
}
{
    res[idx, label, typ, code, icao, desc] <~
        CsvReader(types: ['Int', 'Any', 'Any', 'Any', 'Any', 'Any'],
                  url: 'file://./tests/air-routes-latest-nodes.csv',
                  has_headers: true)
    ?[code, desc] :=
        res[idx, label, typ, code, icao, desc],
        label == 'country'

    :replace country {
        code: String
        =>
        desc: String
    }
}
{
    res[idx, label, typ, code, icao, desc] <~
        CsvReader(types: ['Int', 'Any', 'Any', 'Any', 'Any', 'Any'],
                  url: 'file://./tests/air-routes-latest-nodes.csv',
                  has_headers: true)
    ?[idx, code, desc] :=
        res[idx, label, typ, code, icao, desc],
        label == 'continent'

    :replace continent {
        code: String
        =>
        desc: String
    }
}
{
    res[idx, label, typ, code] <~
        CsvReader(types: ['Int', 'Any', 'Any', 'Any'],
                  url: 'file://./tests/air-routes-latest-nodes.csv',
                  has_headers: true)
    ?[idx, code] :=
        res[idx, label, typ, code],

    :replace idx2code { idx => code }
}
{
    res[] <~
        CsvReader(types: ['Int', 'Int', 'Int', 'String', 'Float?'],
                  url: 'file://./tests/air-routes-latest-edges.csv',
                  has_headers: true)
    ?[fr, to, dist] :=
        res[idx, fr_i, to_i, typ, dist],
        typ == 'route',
        :idx2code[fr_i, fr],
        :idx2code[to_i, to]

    :replace route { fr: String, to: String => dist: Float }
}
{
    res[] <~
        CsvReader(types: ['Int', 'Int', 'Int', 'String'],
                  url: 'file://./tests/air-routes-latest-edges.csv',
                  has_headers: true)
    ?[entity, contained] :=
        res[idx, fr_i, to_i, typ],
        typ == 'contains',
        :idx2code[fr_i, entity],
        :idx2code[to_i, contained]


    :replace contain { entity: String, contained: String }
}
        "##, &Default::default()).unwrap();

        db.run_script("::relation remove idx2code", &Default::default())
            .unwrap();

        dbg!(init.elapsed());
        db
    };
}

fn check_relations() {
    assert_eq!(
        5,
        TEST_DB
            .list_relations()
            .unwrap()
            .get("rows")
            .unwrap()
            .as_array()
            .unwrap()
            .len()
    );
}

#[test]
fn dfs() {
    check_relations();
    let dfs = Instant::now();
    let res = TEST_DB
        .run_script(
            r#"
        starting[] <- [['PEK']]
        ?[] <~ DFS(:route[], :airport[code], starting[], condition: (code == 'LHR'))
    "#,
            &Default::default(),
        )
        .unwrap();
    let rows = res.get("rows").unwrap().as_array().unwrap();
    assert_eq!(rows.len(), 1);
    let row = rows.get(0).unwrap();
    assert_eq!(row.get(0).unwrap().as_str().unwrap(), "PEK");
    assert_eq!(row.get(1).unwrap().as_str().unwrap(), "LHR");
    let path = row.get(2).unwrap().as_array().unwrap();
    assert_eq!(path.first().unwrap().as_str().unwrap(), "PEK");
    assert_eq!(path.last().unwrap().as_str().unwrap(), "LHR");
    dbg!(dfs.elapsed());
}

#[test]
fn bfs() {
    check_relations();
    let bfs = Instant::now();
    let res = TEST_DB
        .run_script(
            r#"
        starting[] <- [['PEK']]
        ?[] <~ BFS(:route[], :airport[code], starting[], condition: (code == 'LHR'))
    "#,
            &Default::default(),
        )
        .unwrap();
    let rows = res.get("rows").unwrap().as_array().unwrap();
    assert_eq!(rows.len(), 1);
    let row = rows.get(0).unwrap();
    assert_eq!(row.get(0).unwrap().as_str().unwrap(), "PEK");
    assert_eq!(row.get(1).unwrap().as_str().unwrap(), "LHR");
    let path = row.get(2).unwrap().as_array().unwrap();
    assert_eq!(path.first().unwrap().as_str().unwrap(), "PEK");
    assert_eq!(path.last().unwrap().as_str().unwrap(), "LHR");
    dbg!(bfs.elapsed());
}

#[test]
fn scc() {
    check_relations();
    let scc = Instant::now();
    let _ = TEST_DB
        .run_script(
            r#"
        res[] <~ StronglyConnectedComponents(:route[], :airport[code]);
        ?[grp, code] := res[code, grp], grp != 0;
    "#,
            &Default::default(),
        )
        .unwrap();
    dbg!(scc.elapsed());
}

#[test]
fn cc() {
    check_relations();
    let cc = Instant::now();
    let _ = TEST_DB
        .run_script(
            r#"
        res[] <~ ConnectedComponents(:route[], :airport[code]);
        ?[grp, code] := res[code, grp], grp != 0;
    "#,
            &Default::default(),
        )
        .unwrap();
    dbg!(cc.elapsed());
}

#[test]
fn astar() {
    check_relations();
    let astar = Instant::now();
    let _ = TEST_DB.run_script(r#"
        code_lat_lon[code, lat, lon] := :airport{code, lat, lon}
        starting[code, lat, lon] := code = 'HFE', :airport{code, lat, lon};
        goal[code, lat, lon] := code = 'LHR', :airport{code, lat, lon};
        ?[] <~ ShortestPathAStar(:route[], code_lat_lon[node, lat1, lon1], starting[], goal[goal, lat2, lon2], heuristic: haversine_deg_input(lat1, lon1, lat2, lon2) * 3963);
    "#, &Default::default()).unwrap();
    dbg!(astar.elapsed());
}

#[test]
fn deg_centrality() {
    check_relations();
    let deg_centrality = Instant::now();
    TEST_DB
        .run_script(
            r#"
        deg_centrality[] <~ DegreeCentrality(:route[a, b]);
        ?[total, out, in] := deg_centrality[node, total, out, in];
        :order -total;
        :limit 10;
    "#,
            &Default::default(),
        )
        .unwrap();
    dbg!(deg_centrality.elapsed());
}

#[test]
fn dijkstra() {
    check_relations();
    let dijkstra = Instant::now();

    TEST_DB
        .run_script(
            r#"
        starting[] <- [['JFK']];
        ending[] <- [['KUL']];
        res[] <~ ShortestPathDijkstra(:route[], starting[], ending[]);
        ?[path] := res[src, dst, cost, path];
    "#,
            &Default::default(),
        )
        .unwrap();

    dbg!(dijkstra.elapsed());
}

#[test]
fn yen() {
    check_relations();
    let yen = Instant::now();

    TEST_DB
        .run_script(
            r#"
        starting[] <- [['PEK']];
        ending[] <- [['SIN']];
        ?[] <~ KShortestPathYen(:route[], starting[], ending[], k: 5);
    "#,
            &Default::default(),
        )
        .unwrap();

    dbg!(yen.elapsed());
}

#[test]
fn starts_with() {
    check_relations();
    let starts_with = Instant::now();
    let res = TEST_DB
        .run_script(
            r#"
         ?[code] := :airport{code}, starts_with(code, 'US');
    "#,
            &Default::default(),
        )
        .unwrap();
    let rows = res.get("rows").unwrap();
    assert_eq!(
        *rows,
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

    dbg!(starts_with.elapsed());
}

#[test]
fn range_check() {
    check_relations();
    let range_check = Instant::now();

    let res = TEST_DB
        .run_script(
            r#"
        r[code, dist] := :airport{code}, :route{fr: code, dist};
        ?[dist] := r['PEK', dist], dist > 7000, dist <= 7722;
    "#,
            &Default::default(),
        )
        .unwrap();
    let rows = res.get("rows").unwrap();
    assert_eq!(*rows, json!([[7176.0], [7270.0], [7311.0], [7722.0]]));
    dbg!(range_check.elapsed());
}

#[test]
fn no_airports() {
    check_relations();
    let no_airports = Instant::now();

    let res = TEST_DB
        .run_script(
            r#"
        ?[desc] := :country{code, desc}, not :airport{country: code};
    "#,
            &Default::default(),
        )
        .unwrap();
    let rows = res.get("rows").unwrap();
    assert_eq!(
        *rows,
        json!([
            ["Andorra"],
            ["Liechtenstein"],
            ["Monaco"],
            ["Pitcairn"],
            ["San Marino"]
        ])
    );
    dbg!(no_airports.elapsed());
}

#[test]
fn no_routes_airport() {
    check_relations();
    let no_routes_airports = Instant::now();

    let res = TEST_DB
        .run_script(
            r#"
        ?[code] := :airport{code}, not :route{fr: code}, not :route{to: code}
    "#,
            &Default::default(),
        )
        .unwrap();
    let rows = res.get("rows").unwrap();
    assert_eq!(
        *rows,
        serde_json::Value::from_str(
            r#"[
            ["AFW"],["APA"],["APK"],["BID"],["BVS"],["BWU"],["CRC"],["CVT"],["EKA"],["GYZ"],
            ["HFN"],["HZK"],["ILG"],["INT"],["ISL"],["KGG"],["NBW"],["NFO"],["PSY"],["RIG"],
            ["SFD"],["SFH"],["SXF"],["TUA"],["TWB"],["TXL"],["VCV"],["YEI"]
        ]"#
        )
        .unwrap()
    );
    dbg!(no_routes_airports.elapsed());
}

#[test]
fn runway_distribution() {
    check_relations();
    let no_routes_airports = Instant::now();

    let res = TEST_DB
        .run_script(
            r#"
        ?[runways, count(code)] := :airport{code, runways}
    "#,
            &Default::default(),
        )
        .unwrap();
    let rows = res.get("rows").unwrap();
    assert_eq!(
        *rows,
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
    dbg!(no_routes_airports.elapsed());
}

#[test]
fn most_out_routes() {
    check_relations();
    let most_out_routes = Instant::now();

    let res = TEST_DB
        .run_script(
            r#"
        route_count[fr, count(fr)] := :route{fr};
        ?[code, n] := route_count[code, n], n > 180;
        :sort -n;
    "#,
            &Default::default(),
        )
        .unwrap();
    let rows = res.get("rows").unwrap();
    assert_eq!(
        *rows,
        serde_json::Value::from_str(
            r#"[
            ["FRA",310],["IST",309],["CDG",293],["AMS",283],["MUC",270],["ORD",265],["DFW",253],
            ["DXB",248],["PEK",248],["ATL",242],["DME",232],["LGW",232],["LHR",221],["DEN",217],
            ["MAN",216],["LAX",214],["PVG",213],["STN",211],["MAD",206],["VIE",206],["JFK",204],
            ["BCN",203],["EWR",203],["BER",202],["FCO",201],["DUS",199],["IAH",199],["MIA",196],
            ["YYZ",195],["BRU",194],["CPH",194],["DOH",187],["DUB",185],["CLT",184],["SVO",181]
            ]"#
        )
        .unwrap()
    );
    dbg!(most_out_routes.elapsed());
}

#[test]
fn most_out_routes_again() {
    check_relations();
    let most_out_routes_again = Instant::now();

    let res = TEST_DB
        .run_script(
            r#"
        route_count[count(fr), fr] := :route{fr};
        ?[code, n] := route_count[n, code], n > 180;
        :sort -n;
    "#,
            &Default::default(),
        )
        .unwrap();
    let rows = res.get("rows").unwrap();
    assert_eq!(
        *rows,
        serde_json::Value::from_str(
            r#"[
            ["FRA",310],["IST",309],["CDG",293],["AMS",283],["MUC",270],["ORD",265],["DFW",253],
            ["DXB",248],["PEK",248],["ATL",242],["DME",232],["LGW",232],["LHR",221],["DEN",217],
            ["MAN",216],["LAX",214],["PVG",213],["STN",211],["MAD",206],["VIE",206],["JFK",204],
            ["BCN",203],["EWR",203],["BER",202],["FCO",201],["DUS",199],["IAH",199],["MIA",196],
            ["YYZ",195],["BRU",194],["CPH",194],["DOH",187],["DUB",185],["CLT",184],["SVO",181]
            ]"#
        )
        .unwrap()
    );
    dbg!(most_out_routes_again.elapsed());
}

#[test]
fn most_routes() {
    check_relations();
    let most_routes = Instant::now();

    let res = TEST_DB
        .run_script(
            r#"
        route_count[a, count(a)] := :route{fr: a}
        route_count[a, count(a)] := :route{to: a}
        ?[code, n] := route_count[code, n], n > 400
        :sort -n;
    "#,
            &Default::default(),
        )
        .unwrap();
    let rows = res.get("rows").unwrap();
    assert_eq!(
        *rows,
        serde_json::Value::from_str(
            r#"[
            ["FRA",620],["IST",618],["CDG",587],["AMS",568],["MUC",541],["ORD",529],["DFW",506],
            ["PEK",497],["DXB",496],["ATL",484],["DME",465],["LGW",464],["LHR",442],["DEN",434],
            ["MAN",431],["LAX",428],["PVG",426],["STN",423],["MAD",412],["VIE",412],["JFK",407],
            ["BCN",406],["EWR",406],["BER",404],["FCO",402]]"#
        )
        .unwrap()
    );
    dbg!(most_routes.elapsed());
}

#[test]
fn airport_with_one_route() {
    check_relations();
    let airport_with_one_route = Instant::now();

    let res = TEST_DB
        .run_script(
            r#"
        route_count[fr, count(fr)] := :route{fr}
        ?[count(a)] := route_count[a, n], n == 1;
    "#,
            &Default::default(),
        )
        .unwrap();
    let rows = res.get("rows").unwrap();
    assert_eq!(*rows, json!([[777]]));
    dbg!(airport_with_one_route.elapsed());
}

#[test]
fn single_runway_with_most_routes() {
    check_relations();
    let single_runway_with_most_routes = Instant::now();

    let res = TEST_DB
        .run_script(
            r#"
        single_or_lgw[code] := code = 'LGW'
        single_or_lgw[code] := :airport{code, runways}, runways == 1
        out_counts[a, count(a)] := single_or_lgw[a], :route{fr: a}
        ?[code, city, out_n] := out_counts[code, out_n], :airport{code, city}

        :order -out_n;
        :limit 10;
    "#,
            &Default::default(),
        )
        .unwrap();
    let rows = res.get("rows").unwrap();
    assert_eq!(
        *rows,
        serde_json::Value::from_str(
            r#"[
        ["LGW","London",232],["STN","London",211],["CTU","Chengdu",139],["LIS","Lisbon",139],
        ["BHX","Birmingham",130],["LTN","London",130],["SZX","Shenzhen",129],
        ["CKG","Chongqing",122],["STR","Stuttgart",121],["CRL","Brussels",117]]"#
        )
        .unwrap()
    );
    dbg!(single_runway_with_most_routes.elapsed());
}

#[test]
fn most_routes_in_canada() {
    check_relations();
    let most_routes_in_canada = Instant::now();

    let res = TEST_DB
        .run_script(
            r#"
        ca_airports[code, count(code)] := :airport{code, country: 'CA'}, :route{fr: code}
        ?[code, city, n_routes] := ca_airports[code, n_routes], :airport{code, city}

        :order -n_routes;
        :limit 10;
    "#,
            &Default::default(),
        )
        .unwrap();
    let rows = res.get("rows").unwrap();
    assert_eq!(
        *rows,
        json!([
            ["YYZ", "Toronto", 195],
            ["YUL", "Montreal", 123],
            ["YVR", "Vancouver", 106],
            ["YYC", "Calgary", 75],
            ["YEG", "Edmonton", 48],
            ["YHZ", "Halifax", 45],
            ["YWG", "Winnipeg", 38],
            ["YOW", "Ottawa", 36],
            ["YZF", "Yellowknife", 21],
            ["YQB", "Quebec City", 20]
        ])
    );
    dbg!(most_routes_in_canada.elapsed());
}

#[test]
fn uk_count() {
    check_relations();
    let uk_count = Instant::now();

    let res = TEST_DB
        .run_script(
            r#"
       ?[region, count(region)] := :airport{country: 'UK', region}
    "#,
            &Default::default(),
        )
        .unwrap();
    let rows = res.get("rows").unwrap();
    assert_eq!(
        *rows,
        json!([["GB-ENG", 27], ["GB-NIR", 3], ["GB-SCT", 25], ["GB-WLS", 3]])
    );
    dbg!(uk_count.elapsed());
}

#[test]
fn airports_by_country() {
    check_relations();
    let airports_by_country = Instant::now();

    let res = TEST_DB
        .run_script(
            r#"
        airports_by_country[country, count(code)] := :airport{code, country}
        ?[country, count] := airports_by_country[country, count];
        ?[country, count] := :country{code: country}, not airports_by_country[country, _], count = 0

        :order count
    "#,
            &Default::default(),
        )
        .unwrap();
    let rows = res.get("rows").unwrap();
    assert_eq!(
        *rows,
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
    ["TC",4],["TJ",4],["UG",4],["AF",5],["AZ",5],["BE",5],["CM",5],["CZ",5],["NA",5],["NL",5],
    ["PA",5],["SD",5],["TD",5],["TO",5],["AT",6],["CH",6],["CK",6],["GH",6],["HN",6],["IL",6],
    ["IQ",6],["LK",6],["SO",6],["BD",7],["CV",7],["DO",7],["IE",7],["IS",7],["MW",7],["PR",7],
    ["DK",8],["HR",8],["LA",8],["MV",8],["TN",8],["TW",9],["YE",9],["ZM",9],["AE",10],["FJ",10],
    ["MN",10],["CD",11],["EG",11],["LY",11],["MZ",11],["NP",11],["TZ",11],["UZ",11],["CU",12],
    ["BZ",13],["CR",13],["MG",13],["PL",13],["AO",14],["GL",14],["KE",14],["RO",14],["BO",15],
    ["EC",15],["KR",15],["UA",15],["ET",16],["MA",16],["CL",17],["MM",17],["SB",17],["BS",18],
    ["NG",19],["PT",19],["FI",20],["ZA",20],["KZ",21],["PK",21],["PE",22],["VN",22],["NZ",25],
    ["PG",26],["SA",26],["VU",26],["VE",27],["DZ",30],["TH",33],["DE",34],["MY",35],["AR",38],
    ["IT",38],["GR",39],["PF",39],["SE",39],["PH",40],["ES",43],["IR",45],["NO",49],["CO",51],
    ["TR",52],["UK",58],["FR",59],["MX",60],["JP",65],["ID",70],["IN",77],["BR",117],["RU",129],
    ["AU",132],["CA",205],["CN",217],["US",586]]"#
        )
        .unwrap()
    );
    dbg!(airports_by_country.elapsed());
}

#[test]
fn n_airports_by_continent() {
    check_relations();
    let n_airports_by_continent = Instant::now();

    let res = TEST_DB
        .run_script(
            r#"
        airports_by_continent[cont, count(code)] := :airport{code}, :contain[cont, code]
        ?[cont, max(count)] := :continent{code: cont}, airports_by_continent[cont, count]
        ?[cont, max(count)] := :continent{code: cont}, count = 0
    "#,
            &Default::default(),
        )
        .unwrap();
    let rows = res.get("rows").unwrap();
    assert_eq!(
        *rows,
        serde_json::Value::from_str(
            r#"[["AF",326],["AN",0],["AS",972],["EU",605],["NA",994],["OC",305],["SA",339]]"#
        )
        .unwrap()
    );
    dbg!(n_airports_by_continent.elapsed());
}

#[test]
fn routes_per_airport() {
    check_relations();
    let routes_per_airport = Instant::now();

    let res = TEST_DB
        .run_script(
            r#"
        given[] <- [['A' ++ 'U' ++ 'S'],['AMS'],['JFK'],['DUB'],['MEX']]
        ?[code, count(code)] := given[code], :route{fr: code}
    "#,
            &Default::default(),
        )
        .unwrap();
    let rows = res.get("rows").unwrap();
    assert_eq!(
        *rows,
        serde_json::Value::from_str(
            r#"[["AMS",283],["AUS",98],["DUB",185],["JFK",204],["MEX",116]]"#
        )
        .unwrap()
    );
    dbg!(routes_per_airport.elapsed());
}

#[test]
fn airports_by_route_number() {
    check_relations();
    let airports_by_route_number = Instant::now();

    let res = TEST_DB
        .run_script(
            r#"
        route_count[fr, count(fr)] := :route{fr}
        ?[n, collect(code)] := route_count[code, n], n = 106;
    "#,
            &Default::default(),
        )
        .unwrap();
    let rows = res.get("rows").unwrap();
    assert_eq!(*rows, json!([[106, ["TFS", "YVR"]]]));
    dbg!(airports_by_route_number.elapsed());
}

#[test]
fn out_from_aus() {
    check_relations();
    let out_from_aus = Instant::now();

    let res = TEST_DB
        .run_script(
            r#"
        out_by_runways[runways, count(code)] := :route{fr: 'AUS', to: code}, :airport{code, runways}
        two_hops[count(a)] := :route{fr: 'AUS', to: a}, :route{fr: a}
        ?[max(total), collect(coll)] := two_hops[total], out_by_runways[n, ct], coll = [n, ct];
    "#,
            &Default::default(),
        )
        .unwrap();
    let rows = res.get("rows").unwrap();
    assert_eq!(
        *rows,
        serde_json::Value::from_str(r#"[[8354,[[1,9],[2,24],[3,30],[4,24],[5,5],[6,4],[7,2]]]]"#)
            .unwrap()
    );
    dbg!(out_from_aus.elapsed());
}

#[test]
fn const_return() {
    check_relations();
    let const_return = Instant::now();

    let res = TEST_DB
        .run_script(
            r#"
        ?[name, count(code)] := :airport{code, region: 'US-OK'}, name = 'OK';
    "#,
            &Default::default(),
        )
        .unwrap();
    let rows = res.get("rows").unwrap();
    assert_eq!(*rows, json!([["OK", 4]]));
    dbg!(const_return.elapsed());
}

#[test]
fn multi_res() {
    check_relations();
    let multi_res = Instant::now();

    let res = TEST_DB
        .run_script(
            r#"
        total[count(code)] := :airport{code}
        high[count(code)] := :airport{code, runways}, runways >= 6
        low[count(code)] := :airport{code, runways}, runways <= 2
        four[count(code)] := :airport{code, runways}, runways == 4
        france[count(code)] := :airport{code, country: 'FR'}

        ?[total, high, low, four, france] := total[total], high[high], low[low],
                                                  four[four], france[france];
    "#,
            &Default::default(),
        )
        .unwrap();
    let rows = res.get("rows").unwrap();
    assert_eq!(
        *rows,
        serde_json::Value::from_str(r#"[[3504,6,3204,53,59]]"#).unwrap()
    );
    dbg!(multi_res.elapsed());
}

#[test]
fn multi_unification() {
    check_relations();
    let multi_unification = Instant::now();

    let res = TEST_DB
        .run_script(
            r#"
        target_airports[collect(code, 5)] := :airport{code}
        ?[a, count(a)] := target_airports[targets], a in targets, :route{fr: a}
    "#,
            &Default::default(),
        )
        .unwrap();
    let rows = res.get("rows").unwrap();
    assert_eq!(
        *rows,
        serde_json::Value::from_str(r#"[["AAA",4],["AAE",8],["AAL",17],["AAN",5],["AAQ",11]]"#)
            .unwrap()
    );
    dbg!(multi_unification.elapsed());
}

#[test]
fn num_routes_from_eu_to_us() {
    check_relations();
    let num_routes_from_eu_to_us = Instant::now();

    let res = TEST_DB
        .run_script(
            r#"
        routes[unique(r)] := :contain['EU', fr],
                             :route{fr, to},
                             :airport{code: to, country: 'US'},
                             r = [fr, to]
        ?[n] := routes[rs], n = length(rs);
    "#,
            &Default::default(),
        )
        .unwrap();
    let rows = res.get("rows").unwrap();
    assert_eq!(*rows, json!([[435]]));
    dbg!(num_routes_from_eu_to_us.elapsed());
}


//     let num_airports_in_us_with_routes_from_eu_time = Instant::now();
//     let res = db.run_script(
//         r#"
//         ?[count_unique(a2)] := [eu continent.code 'EU'],
//                                 [us country.code 'US'],
//                                 [eu geo.contains a],
//                                 [r route.src a],
//                                 [r route.dst a2],
//                                 [a2 airport.country us];
//     "#,
//         &params,
//     )?;
//     dbg!(num_airports_in_us_with_routes_from_eu_time.elapsed());
//     assert_eq!(*res.get("rows").unwrap(), json!([[45]]));
//
//     let num_routes_in_us_airports_from_eu_time = Instant::now();
//     let res = db.run_script(
//         r#"
//         ?[code, count(r)] := [eu continent.code 'EU'],
//                                [us country.code 'US'],
//                                [eu geo.contains a],
//                                [r route.src a],
//                                [r route.dst a2],
//                                [a2 airport.country us],
//                                [a2 airport.iata code];
//         :order count(r);
//     "#,
//         &params,
//     )?;
//     dbg!(num_routes_in_us_airports_from_eu_time.elapsed());
//     assert_eq!(
//         *res.get("rows").unwrap(),
//         serde_json::Value::from_str(
//             r#"
//     [["ANC",1],["BNA",1],["CHS",1],["CLE",1],["IND",1],["MCI",1],["STL",1],["BDL",2],["BWI",2],
//      ["CVG",2],["MSY",2],["PHX",2],["RDU",2],["SJC",2],["AUS",3],["PDX",3],["RSW",3],["SAN",3],
//      ["SLC",3],["PIT",4],["SFB",5],["SWF",5],["TPA",5],["DTW",6],["MSP",6],["OAK",6],["DEN",7],
//      ["FLL",7],["PVD",7],["CLT",8],["IAH",8],["DFW",10],["SEA",10],["LAS",11],["MCO",13],["ATL",15],
//      ["SFO",20],["IAD",21],["PHL",22],["BOS",25],["LAX",25],["ORD",27],["MIA",28],["EWR",38],
//      ["JFK",42]]"#
//         )
//             .unwrap()
//     );
//
//     let routes_from_eu_to_us_starting_with_l_time = Instant::now();
//     let res = db.run_script(
//         r#"
//         ?[eu_code, us_code] := [eu continent.code 'EU'],
//                                [us country.code 'US'],
//                                [eu geo.contains a],
//                                [a airport.iata eu_code],
//                                starts_with(eu_code, 'L'),
//                                [r route.src a],
//                                [r route.dst a2],
//                                [a2 airport.country us],
//                                [a2 airport.iata us_code];
//     "#,
//         &params,
//     )?;
//     dbg!(routes_from_eu_to_us_starting_with_l_time.elapsed());
//     assert_eq!(
//         *res.get("rows").unwrap(),
//         serde_json::Value::from_str(
//             r#"[
//     ["LGW","AUS"],["LGW","BOS"],["LGW","DEN"],["LGW","FLL"],["LGW","JFK"],["LGW","LAS"],
//     ["LGW","LAX"],["LGW","MCO"],["LGW","MIA"],["LGW","OAK"],["LGW","ORD"],["LGW","SEA"],
//     ["LGW","SFO"],["LGW","TPA"],["LHR","ATL"],["LHR","AUS"],["LHR","BNA"],["LHR","BOS"],
//     ["LHR","BWI"],["LHR","CHS"],["LHR","CLT"],["LHR","DEN"],["LHR","DFW"],["LHR","DTW"],
//     ["LHR","EWR"],["LHR","IAD"],["LHR","IAH"],["LHR","JFK"],["LHR","LAS"],["LHR","LAX"],
//     ["LHR","MIA"],["LHR","MSP"],["LHR","MSY"],["LHR","ORD"],["LHR","PDX"],["LHR","PHL"],
//     ["LHR","PHX"],["LHR","PIT"],["LHR","RDU"],["LHR","SAN"],["LHR","SEA"],["LHR","SFO"],
//     ["LHR","SJC"],["LHR","SLC"],["LIS","ATL"],["LIS","BOS"],["LIS","EWR"],["LIS","IAD"],
//     ["LIS","JFK"],["LIS","MIA"],["LIS","ORD"],["LIS","PHL"],["LIS","SFO"]]"#
//         )
//             .unwrap()
//     );
//
//     let len_of_names_count_time = Instant::now();
//     let res = db.run_script(
//         r#"
//         ?[sum(n)] := [a airport.iata 'AUS'],
//                       [r route.src a],
//                       [r route.dst a2],
//                       [a2 airport.city city_name],
//                       n = length(city_name);
//     "#,
//         &params,
//     )?;
//     dbg!(len_of_names_count_time.elapsed());
//     assert_eq!(*res.get("rows").unwrap(), json!([[866.0]]));
//
//     let group_count_by_out_time = Instant::now();
//     let res = db.run_script(
//         r#"
//         route_count[count(r), a] := [r route.src a];
//         rc[max(n), a] := route_count[n, a];
//         rc[max(n), a] := [a airport.iata _], n = 0;
//         ?[n, count(a)] := rc[n, a];
//         :order n;
//         :limit 10;
//     "#,
//         &params,
//     )?;
//     dbg!(group_count_by_out_time.elapsed());
//     assert_eq!(
//         *res.get("rows").unwrap(),
//         serde_json::Value::from_str(
//             r#"[[0,29],[1,777],[2,649],[3,359],[4,232],[5,150],[6,139],[7,100],[8,74],[9,63]]"#
//         )
//             .unwrap()
//     );
//
//     let mean_group_count_time = Instant::now();
//     let res = db.run_script(
//         r#"
//         route_count[count(r), a] := [r route.src a];
//         rc[max(n), a] := route_count[n, a] or ([a airport.iata _], n = 0);
//         ?[mean(n)] := rc[n, _];
//     "#,
//         &params,
//     )?;
//     dbg!(mean_group_count_time.elapsed());
//     let v = res
//         .get("rows")
//         .unwrap()
//         .as_array()
//         .unwrap()
//         .get(0)
//         .unwrap()
//         .as_array()
//         .unwrap()
//         .get(0)
//         .unwrap()
//         .as_f64()
//         .unwrap();
//     let expected = 14.425513698630137;
//     assert!(abs(v - expected) < 1e-8);
//
//     let n_routes_from_london_uk_time = Instant::now();
//     let res = db.run_script(r#"
//         ?[code, count(r)] := [a airport.city 'London'], [a airport.region 'GB-ENG'], [r route.src a], [a airport.iata code];
//     "#, &params,
//     )?;
//     dbg!(n_routes_from_london_uk_time.elapsed());
//     assert_eq!(
//         *res.get("rows").unwrap(),
//         serde_json::Value::from_str(
//             r#"[["LCY",51],["LGW",232],["LHR",221],["LTN",130],["STN",211]]"#
//         )
//             .unwrap()
//     );
//
//     let reachable_from_london_uk_in_two_hops_time = Instant::now();
//     let res = db.run_script(r#"
//         lon_uk_airports[a] := [a airport.city 'London'], [a airport.region 'GB-ENG'];
//         one_hop[a2] := lon_uk_airports[a], [r route.src a], [r route.dst a2], not lon_uk_airports[a2];
//         ?[count_unique(a3)] := one_hop[a2], [r2 route.src a2], [r2 route.dst a3], not lon_uk_airports[a3];
//     "#, &params,
//     )?;
//     dbg!(reachable_from_london_uk_in_two_hops_time.elapsed());
//     assert_eq!(*res.get("rows").unwrap(), json!([[2353]]));
//
//     let routes_within_england_time = Instant::now();
//     let res = db.run_script(
//         r#"
//         eng_aps[a] := [a airport.region 'GB-ENG'];
//         ?[src, dst] := eng_aps[a1], [r route.src a1], [r route.dst a2], eng_aps[a2],
//                          [a1 airport.iata src], [a2 airport.iata dst];
//     "#,
//         &params,
//     )?;
//     dbg!(routes_within_england_time.elapsed());
//     assert_eq!(
//         *res.get("rows").unwrap(),
//         serde_json::Value::from_str(
//             r#"[
//     ["BHX","NCL"],["BRS","NCL"],["EMA","SOU"],["EXT","ISC"],["EXT","MAN"],["EXT","NQY"],
//     ["HUY","NWI"],["ISC","EXT"],["ISC","LEQ"],["ISC","NQY"],["LBA","LHR"],["LBA","NQY"],
//     ["LBA","SOU"],["LCY","MAN"],["LCY","NCL"],["LEQ","ISC"],["LGW","NCL"],["LGW","NQY"],
//     ["LHR","LBA"],["LHR","MAN"],["LHR","NCL"],["LHR","NQY"],["LPL","NQY"],["MAN","EXT"],
//     ["MAN","LCY"],["MAN","LHR"],["MAN","NQY"],["MAN","NWI"],["MAN","SEN"],["MAN","SOU"],
//     ["MME","NWI"],["NCL","BHX"],["NCL","BRS"],["NCL","LCY"],["NCL","LGW"],["NCL","LHR"],
//     ["NCL","SOU"],["NQY","EXT"],["NQY","ISC"],["NQY","LBA"],["NQY","LGW"],["NQY","LHR"],
//     ["NQY","LPL"],["NQY","MAN"],["NQY","SEN"],["NWI","HUY"],["NWI","MAN"],["NWI","MME"],
//     ["SEN","MAN"],["SEN","NQY"],["SOU","EMA"],["SOU","LBA"],["SOU","MAN"],["SOU","NCL"]]"#
//         )
//             .unwrap()
//     );
//
//     let routes_within_england_time_no_dup = Instant::now();
//     let res = db.run_script(
//         r#"
//         eng_aps[a] := [a airport.region 'GB-ENG'];
//         ?[pair] := eng_aps[a1], [r route.src a1], [r route.dst a2], eng_aps[a2],
//                          [a1 airport.iata src], [a2 airport.iata dst],
//                          pair = sorted([src, dst]);
//     "#,
//         &params,
//     )?;
//     dbg!(routes_within_england_time_no_dup.elapsed());
//     assert_eq!(
//         *res.get("rows").unwrap(),
//         serde_json::Value::from_str(
//             r#"[
//     [["BHX","NCL"]],[["BRS","NCL"]],[["EMA","SOU"]],[["EXT","ISC"]],[["EXT","MAN"]],[["EXT","NQY"]],
//     [["HUY","NWI"]],[["ISC","LEQ"]],[["ISC","NQY"]],[["LBA","LHR"]],[["LBA","NQY"]],[["LBA","SOU"]],
//     [["LCY","MAN"]],[["LCY","NCL"]],[["LGW","NCL"]],[["LGW","NQY"]],[["LHR","MAN"]],[["LHR","NCL"]],
//     [["LHR","NQY"]],[["LPL","NQY"]],[["MAN","NQY"]],[["MAN","NWI"]],[["MAN","SEN"]],[["MAN","SOU"]],
//     [["MME","NWI"]],[["NCL","SOU"]],[["NQY","SEN"]]]"#
//         )
//             .unwrap()
//     );
//
//     let hard_route_finding_time = Instant::now();
//     let res = db.run_script(
//         r#"
//         reachable[a, choice(p)] := [s airport.iata 'AUS'],
//                                      [r route.src s], [r route.dst a],
//                                      [a airport.iata code], code != 'YYZ', p = ['AUS', code];
//         reachable[a, choice(p)] := reachable[b, prev],
//                                      [r route.src b], [r route.dst a], [a airport.iata code],
//                                      code != 'YYZ', p = append(prev, code);
//         ?[p] := reachable[a, p], [a airport.iata 'YPO'];
//
//         :limit 1;
//     "#,
//         &params,
//     )?;
//     dbg!(hard_route_finding_time.elapsed());
//     assert_eq!(
//         *res.get("rows").unwrap(),
//         serde_json::Value::from_str(
//             r#"[[["AUS","BOS","YTZ","YTS","YMO","YFA","ZKE","YAT","YPO"]]]"#
//         )
//             .unwrap()
//     );
//
//     let na_from_india_time = Instant::now();
//     let res = db.run_script(
//         r#"
//         ?[ind_c, na_c] := [india country.code 'IN'], [ind_a airport.country india],
//                             [r route.src ind_a], [r route.dst na_a],
//                             [na_a airport.country dst_country],
//                             [dst_country country.code dst_country_name],
//                             dst_country_name in ['US', 'CA'],
//                             [ind_a airport.iata ind_c], [na_a airport.iata na_c];
//
//     "#,
//         &params,
//     )?;
//     dbg!(na_from_india_time.elapsed());
//     assert_eq!(
//         *res.get("rows").unwrap(),
//         serde_json::Value::from_str(
//             r#"[
//     ["BOM","EWR"],["BOM","JFK"],["BOM","YYZ"],["DEL","EWR"],["DEL","IAD"],["DEL","JFK"],
//     ["DEL","ORD"],["DEL","SFO"],["DEL","YVR"],["DEL","YYZ"]]"#
//         )
//             .unwrap()
//     );
//
//     let eu_cities_reachable_from_fll_time = Instant::now();
//     let res = db.run_script(
//         r#"
//         ?[city_name] := [a airport.iata 'FLL'],
//                          [r route.src a],
//                          [r route.dst a2],
//                          [cont geo.contains a2],
//                          [cont continent.code 'EU'],
//                          [a2 airport.city city_name];
//     "#,
//         &params,
//     )?;
//     dbg!(eu_cities_reachable_from_fll_time.elapsed());
//     assert_eq!(
//         *res.get("rows").unwrap(),
//         serde_json::Value::from_str(
//             r#"[
//     ["Barcelona"],["Copenhagen"],["London"],["Madrid"],["Oslo"],["Paris"],["Stockholm"]]"#
//         )
//             .unwrap()
//     );
//
//     let clt_to_eu_or_sa_time = Instant::now();
//     let res = db.run_script(
//         r#"
//         ?[code] := [a airport.iata 'CLT'], [r route.src a], [r route.dst a2],
//                     [cont geo.contains a2], [cont continent.code c_name],
//                     c_name in ['EU', 'SA'],
//                     [a2 airport.iata code];
//     "#,
//         &params,
//     )?;
//     dbg!(clt_to_eu_or_sa_time.elapsed());
//     assert_eq!(
//         *res.get("rows").unwrap(),
//         serde_json::Value::from_str(
//             r#"[["BCN"],["CDG"],["DUB"],["FCO"],["FRA"],["GIG"],["GRU"],["LHR"],["MAD"],["MUC"]]"#
//         )
//             .unwrap()
//     );
//
//     let london_to_us_time = Instant::now();
//     let res = db.run_script(
//         r#"
//         ?[l_code, us_code] := l_code in ['LHR', 'LCY', 'LGW', 'LTN', 'STN'],
//                                 [a airport.iata l_code],
//                                 [r route.src a], [r route.dst a2],
//                                 [us country.code 'US'],
//                                 [a2 airport.country us],
//                                 [a2 airport.iata us_code];
//     "#,
//         &params,
//     )?;
//     dbg!(london_to_us_time.elapsed());
//     assert_eq!(
//         *res.get("rows").unwrap(),
//         serde_json::Value::from_str(
//             r#"
//     [["LGW","AUS"],["LGW","BOS"],["LGW","DEN"],["LGW","FLL"],["LGW","JFK"],["LGW","LAS"],
//      ["LGW","LAX"],["LGW","MCO"],["LGW","MIA"],["LGW","OAK"],["LGW","ORD"],["LGW","SEA"],
//      ["LGW","SFO"],["LGW","TPA"],["LHR","ATL"],["LHR","AUS"],["LHR","BNA"],["LHR","BOS"],
//      ["LHR","BWI"],["LHR","CHS"],["LHR","CLT"],["LHR","DEN"],["LHR","DFW"],["LHR","DTW"],
//      ["LHR","EWR"],["LHR","IAD"],["LHR","IAH"],["LHR","JFK"],["LHR","LAS"],["LHR","LAX"],
//      ["LHR","MIA"],["LHR","MSP"],["LHR","MSY"],["LHR","ORD"],["LHR","PDX"],["LHR","PHL"],
//      ["LHR","PHX"],["LHR","PIT"],["LHR","RDU"],["LHR","SAN"],["LHR","SEA"],["LHR","SFO"],
//      ["LHR","SJC"],["LHR","SLC"],["STN","BOS"],["STN","EWR"],["STN","IAD"],["STN","SFB"]]
//     "#
//         )
//             .unwrap()
//     );
//
//     let tx_to_ny_time = Instant::now();
//     let res = db.run_script(
//         r#"
//         ?[tx_code, ny_code] := [a airport.region 'US-TX'],
//                                  [r route.src a],
//                                  [r route.dst a2],
//                                  [a2 airport.region 'US-NY'],
//                                  [a airport.iata tx_code],
//                                  [a2 airport.iata ny_code];
//     "#,
//         &params,
//     )?;
//     dbg!(tx_to_ny_time.elapsed());
//     assert_eq!(
//         *res.get("rows").unwrap(),
//         serde_json::Value::from_str(
//             r#"
//     [["AUS","BUF"],["AUS","EWR"],["AUS","JFK"],["DAL","LGA"],["DFW","BUF"],["DFW","EWR"],
//      ["DFW","JFK"],["DFW","LGA"],["HOU","EWR"],["HOU","JFK"],["HOU","LGA"],["IAH","EWR"],
//      ["IAH","JFK"],["IAH","LGA"],["SAT","EWR"],["SAT","JFK"]]
//     "#
//         )
//             .unwrap()
//     );
//
//     let denver_to_mexico_time = Instant::now();
//     let res = db.run_script(
//         r#"
//         ?[city_name] := [a airport.iata 'DEN'], [r route.src a], [r route.dst a2],
//                          [a2 airport.country ct],
//                          [ct country.code 'MX'],
//                          [a2 airport.city city_name];
//     "#,
//         &params,
//     )?;
//     dbg!(denver_to_mexico_time.elapsed());
//     assert_eq!(
//         *res.get("rows").unwrap(),
//         serde_json::Value::from_str(
//             r#"[
//     ["Cancun"],["Cozumel"],["Guadalajara"],["Mexico City"],["Monterrey"],
//     ["Puerto Vallarta"],["San José del Cabo"]]"#
//         )
//             .unwrap()
//     );
//
//     let three_cities_time = Instant::now();
//     let res = db.run_script(
//         r#"
//         three[a] := city in ['London', 'Munich', 'Paris'], [a airport.city city];
//         ?[src, dst] := three[s], [r route.src s], [r route.dst d], three[d],
//                          [s airport.iata src], [d airport.iata dst];
//     "#,
//         &params,
//     )?;
//     dbg!(three_cities_time.elapsed());
//     assert_eq!(
//         *res.get("rows").unwrap(),
//         serde_json::Value::from_str(
//             r#"[
//     ["CDG","LCY"],["CDG","LGW"],["CDG","LHR"],["CDG","LTN"],["CDG","MUC"],["LCY","CDG"],
//     ["LCY","MUC"],["LCY","ORY"],["LGW","CDG"],["LGW","MUC"],["LHR","CDG"],["LHR","MUC"],
//     ["LHR","ORY"],["LTN","CDG"],["LTN","MUC"],["LTN","ORY"],["MUC","CDG"],["MUC","LCY"],
//     ["MUC","LGW"],["MUC","LHR"],["MUC","LTN"],["MUC","ORY"],["MUC","STN"],["ORY","LCY"],
//     ["ORY","LHR"],["ORY","MUC"],["STN","MUC"]]"#
//         )
//             .unwrap()
//     );
//
//     let long_distance_from_lgw_time = Instant::now();
//     let res = db.run_script(
//         r#"
//         ?[city, dist] := [a airport.iata 'LGW'], [r route.src a], [r route.dst a2],
//                            [r route.distance dist], dist > 4000, [a2 airport.city city];
//     "#,
//         &params,
//     )?;
//     dbg!(long_distance_from_lgw_time.elapsed());
//     assert_eq!(
//         *res.get("rows").unwrap(),
//         serde_json::Value::from_str(
//             r#"
//     [["Austin",4921],["Beijing",5070],["Bridgetown",4197],["Buenos Aires",6908],["Calgary",4380],
//     ["Cancun",4953],["Cape Town",5987],["Chengdu",5156],["Chongqing",5303],["Colombo",5399],
//     ["Denver",4678],["Duong Dong",6264],["Fort Lauderdale",4410],["Havana",4662],["Hong Kong",5982],
//     ["Kigali",4077],["Kingston",4680],["Langkawi",6299],["Las Vegas",5236],["Los Angeles",5463],
//     ["Malé",5287],["Miami",4429],["Montego Bay",4699],["Oakland",5364],["Orlando",4341],
//     ["Port Louis",6053],["Port of Spain",4408],["Punta Cana",4283],["Rayong",6008],
//     ["Rio de Janeiro",5736],["San Francisco",5374],["San Jose",5419],["Seattle",4807],
//     ["Shanghai",5745],["Singapore",6751],["St. George",4076],["Taipei",6080],["Tampa",4416],
//     ["Tianjin",5147],["Vancouver",4731],["Varadero",4618],["Vieux Fort",4222]]"#
//         )
//             .unwrap()
//     );
//
//     let long_routes_one_dir_time = Instant::now();
//     let res = db.run_script(
//         r#"
//         ?[src, dist, dst] := [r route.distance dist], dist > 8000, [r route.src s],
//                                 [r route.dst d], [s airport.iata src], [d airport.iata dst],
//                                 src < dst;
//     "#,
//         &params,
//     )?;
//     dbg!(long_routes_one_dir_time.elapsed());
//     assert_eq!(
//         *res.get("rows").unwrap(),
//         serde_json::Value::from_str(
//             r#"
//     [["AKL",8186,"ORD"],["AKL",8818,"DXB"],["AKL",9025,"DOH"],["ATL",8434,"JNB"],["AUH",8053,"DFW"],
//     ["AUH",8139,"SFO"],["AUH",8372,"LAX"],["CAN",8754,"MEX"],["DFW",8022,"DXB"],["DFW",8105,"HKG"],
//     ["DFW",8574,"SYD"],["DOH",8030,"IAH"],["DOH",8287,"LAX"],["DXB",8085,"SFO"],["DXB",8150,"IAH"],
//     ["DXB",8321,"LAX"],["EWR",8047,"HKG"],["EWR",9523,"SIN"],["HKG",8054,"JFK"],["HKG",8135,"IAD"],
//     ["IAH",8591,"SYD"],["JED",8314,"LAX"],["JFK",8504,"MNL"],["LAX",8246,"RUH"],["LAX",8756,"SIN"],
//     ["LHR",9009,"PER"],["MEL",8197,"YVR"],["PEK",8884,"PTY"],["SCL",8208,"TLV"],["SEA",8059,"SIN"],
//     ["SFO",8433,"SIN"]]"#
//         )
//             .unwrap()
//     );
//
//     let longest_routes_time = Instant::now();
//     let res = db.run_script(
//         r#"
//         ?[src, dist, dst] := [r route.distance dist], dist > 4000, [r route.src s],
//                                 [r route.dst d], [s airport.iata src], [d airport.iata dst],
//                                 src < dst;
//         :sort -dist;
//         :limit 20;
//     "#,
//         &params,
//     )?;
//     dbg!(longest_routes_time.elapsed());
//     assert_eq!(*res.get("rows").unwrap(), serde_json::Value::from_str(r#"
//     [["EWR",9523,"SIN"],["AKL",9025,"DOH"],["LHR",9009,"PER"],["PEK",8884,"PTY"],["AKL",8818,"DXB"],
//     ["LAX",8756,"SIN"],["CAN",8754,"MEX"],["IAH",8591,"SYD"],["DFW",8574,"SYD"],["JFK",8504,"MNL"],
//     ["ATL",8434,"JNB"],["SFO",8433,"SIN"],["AUH",8372,"LAX"],["DXB",8321,"LAX"],["JED",8314,"LAX"],
//     ["DOH",8287,"LAX"],["LAX",8246,"RUH"],["SCL",8208,"TLV"],["MEL",8197,"YVR"],["AKL",8186,"ORD"]]"#).unwrap());
//
//     let longest_routes_from_each_airports = Instant::now();
//     let res = db.run_script(
//         r#"
//         ap[a, max(dist)] := [r route.src a], [r route.distance dist];
//         ?[src, dist, dst] := ap[a, dist], [r route.src a], [r route.distance dist], [r route.dst d],
//                                 [a airport.iata src], [d airport.iata dst];
//         :limit 10;
//     "#,
//         &params,
//     )?;
//     dbg!(longest_routes_from_each_airports.elapsed());
//     assert_eq!(*res.get("rows").unwrap(), serde_json::Value::from_str(r#"
//     [["ANC",3368,"KEF"],["ATL",8434,"JNB"],["AUS",5294,"FRA"],["BNA",4168,"LHR"],["BOS",7952,"HKG"],
//     ["BWI",3622,"LHR"],["DCA",2434,"SFO"],["DFW",8574,"SYD"],["FLL",7808,"DXB"],["IAD",8135,"HKG"]]"#).unwrap());
//
//     let total_distance_from_three_cities_time = Instant::now();
//     let res = db.run_script(
//         r#"
//         three[a] := city in ['London', 'Munich', 'Paris'], [a airport.city city];
//         ?[sum(dist)] := three[a], [r route.src a], [r route.distance dist];
//     "#,
//         &params,
//     )?;
//     dbg!(total_distance_from_three_cities_time.elapsed());
//     assert_eq!(*res.get("rows").unwrap(), json!([[2733379.0]]));
//
//     let total_distance_within_three_cities_time = Instant::now();
//     let res = db.run_script(
//         r#"
//         three[a] := city in ['London', 'Munich', 'Paris'], [a airport.city city];
//         ?[sum(dist)] := three[a], [r route.src a], [r route.dst a2], three[a2],
//                          [r route.distance dist];
//     "#,
//         &params,
//     )?;
//     dbg!(total_distance_within_three_cities_time.elapsed());
//     assert_eq!(*res.get("rows").unwrap(), json!([[10282.0]]));
//
//     let specific_distance_time = Instant::now();
//     let res = db.run_script(
//         r#"
//         ?[dist] := [a airport.iata 'AUS'], [a2 airport.iata 'MEX'], [r route.src a],
//                     [r route.dst a2], [r route.distance dist];
//     "#,
//         &params,
//     )?;
//     dbg!(specific_distance_time.elapsed());
//     assert_eq!(*res.get("rows").unwrap(), json!([[748]]));
//
//     let n_routes_between_time = Instant::now();
//     let res = db.run_script(
//         r#"
//         us_a[a] := [us country.code 'US'], [us geo.contains a];
//         ?[count(r)] := [r route.distance dist], dist >= 100, dist <= 200,
//                         [r route.src s], us_a[s],
//                         [r route.dst d], us_a[d];
//     "#,
//         &params,
//     )?;
//     dbg!(n_routes_between_time.elapsed());
//     assert_eq!(*res.get("rows").unwrap(), json!([[597]]));
//
//     let one_stop_distance_time = Instant::now();
//     let res = db.run_script(
//         r#"
//         ?[code, dist] := [s airport.iata 'AUS'], [r1 route.src s], [r1 route.dst a],
//            [r2 route.src a], [r2 route.dst d], [d airport.iata 'LHR'],
//            [r1 route.distance dis1], [r2 route.distance dis2], dist = dis1 + dis2,
//            [a airport.iata code];
//         :order dist;
//         :limit 10;
//     "#,
//         &params,
//     )?;
//     dbg!(one_stop_distance_time.elapsed());
//     assert_eq!(
//         *res.get("rows").unwrap(),
//         serde_json::Value::from_str(
//             r#"
//     [["DTW",4893],["YYZ",4901],["ORD",4912],["PIT",4916],["BNA",4923],["DFW",4926],["BOS",4944],
//     ["EWR",4953],["IAD",4959],["JFK",4960]]"#
//         )
//             .unwrap()
//     );
//
//     let airport_most_routes_time = Instant::now();
//     let res = db.run_script(
//         r#"
//         ac[a, count(r)] := [r route.src a];
//         ?[code, n] := ac[a, n], [a airport.iata code];
//         :order -n;
//         :limit 10;
//     "#,
//         &params,
//     )?;
//     dbg!(airport_most_routes_time.elapsed());
//     assert_eq!(
//         *res.get("rows").unwrap(),
//         serde_json::Value::from_str(
//             r#"
//     [["FRA",307],["IST",307],["CDG",293],["AMS",282],["MUC",270],
//     ["ORD",264],["DFW",251],["PEK",248],["DXB",247],["ATL",242]]
//     "#
//         )
//             .unwrap()
//     );
//
//     let north_of_77_time = Instant::now();
//     let res = db.run_script(r#"
//         ?[city, latitude] := [a airport.lat lat], lat > 77, [a airport.city city], latitude = round(lat);
//     "#, &params,
//     )?;
//     dbg!(north_of_77_time.elapsed());
//     assert_eq!(
//         *res.get("rows").unwrap(),
//         serde_json::Value::from_str(r#"[["Longyearbyen",78.0],["Qaanaaq",77.0]]"#).unwrap()
//     );
//
//     let greenwich_meridian_time = Instant::now();
//     let res = db.run_script(
//         r#"
//         ?[code] := [a airport.lon lon], lon > -0.1, lon < 0.1, [a airport.iata code];
//     "#,
//         &params,
//     )?;
//     dbg!(greenwich_meridian_time.elapsed());
//     assert_eq!(
//         *res.get("rows").unwrap(),
//         json!([["CDT"], ["LCY"], ["LDE"], ["LEH"]])
//     );
//
//     let box_around_heathrow_time = Instant::now();
//     let res = db.run_script(
//         r#"
//         h_box[lhr_lon, lhr_lat] := [lhr airport.iata 'LHR'],
//                                      [lhr airport.lon lhr_lon],
//                                      [lhr airport.lat lhr_lat];
//         ?[code] := h_box[lhr_lon, lhr_lat], [a airport.lon lon], [a airport.lat lat],
//                     abs(lhr_lon - lon) < 1, abs(lhr_lat - lat) < 1, [a airport.iata code];
//     "#,
//         &params,
//     )?;
//     dbg!(box_around_heathrow_time.elapsed());
//     assert_eq!(
//         *res.get("rows").unwrap(),
//         json!([["LCY"], ["LGW"], ["LHR"], ["LTN"], ["SOU"], ["STN"]])
//     );
//
//     let dfw_by_region_time = Instant::now();
//     let res = db.run_script(
//         r#"
//         ?[region, collect(code)] := [dfw airport.iata 'DFW'],
//                                       [us country.code 'US'],
//                                       [r route.src dfw],
//                                       [r route.dst a], [a airport.country us],
//                                       region in ['US-CA', 'US-TX', 'US-FL', 'US-CO', 'US-IL'],
//                                       [a airport.region region],
//                                       [a airport.iata code];
//     "#,
//         &params,
//     )?;
//     dbg!(dfw_by_region_time.elapsed());
//     assert_eq!(*res.get("rows").unwrap(), serde_json::Value::from_str(r#"
//     [["US-CA",["BFL","BUR","FAT","LAX","MRY","OAK","ONT","PSP","SAN","SBA","SFO","SJC","SMF","SNA"]],
//     ["US-CO",["ASE","COS","DEN","DRO","EGE","GJT","GUC","HDN","MTJ"]],
//     ["US-FL",["ECP","EYW","FLL","GNV","JAX","MCO","MIA","PBI","PNS","RSW","SRQ","TLH","TPA","VPS"]],
//     ["US-IL",["BMI","CMI","MLI","ORD","PIA","SPI"]],
//     ["US-TX",["ABI","ACT","AMA","AUS","BPT","BRO","CLL","CRP","DRT","ELP","GGG","GRK","HOU","HRL",
//               "IAH","LBB","LRD","MAF","MFE","SAT","SJT","SPS","TYR"]]]
//     "#).unwrap());
//
//     let great_circle_distance = Instant::now();
//     let res = db.run_script(
//         r#"
//         ?[deg_diff] := [a airport.iata 'SFO'], [a airport.lat a_lat], [a airport.lon a_lon],
//                         [b airport.iata 'NRT'], [b airport.lat b_lat], [b airport.lon b_lon],
//                         deg_diff = round(haversine_deg_input(a_lat, a_lon, b_lat, b_lon));
//     "#,
//         &params,
//     )?;
//     dbg!(great_circle_distance.elapsed());
//     assert_eq!(*res.get("rows").unwrap(), json!([[1.0]]));
//
//     let aus_to_edi_time = Instant::now();
//     let res = db.run_script(
//         r#"
//         us_uk_airports[a] := [c country.code 'UK'], [a airport.country c];
//         us_uk_airports[a] := [c country.code 'US'], [a airport.country c];
//         routes[a2, shortest(path)] := [a airport.iata 'AUS'], [r route.src a],
//                                         [r route.dst a2], us_uk_airports[a2],
//                                         [a2 airport.iata dst],
//                                         path = ['AUS', dst];
//         routes[a2, shortest(path)] := routes[a, prev], [r route.src a],
//                                         [r route.dst a2], us_uk_airports[a2],
//                                         [a2 airport.iata dst],
//                                         path = append(prev, dst);
//         ?[path] := [edi airport.iata 'EDI'], routes[edi, path];
//     "#,
//         &params,
//     )?;
//     dbg!(aus_to_edi_time.elapsed());
//     assert_eq!(*res.get("rows").unwrap(), json!([[["AUS", "BOS", "EDI"]]]));
//
//     let reachable_from_lhr = Instant::now();
//     let res = db.run_script(
//         r#"
//         routes[a2, shortest(path)] := [a airport.iata 'LHR'], [r route.src a],
//                                         [r route.dst a2],
//                                         [a2 airport.iata dst],
//                                         path = ['LHR', dst];
//         routes[a2, shortest(path)] := routes[a, prev], [r route.src a],
//                                         [r route.dst a2],
//                                         [a2 airport.iata dst],
//                                         path = append(prev, dst);
//         ?[len, path] := routes[_, path], len = length(path);
//
//         :order -len;
//         :limit 10;
//     "#,
//         &params,
//     )?;
//     dbg!(reachable_from_lhr.elapsed());
//     assert_eq!(
//         *res.get("rows").unwrap(),
//         serde_json::Value::from_str(
//             r#"
//     [[8,["LHR","YYZ","YTS","YMO","YFA","ZKE","YAT","YPO"]],
//     [7,["LHR","DFW","ANC","AKN","PIP","UGB","PTH"]],[7,["LHR","DFW","ANC","ANI","CHU","CKD","RDV"]],
//     [7,["LHR","DFW","ANC","ANI","CHU","CKD","SLQ"]],[7,["LHR","DFW","ANC","BET","OOK","TNK","WWT"]],
//     [7,["LHR","DFW","SYD","AYQ","MEB","WMB","PTJ"]],[7,["LHR","DFW","SYD","WTB","SGO","CMA","XTG"]],
//     [7,["LHR","KEF","GOH","JAV","JUV","NAQ","THU"]],[7,["LHR","LAX","BNE","ISA","BQL","BEU","BVI"]],
//     [7,["LHR","YUL","YGL","YPX","AKV","YIK","YZG"]]]
//     "#
//         )
//             .unwrap()
//     );
//
//     let furthest_from_lhr = Instant::now();
//     let res = db.run_script(
//         r#"
//         routes[a2, min_cost(cost_pair)] := [a airport.iata 'LHR'], [r route.src a],
//                                              [r route.dst a2],
//                                              [r route.distance dist],
//                                              [a2 airport.iata dst],
//                                              path = ['LHR', dst],
//                                              cost_pair = [path, dist];
//         routes[a2, min_cost(cost_pair)] := routes[a, prev], [r route.src a],
//                                              [r route.dst a2],
//                                              [r route.distance dist],
//                                              [a2 airport.iata dst],
//                                              path = append(first(prev), dst),
//                                              cost_pair = [path, last(prev) + dist];
//         ?[cost, path] := routes[dst, cost_pair], cost = last(cost_pair), path = first(cost_pair);
//
//         :order -cost;
//         :limit 10;
//     "#,
//         &params,
//     )?;
//     dbg!(furthest_from_lhr.elapsed());
//     assert_eq!(
//         *res.get("rows").unwrap(),
//         serde_json::Value::from_str(
//             r#"
//     [[12922,["LHR","JNB","HLE","ASI","BZZ"]],[12114,["LHR","PVG","BNE","CHC","IVC"]],
//      [12030,["LHR","PVG","BNE","CHC","DUD"]],[12015,["LHR","NRT","AKL","WLG","TIU"]],
//      [11921,["LHR","PVG","BNE","CHC","HKK"]],[11910,["LHR","NRT","AKL","WLG","WSZ"]],
//      [11826,["LHR","PVG","BNE","CHC"]],[11766,["LHR","PVG","BNE","ZQN"]],
//      [11758,["LHR","NRT","AKL","BHE"]],[11751,["LHR","NRT","AKL","NSN"]]]
//     "#
//         )
//             .unwrap()
//     );
//
//     let furthest_from_lhr_view = Instant::now();
//     let res = db.run_script(
//         r#"
//         routes[a2, min_cost(cost_pair)] := [a airport.iata 'LHR'], :flies_to[a, a2, dist],
//                                              [a2 airport.iata dst],
//                                              path = ['LHR', dst],
//                                              cost_pair = [path, dist];
//         routes[a2, min_cost(cost_pair)] := routes[a, prev], :flies_to[a, a2, dist],
//                                              [a2 airport.iata dst],
//                                              path = append(first(prev), dst),
//                                              cost_pair = [path, last(prev) + dist];
//         ?[cost, path] := routes[dst, cost_pair], cost = last(cost_pair), path = first(cost_pair);
//
//         :order -cost;
//         :limit 10;
//     "#,
//         &params,
//     )?;
//     dbg!(furthest_from_lhr_view.elapsed());
//     assert_eq!(
//         *res.get("rows").unwrap(),
//         serde_json::Value::from_str(
//             r#"
//     [[12922,["LHR","JNB","HLE","ASI","BZZ"]],[12114,["LHR","PVG","BNE","CHC","IVC"]],
//      [12030,["LHR","PVG","BNE","CHC","DUD"]],[12015,["LHR","NRT","AKL","WLG","TIU"]],
//      [11921,["LHR","PVG","BNE","CHC","HKK"]],[11910,["LHR","NRT","AKL","WLG","WSZ"]],
//      [11826,["LHR","PVG","BNE","CHC"]],[11766,["LHR","PVG","BNE","ZQN"]],
//      [11758,["LHR","NRT","AKL","BHE"]],[11751,["LHR","NRT","AKL","NSN"]]]
//     "#
//         )
//             .unwrap()
//     );
//
//     Ok(())
// }
