use log::info;
use serde_json::{json, to_string_pretty};

use cozo::{Db, EncodedVec};
use cozorocks::DbBuilder;

fn create_db(name: &str, destroy_on_exit: bool) -> Db {
    let builder = DbBuilder::default()
        .path(name)
        .create_if_missing(true)
        .destroy_on_exit(destroy_on_exit);
    Db::build(builder).unwrap()
}

// #[test]
// fn air() {
//     init_logger();
//     let db = create_db("cozopy/db_test_flights", false);
//     let mut it = db.total_iter();
//     while let Some((k, v)) = it.pair().unwrap() {
//         println!("{:?}", EncodedVec::from(k));
//         it.next();
//     }
// }

fn init_logger() {
    let _ = env_logger::builder().is_test(true).try_init();
}

fn test_send_sync<T: Send + Sync>(_: &T) {}

#[test]
fn creation() {
    init_logger();
    let db = create_db("_test_db", true);
    test_send_sync(&db);
    assert!(db.current_schema().unwrap().as_array().unwrap().is_empty());
    let res = db.transact_attributes(&json!({
        "attrs": [
            {"put": {"name": "person.idd", "cardinality": "one", "type": "string", "index": "identity", "history": false}},
            {"put": {"name": "person.first_name", "cardinality": "one", "type": "string", "index": true}},
            {"put": {"name": "person.last_name", "cardinality": "one", "type": "string", "index": true}},
            {"put": {"name": "person.age", "cardinality": "one", "type": "int"}},
            {"put": {"name": "person.friend", "cardinality": "many", "type": "ref"}},
            {"put": {"name": "person.weight", "cardinality": "one", "type": "float"}},
            {"put": {"name": "person.covid", "cardinality": "one", "type": "bool"}},
        ]
    }))
    .unwrap();
    info!("{}", res);
    let first_id = res["results"][0][0].as_u64().unwrap();
    let last_id = res["results"][6][0].as_u64().unwrap();
    db.transact_attributes(&json!({
        "attrs": [
            {"put": {"id": first_id, "name": "person.id", "cardinality": "one", "type": "string", "index": "identity", "history": false}},
            {"retract": {"id": last_id, "name": "person.covid", "cardinality": "one", "type": "bool"}}
        ]
    })).unwrap();
    assert_eq!(db.current_schema().unwrap().as_array().unwrap().len(), 6);
    info!(
        "{}",
        to_string_pretty(&db.current_schema().unwrap()).unwrap()
    );
    db.transact_triples(&json!({
        "tx": [
            {"put": {
                "_temp_id": "alice",
                "person.first_name": "Alice",
                "person.age": 7,
                "person.last_name": "Amorist",
                "person.id": "alice_amorist",
                "person.weight": 25,
                "person.friend": "eve"}},
            {"put": {
                "_temp_id": "bob",
                "person.first_name": "Bob",
                "person.age": 70,
                "person.last_name": "Wonderland",
                "person.id": "bob_wonderland",
                "person.weight": 100,
                "person.friend": "alice"
            }},
            {"put": {
                "_temp_id": "eve",
                "person.first_name": "Eve",
                "person.age": 18,
                "person.last_name": "Faking",
                "person.id": "eve_faking",
                "person.weight": 50,
                "person.friend": [
                    "alice",
                    "bob",
                    {
                        "person.first_name": "Charlie",
                        "person.age": 22,
                        "person.last_name": "Goodman",
                        "person.id": "charlie_goodman",
                        "person.weight": 120,
                        "person.friend": "eve"
                    }
                ]
            }},
            {"put": {
                "_temp_id": "david",
                "person.first_name": "David",
                "person.age": 7,
                "person.last_name": "Dull",
                "person.id": "david_dull",
                "person.weight": 25,
                "person.friend": {
                    "_temp_id": "george",
                    "person.first_name": "George",
                    "person.age": 7,
                    "person.last_name": "Geomancer",
                    "person.id": "george_geomancer",
                    "person.weight": 25,
                    "person.friend": "george"}}},
        ]
    }))
    .unwrap();

    info!(
        "{}",
        to_string_pretty(&db.entities_at(&json!(null)).unwrap()).unwrap()
    );

    let pulled = db
        .pull(
            &json!(10000001),
            &json!([
                "_id",
                "person.first_name",
                "person.last_name",
                {"pull":"person.friend", "as": "friends", "recurse": true},
            ]),
            &json!(()),
        )
        .unwrap();

    info!("{}", to_string_pretty(&pulled).unwrap());

    let query = r#"
    friend_of_friend[?a, ?b] := [?a person.friend ?b];
    friend_of_friend[?a, ?b] := [?a person.friend ?c], friend_of_friend[?c, ?b];

    ?[?a, ?n] := [?alice person.first_name "Alice"],
                 not friend_of_friend[?alice, ?a],
                 [?a person.first_name ?n];

    :limit 1;
    :out {friend: ?a[person.first_name as first_name,
                     person.last_name as last_name]};
    :sort -?n;
    "#;

    let ret = db.run_script(query).unwrap();
    let res = to_string_pretty(&ret).unwrap();
    info!("{}", res);

    // ?(?c, ?code, ?desc) := country.code[?c, 'CU'] or ?c = 10000239, country.code[?c, ?code], country.desc[?c, ?desc];
    // :limit = 25;
    // :offset = 2;
    // :out = {friend: ?a[] }

    // // iteration
    // let mut it = db.total_iter();
    // while let Some((k_slice, v_slice)) = it.pair().unwrap() {
    //     let key = EncodedVec::new(k_slice);
    //     let val = key.debug_value(v_slice);
    //     dbg!(key);
    //     dbg!(val);
    //     it.next();
    // }
}
