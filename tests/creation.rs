use itertools::Itertools;
use log::info;
use serde_json::{json, to_string_pretty};

use cozo::{Db, EntityId, Validity};
use cozorocks::DbBuilder;

fn create_db(name: &str) -> Db {
    let builder = DbBuilder::default()
        .path(name)
        .create_if_missing(true)
        .destroy_on_exit(true);
    Db::build(builder).unwrap()
}

fn init_logger() {
    let _ = env_logger::builder().is_test(true).try_init();
}

fn test_send_sync<T: Send + Sync>(_: &T) {}

#[test]
fn creation() {
    init_logger();
    let db = create_db("_test_db");
    test_send_sync(&db);
    assert!(db.current_schema().unwrap().as_array().unwrap().is_empty());
    let res = db.transact_attributes(&json!({
        "attrs": [
            {"put": {"keyword": "person.idd", "cardinality": "one", "type": "string", "index": "identity", "history": false}},
            {"put": {"keyword": "person.first_name", "cardinality": "one", "type": "string", "index": true}},
            {"put": {"keyword": "person.last_name", "cardinality": "one", "type": "string", "index": true}},
            {"put": {"keyword": "person.age", "cardinality": "one", "type": "int"}},
            {"put": {"keyword": "person.friend", "cardinality": "many", "type": "ref"}},
            {"put": {"keyword": "person.weight", "cardinality": "one", "type": "float"}},
            {"put": {"keyword": "person.covid", "cardinality": "one", "type": "bool"}},
        ]
    }))
    .unwrap();
    info!("{}", res);
    let first_id = res["results"][0][0].as_u64().unwrap();
    let last_id = res["results"][6][0].as_u64().unwrap();
    db.transact_attributes(&json!({
        "attrs": [
            {"put": {"id": first_id, "keyword": ":person.id", "cardinality": "one", "type": "string", "index": "identity", "history": false}},
            {"retract": {"id": last_id, "keyword": ":person.covid", "cardinality": "one", "type": "bool"}}
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
        to_string_pretty(&db.entities_at(None).unwrap()).unwrap()
    );

    let pulled = db
        .pull(
            EntityId::MIN_PERM,
            &json!([
                "_id",
                "person.first_name",
                "person.last_name",
                {"pull":"person.friend", "as": "friends", "recurse": true},
            ]),
            Validity::current(),
        )
        .unwrap();

    info!("{}", to_string_pretty(&pulled).unwrap());

    let query = json!({
        "q": [
            {
                "rule": "ff",
                "args": [["?a", "?b"], ["?a", "person.friend", "?b"]]
            },
            {
                "rule": "ff",
                "args": [["?a", "?b"], ["?a", "person.friend", "?c"], {"rule": "ff", "args": ["?c", "?b"]}]
            },
            {
                "rule": "?",
                "args": [["?a"],
                    ["?alice", "person.first_name", "Alice"],
                    {"rule": "ff", "args": ["?alice", "?a"]},
                    // {"not_exists": {"rule": "ff", "args": ["?alice", "?a"]}},
                    ["?a", "person.first_name", "?n"],
                ]
            }
        ],
        "out": {"friend": {"pull": "?a", "spec": ["person.first_name"]}}
    });
    let mut tx = db.transact().unwrap();
    let ret = tx.run_query(&query).unwrap();
    let res: Vec<_> = ret.try_collect().unwrap();
    let res = json!(res);
    let res = to_string_pretty(&res).unwrap();
    info!("{}", res);

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
