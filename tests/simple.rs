use std::collections::BTreeMap;
use std::fs;

use log::info;
use serde_json::to_string_pretty;

use cozo::Db;
use cozorocks::DbBuilder;

fn create_db(name: &str) -> Db {
    let builder = DbBuilder::default()
        .path(name)
        .create_if_missing(true);
    Db::build(builder).unwrap()
}

fn init_logger() {
    let _ = env_logger::builder().is_test(true).try_init();
}

fn test_send_sync<T: Send + Sync>(_: &T) {}

#[test]
fn simple() {
    init_logger();
    let db = create_db("_test_db");
    test_send_sync(&db);
    let params: BTreeMap<String, serde_json::Value> = Default::default();
    db.run_script(
        r#"
        :schema
        :put person {
            id: string unique,
            first_name: string index,
            last_name: string index,
            age: int,
            friend: ref many,
            weight: float,
        }
    "#,
        &params,
    )
        .unwrap();
    info!(
        "{}",
        to_string_pretty(&db.current_schema().unwrap()).unwrap()
    );
    db.run_script(
        r#"
        :tx
        {
            _tid: "alice",
            person.first_name: "Alice",
            person.age: 7,
            person.last_name: "Amorist",
            person.id: "alice_amorist",
            person.weight: 25,
            person.friend: "eve"
        }
        {
            _tid: "bob",
            person.first_name: "Bob",
            person.age: 70,
            person.last_name: "Wonderland",
            person.id: "bob_wonderland",
            person.weight: 100,
            person.friend: "alice"
        }
        {
            _tid: "eve",
            person.first_name: "Eve",
            person.age: 18,
            person.last_name: "Faking",
            person.id: "eve_faking",
            person.weight: 50,
            *person.friend: [
                "alice",
                "bob",
                {
                    person.first_name: "Charlie",
                    person.age: 22,
                    person.last_name: "Goodman",
                    person.id: "charlie_goodman",
                    person.weight: 120,
                    person.friend: "eve"
                }
            ]
        }
        {
            _tid: "david",
            person.first_name: "David",
            person.age: 7,
            person.last_name: "Dull",
            person.id: "david_dull",
            person.weight: 25,
            person.friend: {
                _tid: "george",
                person.first_name: "George",
                person.age: 7,
                person.last_name: "Geomancer",
                person.id: "george_geomancer",
                person.weight: 25,
                person.friend: "george"},
        }
    "#,
        &params,
    )
        .unwrap();
    let query = r#"
    friend_of_friend[a, b] := [a person.friend b];
    friend_of_friend[a, b] := [a person.friend c], friend_of_friend[c, b];

    ?[a, n] := [alice person.first_name "Alice"],
               not friend_of_friend[alice, a],
               [a person.first_name n];

    # :limit 1;
    # :out {friend: ?a[person.first_name as first_name,
    #                  person.last_name as last_name]};
    :sort -n;
    "#;

    let ret = db.run_script(query, &params).unwrap();
    let res = to_string_pretty(&ret).unwrap();
    println!("{}", res);
    fs::remove_dir_all("_test_db").unwrap();
}
