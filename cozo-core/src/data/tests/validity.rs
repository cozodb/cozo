/*
 *  Copyright 2022, The Cozo Project Authors.
 *
 *  This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 *  If a copy of the MPL was not distributed with this file,
 *  You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 */

use crate::data::value::DataValue;
use crate::DbInstance;
use serde_json::json;
use std::env;

#[test]
fn test_validity() {
    let path = "_test_validity";
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_dir_all(path);
    let db_kind = env::var("COZO_TEST_DB_ENGINE").unwrap_or("mem".to_string());
    println!("Using {} engine", db_kind);
    let db = DbInstance::new(&db_kind, path, Default::default()).unwrap();

    db.run_script(":create vld {a, v: Validity => d}", Default::default())
        .unwrap();

    assert!(db
        .run_script(
            r#"
    ?[a, v, d] <- [[1, [9223372036854775807, true], null]]
    :put vld {a, v => d}
    "#,
            Default::default(),
        )
        .is_err());

    assert!(db
        .run_script(
            r#"
    ?[a, v, d] <- [[1, [-9223372036854775808, true], null]]
    :put vld {a, v => d}
    "#,
            Default::default(),
        )
        .is_err());

    db.run_script(
        r#"
    ?[a, v, d] <- [[1, [0, true], 0]]
    :put vld {a, v => d}
    "#,
        Default::default(),
    )
    .unwrap();

    let res = db
        .run_script(
            r#"
        ?[a, v, d] := *vld{a, v, d @ "NOW"}
    "#,
            Default::default(),
        )
        .unwrap()
        .rows;
    assert_eq!(res.len(), 1);

    let res = db
        .run_script(
            r#"
        ?[a, v, d] := *vld{a, v, d}
    "#,
            Default::default(),
        )
        .unwrap()
        .rows;
    assert_eq!(res.len(), 1);

    db.run_script(
        r#"
    ?[a, v, d] <- [[1, [1, false], 1]]
    :put vld {a, v => d}
    "#,
        Default::default(),
    )
    .unwrap();

    let res = db
        .run_script(
            r#"
        ?[a, v, d] := *vld{a, v, d @ "NOW"}
    "#,
            Default::default(),
        )
        .unwrap()
        .rows;
    assert_eq!(res.len(), 0);

    let res = db
        .run_script(
            r#"
        ?[a, v, d] := *vld{a, v, d}
    "#,
            Default::default(),
        )
        .unwrap()
        .rows;
    assert_eq!(res.len(), 2);

    db.run_script(
        r#"
    ?[a, v, d] <- [[1, "ASSERT", 2]]
    :put vld {a, v => d}
    "#,
        Default::default(),
    )
    .unwrap();

    let res = db
        .run_script(
            r#"
        ?[a, v, d] := *vld{a, v, d @ "NOW"}
    "#,
            Default::default(),
        )
        .unwrap()
        .rows;
    assert_eq!(res.len(), 1);
    assert_eq!(res[0][2].get_int().unwrap(), 2);

    let res = db
        .run_script(
            r#"
        ?[a, v, d] := *vld{a, v, d}
    "#,
            Default::default(),
        )
        .unwrap()
        .rows;
    assert_eq!(res.len(), 3);

    db.run_script(
        r#"
    ?[a, v, d] <- [[1, "RETRACT", 3]]
    :put vld {a, v => d}
    "#,
        Default::default(),
    )
    .unwrap();

    let res = db
        .run_script(
            r#"
        ?[a, v, d] := *vld{a, v, d @ "NOW"}
    "#,
            Default::default(),
        )
        .unwrap()
        .rows;
    assert_eq!(res.len(), 0);

    let res = db
        .run_script(
            r#"
        ?[a, v, d] := *vld{a, v, d}
    "#,
            Default::default(),
        )
        .unwrap()
        .rows;
    assert_eq!(res.len(), 4);
    db.run_script(
        r#"
    ?[a, v, d] <- [[1, [9223372036854775806, true], null]]
    :put vld {a, v => d}
    "#,
        Default::default(),
    )
    .unwrap();

    let res = db
        .run_script(
            r#"
        ?[a, v, d] := *vld{a, v, d @ "NOW"}
    "#,
            Default::default(),
        )
        .unwrap()
        .rows;
    assert_eq!(res.len(), 0);

    let res = db
        .run_script(
            r#"
        ?[a, v, d] := *vld{a, v, d @ "END"}
    "#,
            Default::default(),
        )
        .unwrap()
        .rows;
    assert_eq!(res.len(), 1);
    assert_eq!(res[0][2], DataValue::Null);

    let res = db
        .run_script(
            r#"
        ?[a, v, d] := *vld{a, v, d}
    "#,
            Default::default(),
        )
        .unwrap()
        .rows;
    assert_eq!(res.len(), 5);

    println!("{}", json!(res));
}
