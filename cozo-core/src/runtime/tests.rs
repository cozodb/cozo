/*
 *  Copyright 2022, The Cozo Project Authors.
 *
 *  This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 *  If a copy of the MPL was not distributed with this file,
 *  You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 */

use itertools::Itertools;
use log::debug;
use serde_json::json;

use crate::new_cozo_mem;

#[test]
fn test_limit_offset() {
    let db = new_cozo_mem().unwrap();
    let res = db
        .run_script("?[a] := a in [5,3,1,2,4] :limit 2", Default::default())
        .unwrap()
        .rows
        .into_iter()
        .flatten()
        .collect_vec();
    assert_eq!(json!(res), json!([3, 5]));
    let res = db
        .run_script(
            "?[a] := a in [5,3,1,2,4] :limit 2 :offset 1",
            Default::default(),
        )
        .unwrap()
        .rows
        .into_iter()
        .flatten()
        .collect_vec();
    assert_eq!(json!(res), json!([1, 3]));
    let res = db
        .run_script(
            "?[a] := a in [5,3,1,2,4] :limit 2 :offset 4",
            Default::default(),
        )
        .unwrap()
        .rows
        .into_iter()
        .flatten()
        .collect_vec();
    assert_eq!(json!(res), json!([4]));
    let res = db
        .run_script(
            "?[a] := a in [5,3,1,2,4] :limit 2 :offset 5",
            Default::default(),
        )
        .unwrap()
        .rows
        .into_iter()
        .flatten()
        .collect_vec();
    assert_eq!(json!(res), json!([]));
}
#[test]
fn test_normal_aggr_empty() {
    let db = new_cozo_mem().unwrap();
    let res = db
        .run_script("?[count(a)] := a in []", Default::default())
        .unwrap()
        .rows;
    assert_eq!(res, vec![vec![json!(0)]]);
}
#[test]
fn test_meet_aggr_empty() {
    let db = new_cozo_mem().unwrap();
    let res = db
        .run_script("?[min(a)] := a in []", Default::default())
        .unwrap()
        .rows;
    assert_eq!(res, vec![vec![json!(null)]]);

    let res = db
        .run_script("?[min(a), count(a)] := a in []", Default::default())
        .unwrap()
        .rows;
    assert_eq!(res, vec![vec![json!(null), json!(0)]]);
}
#[test]
fn test_layers() {
    let _ = env_logger::builder().is_test(true).try_init();

    let db = new_cozo_mem().unwrap();
    let res = db
        .run_script(
            r#"
        y[a] := a in [1,2,3]
        x[sum(a)] := y[a]
        x[sum(a)] := a in [4,5,6]
        ?[sum(a)] := x[a]
        "#,
            Default::default(),
        )
        .unwrap()
        .rows;
    assert_eq!(res[0][0], json!(21.))
}
#[test]
fn test_conditions() {
    let _ = env_logger::builder().is_test(true).try_init();
    let db = new_cozo_mem().unwrap();
    db.run_script(
        r#"
        {
            ?[code] <- [['a'],['b'],['c']]
            :create airport {code}
        }
        {
            ?[fr, to, dist] <- [['a', 'b', 1.1], ['a', 'c', 0.5], ['b', 'c', 9.1]]
            :create route {fr, to => dist}
        }
        "#,
        Default::default(),
    )
    .unwrap();
    debug!("real test begins");
    let res = db
        .run_script(
            r#"
        r[code, dist] := *airport{code}, *route{fr: code, dist};
        ?[dist] := r['a', dist], dist > 0.5, dist <= 1.1;
        "#,
            Default::default(),
        )
        .unwrap()
        .rows;
    assert_eq!(res[0][0], json!(1.1))
}
#[test]
fn test_classical() {
    let _ = env_logger::builder().is_test(true).try_init();
    let db = new_cozo_mem().unwrap();
    let res = db
        .run_script(
            r#"
parent[] <- [['joseph', 'jakob'],
             ['jakob', 'issac'],
             ['issac', 'abraham']]
grandparent[gcld, gp] := parent[gcld, p], parent[p, gp]
?[who] := grandparent[who, 'abraham']
        "#,
            Default::default(),
        )
        .unwrap()
        .rows;
    println!("{:?}", res);
    assert_eq!(res[0][0], json!("jakob"))
}

#[test]
fn default_columns() {
    let db = new_cozo_mem().unwrap();

    db.run_script(
        r#"
            :create status {uid: String, ts default now() => quitted: Bool, mood: String}
            "#,
        Default::default(),
    )
    .unwrap();

    db.run_script(
        r#"
        ?[uid, quitted, mood] <- [['z', true, 'x']]
            :put status {uid => quitted, mood}
        "#,
        Default::default(),
    )
    .unwrap();
}

#[test]
fn rm_does_not_need_all_keys() {
    let db = new_cozo_mem().unwrap();
    db.run_script(":create status {uid => mood}", Default::default())
        .unwrap();
    assert!(db
        .run_script(
            "?[uid, mood] <- [[1, 2]] :put status {uid => mood}",
            Default::default()
        )
        .is_ok());
    assert!(db
        .run_script(
            "?[uid, mood] <- [[2]] :put status {uid}",
            Default::default()
        )
        .is_err());
    assert!(db
        .run_script(
            "?[uid, mood] <- [[3, 2]] :rm status {uid => mood}",
            Default::default()
        )
        .is_ok());
    assert!(db
        .run_script("?[uid] <- [[1]] :rm status {uid}", Default::default())
        .is_ok());
}

#[test]
fn strict_checks_for_fixed_rules_args() {
    let db = new_cozo_mem().unwrap();
    let res = db.run_script(
        r#"
            r[] <- [[1, 2]]
            ?[] <~ PageRank(r[_, _])
        "#,
        Default::default(),
    );
    assert!(res.is_ok());

    let db = new_cozo_mem().unwrap();
    let res = db.run_script(
        r#"
            r[] <- [[1, 2]]
            ?[] <~ PageRank(r[a, b])
        "#,
        Default::default(),
    );
    assert!(res.is_ok());

    let db = new_cozo_mem().unwrap();
    let res = db.run_script(
        r#"
            r[] <- [[1, 2]]
            ?[] <~ PageRank(r[a, a])
        "#,
        Default::default(),
    );
    assert!(res.is_err());
}

#[test]
fn do_not_unify_underscore() {
    let db = new_cozo_mem().unwrap();
    let res = db
        .run_script(
            r#"
        r1[] <- [[1, 'a'], [2, 'b']]
        r2[] <- [[2, 'B'], [3, 'C']]

        ?[l1, l2] := r1[_ , l1], r2[_ , l2]
        "#,
            Default::default(),
        )
        .unwrap()
        .rows;
    assert_eq!(res.len(), 4);

    let res = db.run_script(
        r#"
        ?[_] := _ = 1
        "#,
        Default::default(),
    );
    assert!(res.is_err());

    let res = db
        .run_script(
            r#"
        ?[x] := x = 1, _ = 1, _ = 2
        "#,
            Default::default(),
        )
        .unwrap()
        .rows;

    assert_eq!(res.len(), 1);
}

#[test]
fn returning_relations() {
    let db = new_cozo_mem().unwrap();
    let res = db
        .run_script(
            r#"
        {
            ?[] <- [[1,2,3]]
            :yield nxt
        }
        {
            ?[a,b,c] := nxt[a, b, c]
        }
        "#,
            Default::default(),
        )
        .unwrap()
        .rows;
    assert_eq!(json!(res), json!([[1, 2, 3]]));

    let res = db
        .run_script(
            r#"
            {
                ?[a] <- [[1]]
                :yield first_yield
            }
            {
                ?[a] := first_yield[b], a = b + 1
                :yield second_yield
            }
            {
                ?[a] := first_yield[a]
                ?[a] := second_yield[a]
            }
        "#,
            Default::default(),
        )
        .unwrap()
        .rows;
    assert_eq!(json!(res), json!([[1], [2]]));

    let res = db.run_script(
        r#"
        {
            ?[] <- [[1,2,3]]
            :yield nxt
        }
        {
            nxt[] <- [[2, 3, 5]]
            ?[a,b,c] := nxt[a, b, c]
        }
        "#,
        Default::default(),
    );
    assert!(res.is_err());
}

#[test]
fn test_trigger() {
    let db = new_cozo_mem().unwrap();
    db.run_script(":create friends {fr: Int, to: Int}", Default::default())
        .unwrap();
    db.run_script(":create friends.rev {to: Int, fr: Int}", Default::default())
        .unwrap();
    db.run_script(
        r#"
        ::set_triggers friends

        on put {
            ?[fr, to] := _new[fr, to]

            :put friends.rev{ to, fr }
        }
        on rm {
            ?[fr, to] := _old[fr, to]

            :rm friends.rev{ to, fr }
        }
        "#,
        Default::default(),
    )
    .unwrap();
    db.run_script(
        r"?[fr, to] <- [[1,2]] :put friends {fr, to}",
        Default::default(),
    )
    .unwrap();
    let ret = db
        .export_relations(["friends", "friends.rev"].into_iter())
        .unwrap();
    let frs = ret.get("friends").unwrap();
    assert_eq!(vec![json!(1), json!(2)], frs.rows[0]);

    let frs_rev = ret.get("friends.rev").unwrap();
    assert_eq!(vec![json!(2), json!(1)], frs_rev.rows[0]);
}
