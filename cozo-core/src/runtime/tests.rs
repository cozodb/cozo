/*
 *  Copyright 2022, The Cozo Project Authors.
 *
 *  This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 *  If a copy of the MPL was not distributed with this file,
 *  You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 */

use std::collections::BTreeMap;
use std::time::Duration;

use itertools::Itertools;
use log::debug;
use serde_json::json;
use smartstring::{LazyCompact, SmartString};

use crate::data::expr::Expr;
use crate::data::symb::Symbol;
use crate::data::value::DataValue;
use crate::fixed_rule::FixedRulePayload;
use crate::parse::SourceSpan;
use crate::runtime::callback::CallbackOp;
use crate::runtime::db::Poison;
use crate::{new_cozo_mem, DbInstance, FixedRule, RegularTempStore};

#[test]
fn test_limit_offset() {
    let db = new_cozo_mem().unwrap();
    let res = db
        .run_script("?[a] := a in [5,3,1,2,4] :limit 2", Default::default())
        .unwrap()
        .into_json();
    assert_eq!(res["rows"], json!([[3], [5]]));
    let res = db
        .run_script(
            "?[a] := a in [5,3,1,2,4] :limit 2 :offset 1",
            Default::default(),
        )
        .unwrap()
        .into_json();
    assert_eq!(res["rows"], json!([[1], [3]]));
    let res = db
        .run_script(
            "?[a] := a in [5,3,1,2,4] :limit 2 :offset 4",
            Default::default(),
        )
        .unwrap()
        .into_json();
    assert_eq!(res["rows"], json!([[4]]));
    let res = db
        .run_script(
            "?[a] := a in [5,3,1,2,4] :limit 2 :offset 5",
            Default::default(),
        )
        .unwrap()
        .into_json();
    assert_eq!(res["rows"], json!([]));
}
#[test]
fn test_normal_aggr_empty() {
    let db = new_cozo_mem().unwrap();
    let res = db
        .run_script("?[count(a)] := a in []", Default::default())
        .unwrap()
        .rows;
    assert_eq!(res, vec![vec![DataValue::from(0)]]);
}
#[test]
fn test_meet_aggr_empty() {
    let db = new_cozo_mem().unwrap();
    let res = db
        .run_script("?[min(a)] := a in []", Default::default())
        .unwrap()
        .rows;
    assert_eq!(res, vec![vec![DataValue::Null]]);

    let res = db
        .run_script("?[min(a), count(a)] := a in []", Default::default())
        .unwrap()
        .rows;
    assert_eq!(res, vec![vec![DataValue::Null, DataValue::from(0)]]);
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
    assert_eq!(res[0][0], DataValue::from(21.))
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
    assert_eq!(res[0][0], DataValue::from(1.1))
}
#[test]
fn test_classical() {
    let _ = env_logger::builder().is_test(true).try_init();
    let db = new_cozo_mem().unwrap();
    let res = db
        .run_script(
            r#"
parent[] <- [['joseph', 'jakob'],
             ['jakob', 'isaac'],
             ['isaac', 'abraham']]
grandparent[gcld, gp] := parent[gcld, p], parent[p, gp]
?[who] := grandparent[who, 'abraham']
        "#,
            Default::default(),
        )
        .unwrap()
        .rows;
    println!("{:?}", res);
    assert_eq!(res[0][0], DataValue::from("jakob"))
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
fn imperative_script() {
    let db = new_cozo_mem().unwrap();
    let res = db
        .run_script(
            r#"
        {:create _test {a}}

        %loop
            %if { len[count(x)] := *_test[x]; ?[x] := len[z], x = z >= 10 }
                %then %return _test
            %end
            { ?[a] := a = rand_uuid_v1(); :put _test {a} }
            %debug _test
        %end
    "#,
            Default::default(),
        )
        .unwrap();
    assert_eq!(res.rows.len(), 10);

    let res = db
        .run_script(
            r#"
        {?[a] <- [[1], [2], [3]]
         :replace _test {a}}

        %loop
            { ?[a] := *_test[a]; :limit 1; :rm _test {a} }
            %debug _test

            %if_not _test
            %then %break
            %end
        %end

        %return _test
    "#,
            Default::default(),
        )
        .unwrap();
    assert_eq!(res.rows.len(), 0);

    let res = db.run_script(
        r#"
        {:create _test {a}}

        %loop
            { ?[a] := a = rand_uuid_v1(); :put _test {a} }

            %if { len[count(x)] := *_test[x]; ?[x] := len[z], x = z < 10 }
                %continue
            %end

            %return _test
            %debug _test
        %end
    "#,
        Default::default(),
    );
    if let Err(err) = &res {
        eprintln!("{err:?}");
    }
    assert_eq!(res.unwrap().rows.len(), 10);

    let res = db
        .run_script(
            r#"
        {?[a] <- [[1], [2], [3]]
         :replace _test {a}}
        {?[a] <- []
         :replace _test2 {a}}
        %swap _test _test2
        %return _test
    "#,
            Default::default(),
        )
        .unwrap();
    assert_eq!(res.rows.len(), 0);
}

#[test]
fn returning_relations() {
    let db = new_cozo_mem().unwrap();
    let res = db
        .run_script(
            r#"
        {:create _xxz {a}}
        {?[a] := a in [5,4,1,2,3] :put _xxz {a}}
        {?[a] := *_xxz[a], a % 2 == 0 :rm _xxz {a}}
        {?[a] := *_xxz[b], a = b * 2}
        "#,
            Default::default(),
        )
        .unwrap();
    assert_eq!(res.into_json()["rows"], json!([[2], [6], [10]]));
    let res = db.run_script(
        r#"
        {?[a] := *_xxz[b], a = b * 2}
        "#,
        Default::default(),
    );
    assert!(res.is_err());
}

#[test]
fn test_trigger() {
    let db = new_cozo_mem().unwrap();
    db.run_script(
        ":create friends {fr: Int, to: Int => data: Any}",
        Default::default(),
    )
    .unwrap();
    db.run_script(
        ":create friends.rev {to: Int, fr: Int => data: Any}",
        Default::default(),
    )
    .unwrap();
    db.run_script(
        r#"
        ::set_triggers friends

        on put {
            ?[fr, to, data] := _new[fr, to, data]

            :put friends.rev{ to, fr => data}
        }
        on rm {
            ?[fr, to] := _old[fr, to, data]

            :rm friends.rev{ to, fr }
        }
        "#,
        Default::default(),
    )
    .unwrap();
    db.run_script(
        r"?[fr, to, data] <- [[1,2,3]] :put friends {fr, to => data}",
        Default::default(),
    )
    .unwrap();
    let ret = db
        .export_relations(["friends", "friends.rev"].into_iter())
        .unwrap();
    let frs = ret.get("friends").unwrap();
    assert_eq!(
        vec![DataValue::from(1), DataValue::from(2), DataValue::from(3)],
        frs.rows[0]
    );

    let frs_rev = ret.get("friends.rev").unwrap();
    assert_eq!(
        vec![DataValue::from(2), DataValue::from(1), DataValue::from(3)],
        frs_rev.rows[0]
    );
    db.run_script(
        r"?[fr, to] <- [[1,2], [2,3]] :rm friends {fr, to}",
        Default::default(),
    )
    .unwrap();
    let ret = db
        .export_relations(["friends", "friends.rev"].into_iter())
        .unwrap();
    let frs = ret.get("friends").unwrap();
    assert!(frs.rows.is_empty());
}

#[test]
fn test_callback() {
    let db = new_cozo_mem().unwrap();
    let mut collected = vec![];
    let (_id, receiver) = db.register_callback("friends", None);
    db.run_script(
        ":create friends {fr: Int, to: Int => data: Any}",
        Default::default(),
    )
    .unwrap();
    db.run_script(
        r"?[fr, to, data] <- [[1,2,3],[4,5,6]] :put friends {fr, to => data}",
        Default::default(),
    )
    .unwrap();
    db.run_script(
        r"?[fr, to, data] <- [[1,2,4],[4,7,6]] :put friends {fr, to => data}",
        Default::default(),
    )
    .unwrap();
    db.run_script(
        r"?[fr, to] <- [[1,9],[4,5]] :rm friends {fr, to}",
        Default::default(),
    )
    .unwrap();
    std::thread::sleep(Duration::from_secs_f64(0.01));
    while let Ok(d) = receiver.try_recv() {
        collected.push(d);
    }
    let collected = collected;
    assert_eq!(collected[0].0, CallbackOp::Put);
    assert_eq!(collected[0].1.rows.len(), 2);
    assert_eq!(collected[0].1.rows[0].len(), 3);
    assert_eq!(collected[0].2.rows.len(), 0);
    assert_eq!(collected[1].0, CallbackOp::Put);
    assert_eq!(collected[1].1.rows.len(), 2);
    assert_eq!(collected[1].1.rows[0].len(), 3);
    assert_eq!(collected[1].2.rows.len(), 1);
    assert_eq!(
        collected[1].2.rows[0],
        vec![DataValue::from(1), DataValue::from(2), DataValue::from(3)]
    );
    assert_eq!(collected[2].0, CallbackOp::Rm);
    assert_eq!(collected[2].1.rows.len(), 2);
    assert_eq!(collected[2].1.rows[0].len(), 2);
    assert_eq!(collected[2].2.rows.len(), 1);
    assert_eq!(collected[2].2.rows[0].len(), 3);
}

#[test]
fn test_index() {
    let db = new_cozo_mem().unwrap();
    db.run_script(
        ":create friends {fr: Int, to: Int => data: Any}",
        Default::default(),
    )
    .unwrap();

    db.run_script(
        r"?[fr, to, data] <- [[1,2,3],[4,5,6]] :put friends {fr, to => data}",
        Default::default(),
    )
    .unwrap();

    assert!(db
        .run_script("::index create friends:rev {to, no}", Default::default())
        .is_err());
    db.run_script("::index create friends:rev {to, data}", Default::default())
        .unwrap();

    db.run_script(
        r"?[fr, to, data] <- [[1,2,5],[6,5,7]] :put friends {fr, to => data}",
        Default::default(),
    )
    .unwrap();
    db.run_script(
        r"?[fr, to] <- [[4,5]] :rm friends {fr, to}",
        Default::default(),
    )
    .unwrap();

    let rels_data = db
        .export_relations(["friends", "friends:rev"].into_iter())
        .unwrap();
    assert_eq!(
        rels_data["friends"].clone().into_json()["rows"],
        json!([[1, 2, 5], [6, 5, 7]])
    );
    assert_eq!(
        rels_data["friends:rev"].clone().into_json()["rows"],
        json!([[2, 5, 1], [5, 7, 6]])
    );

    let rels = db.run_script("::relations", Default::default()).unwrap();
    assert_eq!(rels.rows[1][0], DataValue::from("friends:rev"));
    assert_eq!(rels.rows[1][1], DataValue::from(3));
    assert_eq!(rels.rows[1][2], DataValue::from("index"));

    let cols = db
        .run_script("::columns friends:rev", Default::default())
        .unwrap();
    assert_eq!(cols.rows.len(), 3);

    let res = db
        .run_script(
            "?[fr, data] := *friends:rev{to: 2, fr, data}",
            Default::default(),
        )
        .unwrap();
    assert_eq!(res.into_json()["rows"], json!([[1, 5]]));

    let res = db
        .run_script(
            "?[fr, data] := *friends{to: 2, fr, data}",
            Default::default(),
        )
        .unwrap();
    assert_eq!(res.into_json()["rows"], json!([[1, 5]]));

    let expl = db
        .run_script(
            "::explain { ?[fr, data] := *friends{to: 2, fr, data} }",
            Default::default(),
        )
        .unwrap();
    let joins = expl.into_json()["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row.as_array().unwrap()[5].clone())
        .collect_vec();
    assert!(joins.contains(&json!(":friends:rev")));
}

#[test]
fn test_custom_rules() {
    let db = new_cozo_mem().unwrap();
    struct Custom;

    impl FixedRule for Custom {
        fn arity(
            &self,
            _options: &BTreeMap<SmartString<LazyCompact>, Expr>,
            _rule_head: &[Symbol],
            _span: SourceSpan,
        ) -> miette::Result<usize> {
            Ok(1)
        }

        fn run(
            &self,
            payload: FixedRulePayload<'_, '_>,
            out: &'_ mut RegularTempStore,
            _poison: Poison,
        ) -> miette::Result<()> {
            let rel = payload.get_input(0)?;
            let mult = payload.integer_option("mult", Some(2))?;
            for maybe_row in rel.iter()? {
                let row = maybe_row?;
                let mut sum = 0;
                for col in row {
                    let d = col.get_int().unwrap_or(0);
                    sum += d;
                }
                sum *= mult;
                out.put(vec![DataValue::from(sum)])
            }
            Ok(())
        }
    }

    db.register_fixed_rule("SumCols".to_string(), Custom)
        .unwrap();
    let res = db
        .run_script(
            r#"
        rel[] <- [[1,2,3,4],[5,6,7,8]]
        ?[x] <~ SumCols(rel[], mult: 100)
    "#,
            Default::default(),
        )
        .unwrap();
    assert_eq!(res.into_json()["rows"], json!([[1000], [2600]]));
}

#[test]
fn test_index_short() {
    let db = new_cozo_mem().unwrap();
    db.run_script(
        ":create friends {fr: Int, to: Int => data: Any}",
        Default::default(),
    )
    .unwrap();

    db.run_script(
        r"?[fr, to, data] <- [[1,2,3],[4,5,6]] :put friends {fr, to => data}",
        Default::default(),
    )
    .unwrap();

    db.run_script("::index create friends:rev {to}", Default::default())
        .unwrap();

    db.run_script(
        r"?[fr, to, data] <- [[1,2,5],[6,5,7]] :put friends {fr, to => data}",
        Default::default(),
    )
    .unwrap();
    db.run_script(
        r"?[fr, to] <- [[4,5]] :rm friends {fr, to}",
        Default::default(),
    )
    .unwrap();

    let rels_data = db
        .export_relations(["friends", "friends:rev"].into_iter())
        .unwrap();
    assert_eq!(
        rels_data["friends"].clone().into_json()["rows"],
        json!([[1, 2, 5], [6, 5, 7]])
    );
    assert_eq!(
        rels_data["friends:rev"].clone().into_json()["rows"],
        json!([[2, 1], [5, 6]])
    );

    let rels = db.run_script("::relations", Default::default()).unwrap();
    assert_eq!(rels.rows[1][0], DataValue::from("friends:rev"));
    assert_eq!(rels.rows[1][1], DataValue::from(2));
    assert_eq!(rels.rows[1][2], DataValue::from("index"));

    let cols = db
        .run_script("::columns friends:rev", Default::default())
        .unwrap();
    assert_eq!(cols.rows.len(), 2);

    let expl = db
        .run_script(
            "::explain { ?[fr, data] := *friends{to: 2, fr, data} }",
            Default::default(),
        )
        .unwrap()
        .into_json();

    for row in expl["rows"].as_array().unwrap() {
        println!("{}", row);
    }

    let joins = expl["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row.as_array().unwrap()[5].clone())
        .collect_vec();
    assert!(joins.contains(&json!(":friends:rev")));

    let res = db
        .run_script(
            "?[fr, data] := *friends{to: 2, fr, data}",
            Default::default(),
        )
        .unwrap();
    assert_eq!(res.into_json()["rows"], json!([[1, 5]]));
}

#[test]
fn test_multi_tx() {
    let db = DbInstance::new("mem", "", "").unwrap();
    let tx = db.multi_transaction(true);
    tx.run_script(":create a {a}", Default::default()).unwrap();
    tx.run_script("?[a] <- [[1]] :put a {a}", Default::default())
        .unwrap();
    assert!(tx.run_script(":create a {a}", Default::default()).is_err());
    tx.run_script("?[a] <- [[2]] :put a {a}", Default::default())
        .unwrap();
    tx.run_script("?[a] <- [[3]] :put a {a}", Default::default())
        .unwrap();
    tx.commit().unwrap();
    assert_eq!(
        db.run_script("?[a] := *a[a]", Default::default())
            .unwrap()
            .into_json()["rows"],
        json!([[1], [2], [3]])
    );

    let db = DbInstance::new("mem", "", "").unwrap();
    let tx = db.multi_transaction(true);
    tx.run_script(":create a {a}", Default::default()).unwrap();
    tx.run_script("?[a] <- [[1]] :put a {a}", Default::default())
        .unwrap();
    assert!(tx.run_script(":create a {a}", Default::default()).is_err());
    tx.run_script("?[a] <- [[2]] :put a {a}", Default::default())
        .unwrap();
    tx.run_script("?[a] <- [[3]] :put a {a}", Default::default())
        .unwrap();
    tx.abort().unwrap();
    assert!(db.run_script("?[a] := *a[a]", Default::default()).is_err());
}

#[test]
fn test_vec_types() {
    let db = DbInstance::new("mem", "", "").unwrap();
    db.run_script(":create a {k: String => v: <F32; 8>}", Default::default())
        .unwrap();
    db.run_script(
        "?[k, v] <- [['k', [1,2,3,4,5,6,7,8]]] :put a {k => v}",
        Default::default(),
    )
    .unwrap();
    let res = db
        .run_script("?[k, v] := *a{k, v}", Default::default())
        .unwrap();
    assert_eq!(
        json!([1., 2., 3., 4., 5., 6., 7., 8.]),
        res.into_json()["rows"][0][1]
    );
    let res = db
        .run_script("?[v] <- [[vec([1,2,3,4,5,6,7,8])]]", Default::default())
        .unwrap();
    assert_eq!(
        json!([1., 2., 3., 4., 5., 6., 7., 8.]),
        res.into_json()["rows"][0][0]
    );
    let res = db
        .run_script("?[v] <- [[rand_vec(5)]]", Default::default())
        .unwrap();
    assert_eq!(5, res.into_json()["rows"][0][0].as_array().unwrap().len());
    let res = db
        .run_script(r#"
            val[v] <- [[vec([1,2,3,4,5,6,7,8])]]
            ?[x,y,z] := val[v], x=l2_dist(v, v), y=cos_dist(v, v), nv = l2_normalize(v), z=ip_dist(nv, nv)
        "#, Default::default())
        .unwrap();
    println!("{}", res.into_json());
}

#[test]
fn test_vec_index() {
    let db = DbInstance::new("mem", "", "").unwrap();
    db.run_script(
        r"
        ?[k, v] <- [['a', [1,2,3,4,5,6,7,8]],
                    ['b', [2,3,4,5,6,7,8,9]],
                    ['bb', [2,3,4,5,6,7,8,9]],
                    ['c', [2,3,4,5,6,7,8,19]],
                    ['a', [2,3,4,5,6,7,8,9]],
                    ['b', [1,1,1,1,1,1,1,1]]]

        :create a {k: String => v: <F32; 8>}
    ",
        Default::default(),
    )
    .unwrap();
    db.run_script(
        r"
        ::hnsw create a:vec {
            dim: 8,
            m: 50,
            dtype: F32,
            fields: [v],
            distance: Cosine,
            ef_construction: 20,
            filter: k != 'k1'
        }",
        Default::default(),
    )
    .unwrap();
    db.run_script(
        r"
        ?[k, v] <- [
                    ['a2', [1,2,3,4,5,6,7,8]],
                    ['b2', [2,3,4,5,6,7,8,9]],
                    ['bb2', [2,3,4,5,6,7,8,9]],
                    ['c2', [2,3,4,5,6,7,8,19]],
                    ['a2', [2,3,4,5,6,7,8,9]],
                    ['b2', [1,1,1,1,1,1,1,1]]
                    ]
        :put a {k => v}
        ",
        Default::default(),
    )
    .unwrap();
    let res = db
        .run_script(
            r"
        #::explain {
        ?[k, dist, v] := ~a:vec{k, v | query: q, k: 10, ef: 20, bind_distance: dist}, q = vec([1,1,1,1,1,1,1,1])
        #}
        ",
            Default::default(),
        )
        .unwrap();
    println!("res: {:#?}", res.into_json()["rows"]);
    // println!("{:#?}", db.export_relations(["a", "a:vec"].iter()));
}
