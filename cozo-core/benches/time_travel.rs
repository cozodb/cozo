/*
 *  Copyright 2022, The Cozo Project Authors.
 *
 *  This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 *  If a copy of the MPL was not distributed with this file,
 *  You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 */
#![feature(test)]

extern crate test;

use cozo::{DataValue, DbInstance, NamedRows, Validity};
use itertools::Itertools;
use lazy_static::{initialize, lazy_static};
use rand::Rng;
use rayon::prelude::*;
use std::cmp::max;
use std::collections::BTreeMap;
use std::time::Instant;
use test::Bencher;

fn insert_data(db: &DbInstance) {
    let insert_plain_time = Instant::now();
    let mut to_import = BTreeMap::new();
    to_import.insert(
        "plain".to_string(),
        NamedRows {
            headers: vec!["k".to_string(), "v".to_string()],
            rows: (0..10000).map(|i| vec![DataValue::from(i as i64), DataValue::from(i as i64)]).collect_vec(),
            next: None,
        },
    );
    db.import_relations(to_import).unwrap();
    dbg!(insert_plain_time.elapsed());

    let insert_tt1_time = Instant::now();
    let mut to_import = BTreeMap::new();
    to_import.insert(
        "tt1".to_string(),
        NamedRows {
            headers: vec!["k".to_string(), "vld".to_string(), "v".to_string()],
            rows: (0..10000)
                .map(|i| vec![
                    DataValue::from(i as i64),
                    DataValue::Validity(Validity::from((0, true))),
                    DataValue::from(i as i64),
                ])
                .collect_vec(),
            next: None,
        },
    );
    db.import_relations(to_import).unwrap();
    dbg!(insert_tt1_time.elapsed());

    let insert_tt10_time = Instant::now();
    let mut to_import = BTreeMap::new();
    to_import.insert(
        "tt10".to_string(),
        NamedRows {
            headers: vec!["k".to_string(), "vld".to_string(), "v".to_string()],
            rows: (0..10000)
                .flat_map(|i| (0..10).map(move |vld| vec![
                    DataValue::from(i as i64),
                    DataValue::Validity(Validity::from((vld, true))),
                    DataValue::from(i as i64),
                ]))
                .collect_vec(),
            next: None,
        },
    );
    db.import_relations(to_import).unwrap();
    dbg!(insert_tt10_time.elapsed());

    let insert_tt100_time = Instant::now();
    let mut to_import = BTreeMap::new();
    to_import.insert(
        "tt100".to_string(),
        NamedRows {
            headers: vec!["k".to_string(), "vld".to_string(), "v".to_string()],
            rows: (0..10000)
                .flat_map(|i| (0..100).map(move |vld| vec![
                    DataValue::from(i as i64),
                    DataValue::Validity(Validity::from((vld, true))),
                    DataValue::from(i as i64),
                ]))
                .collect_vec(),
            next: None,
        },
    );
    db.import_relations(to_import).unwrap();
    dbg!(insert_tt100_time.elapsed());

    let insert_tt1000_time = Instant::now();
    let mut to_import = BTreeMap::new();
    to_import.insert(
        "tt1000".to_string(),
        NamedRows {
            headers: vec!["k".to_string(), "vld".to_string(), "v".to_string()],
            rows: (0..10000)
                .flat_map(|i| {
                    (0..1000).map(move |vld| vec![
                        DataValue::from(i as i64),
                        DataValue::Validity((vld, true).into()),
                        DataValue::from(i as i64),
                    ])
                })
                .collect_vec(),
            next: None,
        },
    );
    db.import_relations(to_import).unwrap();
    dbg!(insert_tt1000_time.elapsed());
}

lazy_static! {
    static ref TEST_DB: DbInstance = {
        let db_path = "_time_travel_rocks.db";
        let db = DbInstance::new("rocksdb", db_path, "").unwrap();

        let create_res = db.run_script(
            r#"
        {:create plain {k: Int => v}}
        {:create tt1 {k: Int, vld: Validity => v}}
        {:create tt10 {k: Int, vld: Validity => v}}
        {:create tt100 {k: Int, vld: Validity => v}}
        {:create tt1000 {k: Int, vld: Validity => v}}
        "#,
            Default::default(),
        );

        if create_res.is_ok() {
            insert_data(&db);
        } else {
            println!("database already exists, skip import");
        }

        db
    };
}

fn single_plain_read() {
    let i = rand::thread_rng().gen_range(0..10000);
    TEST_DB
        .run_script(
            "?[v] := *plain{k: $id, v}",
            BTreeMap::from([("id".to_string(), DataValue::from(i as i64))]),
        )
        .unwrap();
}

fn plain_aggr() {
    TEST_DB
        .run_script(
            r#"
    ?[sum(v)] := *plain{v}
    "#,
            BTreeMap::default(),
        )
        .unwrap();
}

fn tt_stupid_aggr(k: usize) {
    TEST_DB
        .run_script(
            &format!(
                r#"
    r[k, smallest_by(pack)] := *tt{}{{k, vld, v}}, pack = [v, vld]
    ?[sum(v)] := r[k, v]
    "#,
                k
            ),
            BTreeMap::default(),
        )
        .unwrap();
}

fn tt_travel_aggr(k: usize) {
    TEST_DB
        .run_script(
            &format!(
                r#"
    ?[sum(v)] := *tt{}{{v @ "NOW"}}
    "#,
                k
            ),
            BTreeMap::default(),
        )
        .unwrap();
}

fn single_tt_read(k: usize) {
    let i = rand::thread_rng().gen_range(0..10000);
    TEST_DB
        .run_script(
            &format!(
                r#"
            ?[smallest_by(pack)] := *tt{}{{k: $id, vld, v}}, pack = [v, vld]
            "#,
                k
            ),
            BTreeMap::from([("id".to_string(), DataValue::from(i as i64))]),
        )
        .unwrap();
}

fn single_tt_travel_read(k: usize) {
    let i = rand::thread_rng().gen_range(0..10000);
    TEST_DB
        .run_script(
            &format!(
                r#"
            ?[v] := *tt{}{{k: $id, v @ "NOW"}}
            "#,
                k
            ),
            BTreeMap::from([("id".to_string(), DataValue::from(i as i64))]),
        )
        .unwrap();
}

#[bench]
fn time_travel_init(_: &mut Bencher) {
    initialize(&TEST_DB);

    let count = 100_000;
    let qps_single_plain_time = Instant::now();
    (0..count).into_par_iter().for_each(|_| {
        single_plain_read();
    });
    dbg!((count as f64) / qps_single_plain_time.elapsed().as_secs_f64());

    for k in [1, 10, 100, 1000] {
        let count = 100_000;
        let qps_single_tt_time = Instant::now();
        (0..count).into_par_iter().for_each(|_| {
            single_tt_read(k);
        });
        dbg!(k);
        dbg!((count as f64) / qps_single_tt_time.elapsed().as_secs_f64());
    }

    for k in [1, 10, 100, 1000] {
        let count = 100_000;
        let qps_single_tt_travel_time = Instant::now();
        (0..count).into_par_iter().for_each(|_| {
            single_tt_travel_read(k);
        });
        dbg!(k);
        dbg!((count as f64) / qps_single_tt_travel_time.elapsed().as_secs_f64());
    }

    let count = 100;

    let plain_aggr_time = Instant::now();
    (0..count).for_each(|_| {
        plain_aggr();
    });
    dbg!(plain_aggr_time.elapsed().as_secs_f64() * 1000. / (count as f64));

    for k in [1, 10, 100, 1000] {
        let count = max(1000 / k, 5);
        let tt_stupid_aggr_time = Instant::now();
        (0..count).for_each(|_| {
            tt_stupid_aggr(k);
        });
        dbg!(k);
        dbg!(tt_stupid_aggr_time.elapsed().as_secs_f64() * 1000. / (count as f64));

        let count = 20;
        let tt_travel_aggr_time = Instant::now();
        (0..count).for_each(|_| {
            tt_travel_aggr(k);
        });
        dbg!(k);
        dbg!(tt_travel_aggr_time.elapsed().as_secs_f64() * 1000. / (count as f64));
    }
}
