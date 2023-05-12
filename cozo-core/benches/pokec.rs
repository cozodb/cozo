/*
 * Copyright 2022, The Cozo Project Authors.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file,
 * You can obtain one at https://mozilla.org/MPL/2.0/.
 */
#![feature(test)]

extern crate test;

use std::collections::BTreeMap;
use std::fs::File;
use std::io::BufRead;
use std::path::{Path, PathBuf};
use std::time::Instant;
use std::{env, io, mem};
use test::Bencher;

use lazy_static::{initialize, lazy_static};
use rand::Rng;
use rayon::prelude::*;
use regex::Regex;

use cozo::{DataValue, DbInstance, NamedRows};

lazy_static! {
    static ref ITERATIONS: usize = {
        let size = env::var("COZO_BENCH_ITERATIONS").unwrap_or("100".to_string());
        size.parse::<usize>().unwrap()
    };
    static ref SIZES: (usize, usize) = {
        let size = env::var("COZO_BENCH_POKEC_SIZE").unwrap_or("medium".to_string());
        match &size as &str {
            "small" => (10000, 121716),
            "medium" => (100000, 1768515),
            "large" => (1632803, 30622564),
            _ => panic!()
        }
    };

    static ref TEST_DB: DbInstance = {
        let data_dir = PathBuf::from(env::var("COZO_BENCH_POKEC_DIR").unwrap());
        let db_kind = env::var("COZO_TEST_DB_ENGINE").unwrap_or("mem".to_string());
        let mut db_path = data_dir.clone();
        let data_size = env::var("COZO_BENCH_POKEC_SIZE").unwrap_or("medium".to_string());
        let batch_size = env::var("COZO_BENCH_POKEC_BATCH")
            .unwrap()
            .parse::<usize>()
            .unwrap();
        db_path.push(format!("{}-{}.db", db_kind, data_size));
        // let _ = std::fs::remove_file(&db_path);
        // let _ = std::fs::remove_dir_all(&db_path);
        let path_exists = Path::exists(&db_path);
        let db = DbInstance::new(&db_kind, db_path.to_str().unwrap(), "").unwrap();
        if path_exists {
            db.run_script("::compact", Default::default()).unwrap();
            return db
        }

        let mut backup_path = data_dir.clone();
        backup_path.push(format!("backup-{}.db", data_size));
        if Path::exists(&backup_path) {
            println!("restore from backup");
            let import_time = Instant::now();
            db.restore_backup(backup_path.to_str().unwrap()).unwrap();
            dbg!(import_time.elapsed());
            dbg!(((SIZES.0 + 2 * SIZES.1) as f64) / import_time.elapsed().as_secs_f64());
        } else {
            println!("parse data from text file");
            let mut file_path = data_dir.clone();
            file_path.push(format!("pokec_{}_import.cypher", data_size));

            // dbg!(&db_kind);
            // dbg!(&data_dir);
            // dbg!(&file_path);
            // dbg!(&data_size);
            // dbg!(&n_threads);

            if db.run_script(
                r#"
            {:create user {uid: Int => cmpl_pct: Int, gender: String?, age: Int?}}
            {:create friends {fr: Int, to: Int}}
            {:create friends.rev {to: Int, fr: Int}}
            "#,
                Default::default(),
            ).is_err() {
                return db
            }

            let node_re = Regex::new(r#"CREATE \(:User \{id: (\d+), completion_percentage: (\d+), gender: "(\w+)", age: (\d+)}\);"#).unwrap();
            let node_partial_re =
                Regex::new(r#"CREATE \(:User \{id: (\d+), completion_percentage: (\d+)}\);"#).unwrap();
            let edge_re = Regex::new(r#"MATCH \(n:User \{id: (\d+)}\), \(m:User \{id: (\d+)}\) CREATE \(n\)-\[e: Friend]->\(m\);"#).unwrap();

            let file = File::open(&file_path).unwrap();
            let mut friends = Vec::with_capacity(batch_size);
            let mut users = Vec::with_capacity(batch_size);
            let mut push_to_users = |row: Option<Vec<DataValue>>, force: bool| {
                if let Some(row) = row {
                    users.push(row);
                }
                if users.len() >= batch_size || (force && !users.is_empty()) {
                    let mut new_rows = Vec::with_capacity(batch_size);
                    mem::swap(&mut new_rows, &mut users);
                    db.import_relations(BTreeMap::from([(
                        "user".to_string(),
                        NamedRows {
                            headers: vec![
                                "uid".to_string(),
                                "cmpl_pct".to_string(),
                                "gender".to_string(),
                                "age".to_string(),
                            ],
                            rows: new_rows,
                            next: None
                        },
                    )]))
                    .unwrap();
                }
            };

            let mut push_to_friends = |row: Option<Vec<DataValue>>, force: bool| {
                if let Some(row) = row {
                    friends.push(row);
                }
                if friends.len() >= batch_size || (force && !friends.is_empty()) {
                    let mut new_rows = Vec::with_capacity(batch_size);
                    mem::swap(&mut new_rows, &mut friends);
                    db.import_relations(BTreeMap::from([
                        (
                            "friends".to_string(),
                            NamedRows {
                                headers: vec!["fr".to_string(), "to".to_string()],
                                rows: new_rows.clone(),
                                next: None,
                            },
                        ),
                        (
                            "friends.rev".to_string(),
                            NamedRows {
                                headers: vec!["fr".to_string(), "to".to_string()],
                                rows: new_rows,
                                next: None,
                            },
                        ),
                    ]))
                    .unwrap();
                }
            };

            let import_time = Instant::now();
            let mut n_rows = 0usize;
            for line in io::BufReader::new(file).lines() {
                let line = line.unwrap();
                if let Some(data) = edge_re.captures(&line) {
                    n_rows += 2;
                    let fr = data.get(1).unwrap().as_str().parse::<i64>().unwrap();
                    let to = data.get(2).unwrap().as_str().parse::<i64>().unwrap();
                    push_to_friends(Some(vec![DataValue::from(fr), DataValue::from(to)]), false);
                    continue;
                }
                if let Some(data) = node_re.captures(&line) {
                    n_rows += 1;
                    let uid = data.get(1).unwrap().as_str().parse::<i64>().unwrap();
                    let cmpl_pct = data.get(2).unwrap().as_str().parse::<i64>().unwrap();
                    let gender = data.get(3).unwrap().as_str();
                    let age = data.get(4).unwrap().as_str().parse::<i64>().unwrap();
                    push_to_users(
                        Some(vec![DataValue::from(uid), DataValue::from(cmpl_pct), DataValue::from(gender), DataValue::from(age)]),
                        false,
                    );
                    continue;
                }
                if let Some(data) = node_partial_re.captures(&line) {
                    n_rows += 1;
                    let uid = data.get(1).unwrap().as_str().parse::<i64>().unwrap();
                    let cmpl_pct = data.get(2).unwrap().as_str().parse::<i64>().unwrap();
                    push_to_users(
                        Some(vec![
                            DataValue::from(uid),
                            DataValue::from(cmpl_pct),
                            DataValue::Null,
                            DataValue::Null,
                        ]),
                        false,
                    );
                    continue;
                }
                if line.len() < 3 {
                    continue;
                }
                panic!("Err: {}", line)
            }
            push_to_users(None, true);
            push_to_friends(None, true);
            dbg!(import_time.elapsed());
            dbg!((n_rows as f64) / import_time.elapsed().as_secs_f64());
        }
        db
    };
}

type QueryFn = fn() -> ();

const READ_QUERIES: [QueryFn; 1] = [single_vertex_read];
const WRITE_QUERIES: [QueryFn; 2] = [single_edge_write, single_vertex_write];
const UPDATE_QUERIES: [QueryFn; 1] = [single_vertex_update];
#[allow(dead_code)]
const AGGREGATE_QUERIES: [QueryFn; 4] = [
    aggregation_group,
    aggregation_filter,
    aggregation_count,
    aggregation_min_max,
];
const ANALYTICAL_QUERIES: [QueryFn; 15] = [
    expansion_1_plain,
    expansion_2_plain,
    expansion_3_plain,
    expansion_4_plain,
    expansion_1_filter,
    expansion_2_filter,
    expansion_3_filter,
    expansion_4_filter,
    neighbours_2_plain,
    neighbours_2_filter_only,
    neighbours_2_data_only,
    neighbours_2_filter_data,
    pattern_cycle,
    pattern_long,
    pattern_short,
];

fn single_vertex_read() {
    let i = rand::thread_rng().gen_range(1..SIZES.0);
    TEST_DB
        .run_script(
            "?[cmpl_pct, gender, age] := *user{uid: $id, cmpl_pct, gender, age}",
            BTreeMap::from([("id".to_string(), DataValue::from(i as i64))]),
        )
        .unwrap();
}

fn single_vertex_write() {
    let i = rand::thread_rng().gen_range(1..SIZES.0 * 10);
    for _ in 0..10 {
        if TEST_DB
            .run_script(
                "?[uid, cmpl_pct, gender, age] <- [[$id, 0, null, null]] :put user {uid => cmpl_pct, gender, age}",
                BTreeMap::from([("id".to_string(), DataValue::from(i as i64))]),
            )
            .is_ok() {
            return;
        }
    }
    panic!()
}

fn single_edge_write() {
    let i = rand::thread_rng().gen_range(1..SIZES.0);
    let mut j = rand::thread_rng().gen_range(1..SIZES.0);
    while j == i {
        j = rand::thread_rng().gen_range(1..SIZES.0);
    }
    for _ in 0..10 {
        if TEST_DB
            .run_script(
                r#"
            {?[fr, to] <- [[$i, $j]] :put friends {fr, to}}
            {?[fr, to] <- [[$i, $j]] :put friends.rev {fr, to}}
            "#,
                BTreeMap::from([("i".to_string(), DataValue::from(i as i64)), ("j".to_string(), DataValue::from(j as i64))]),
            )
            .is_ok()
        {
            return;
        }
    }
    panic!()
}

fn pagerank() {
    TEST_DB
        .run_script(
            r#"
            ?[] <~ PageRank(*friends[])
            "#,
            Default::default(),
        )
        .unwrap();
}

fn single_vertex_update() {
    let i = rand::thread_rng().gen_range(1..SIZES.0);
    for _ in 0..10 {
        if TEST_DB
            .run_script(
                r#"
            ?[uid, cmpl_pct, age, gender] := uid = $id, *user{uid, age, gender}, cmpl_pct = -1
            :put user {uid => cmpl_pct, age, gender}
            "#,
                BTreeMap::from([("id".to_string(), DataValue::from(i as i64))]),
            )
            .is_ok()
        {
            return;
        }
    }
    panic!()
}

fn aggregation_group() {
    TEST_DB
        .run_script("?[age, count(uid)] := *user{uid, age}", Default::default())
        .unwrap();
}

fn aggregation_count() {
    TEST_DB
        .run_script(
            "?[count(uid), count(age)] := *user{uid, age}",
            Default::default(),
        )
        .unwrap();
}

fn aggregation_filter() {
    TEST_DB
        .run_script(
            "?[age, count(age)] := *user{age}, age ~ 0 >= 18",
            Default::default(),
        )
        .unwrap();
}

fn aggregation_min_max() {
    TEST_DB
        .run_script(
            "?[min(uid), max(uid), mean(uid)] := *user{uid, age}",
            Default::default(),
        )
        .unwrap();
}

fn expansion_1_plain() {
    let mut rng = rand::thread_rng();
    let i = rng.gen_range(1..SIZES.0);
    TEST_DB
        .run_script(
            "?[to] := *friends{fr: $id, to}",
            BTreeMap::from([("id".to_string(), DataValue::from(i as i64))]),
        )
        .unwrap();
}

fn expansion_1_filter() {
    let mut rng = rand::thread_rng();
    let i = rng.gen_range(1..SIZES.0);
    TEST_DB
        .run_script(
            "?[to] := *friends{fr: $id, to}, *user{uid: to, age}, age ~ 0 >= 18",
            BTreeMap::from([("id".to_string(), DataValue::from(i as i64))]),
        )
        .unwrap();
}

fn expansion_2_plain() {
    let mut rng = rand::thread_rng();
    let i = rng.gen_range(1..SIZES.0);
    TEST_DB
        .run_script(
            "?[to] := *friends{fr: $id, to: a}, *friends{fr: a, to}",
            BTreeMap::from([("id".to_string(), DataValue::from(i as i64))]),
        )
        .unwrap();
}

fn expansion_2_filter() {
    let mut rng = rand::thread_rng();
    let i = rng.gen_range(1..SIZES.0);
    TEST_DB
        .run_script(
            "?[to] := *friends{fr: $id, to: a}, *friends{fr: a, to}, *user{uid: to, age}, age ~ 0 >= 18",
            BTreeMap::from([("id".to_string(), DataValue::from(i as i64))]),
        )
        .unwrap();
}

fn expansion_3_plain() {
    let mut rng = rand::thread_rng();
    let i = rng.gen_range(1..SIZES.0);
    TEST_DB
        .run_script(
            r#"
            l1[to] := *friends{fr: $id, to}
            l2[to] := l1[fr], *friends{fr, to}
            ?[to] := l2[fr], *friends{fr, to}
            "#,
            BTreeMap::from([("id".to_string(), DataValue::from(i as i64))]),
        )
        .unwrap();
}

fn expansion_3_filter() {
    let i = rand::thread_rng().gen_range(1..SIZES.0);
    TEST_DB
        .run_script(
            r#"
                        l1[to] := *friends{fr: $id, to}
                        l2[to] := l1[fr], *friends{fr, to}
                        ?[to] := l2[fr], *friends{fr, to}, *user{uid: to, age}, age ~ 0 >= 18
                        "#,
            BTreeMap::from([("id".to_string(), DataValue::from(i as i64))]),
        )
        .unwrap();
}

fn expansion_4_plain() {
    let i = rand::thread_rng().gen_range(1..SIZES.0);
    TEST_DB
        .run_script(
            r#"
                        l1[to] := *friends{fr: $id, to}
                        l2[to] := l1[fr], *friends{fr, to}
                        l3[to] := l2[fr], *friends{fr, to}
                        ?[to] := l3[fr], *friends{fr, to}
                        "#,
            BTreeMap::from([("id".to_string(), DataValue::from(i as i64))]),
        )
        .unwrap();
}

fn expansion_4_filter() {
    let i = rand::thread_rng().gen_range(1..SIZES.0);
    TEST_DB
        .run_script(
            r#"
                        l1[to] := *friends{fr: $id, to}
                        l2[to] := l1[fr], *friends{fr, to}
                        l3[to] := l2[fr], *friends{fr, to}
                        ?[to] := l3[fr], *friends{fr, to}
                        "#,
            BTreeMap::from([("id".to_string(), DataValue::from(i as i64))]),
        )
        .unwrap();
}

fn neighbours_2_plain() {
    let mut rng = rand::thread_rng();
    let i = rng.gen_range(1..SIZES.0);
    TEST_DB
        .run_script(
            r#"
            l1[to] := *friends{fr: $id, to}
            ?[to] := l1[to]
            ?[to] := l1[fr], *friends{fr, to}
            "#,
            BTreeMap::from([("id".to_string(), DataValue::from(i as i64))]),
        )
        .unwrap();
}

fn neighbours_2_filter_only() {
    let mut rng = rand::thread_rng();
    let i = rng.gen_range(1..SIZES.0);
    TEST_DB
        .run_script(
            r#"
            l1[to] := *friends{fr: $id, to}
            ?[to] := l1[to], *user{uid: to, age}, age ~ 0 >= 18
            ?[to] := l1[fr], *friends{fr, to}, *user{uid: to, age}, age ~ 0 >= 18
            "#,
            BTreeMap::from([("id".to_string(), DataValue::from(i as i64))]),
        )
        .unwrap();
}

fn neighbours_2_data_only() {
    let mut rng = rand::thread_rng();
    let i = rng.gen_range(1..SIZES.0);
    TEST_DB
        .run_script(
            r#"
            l1[to] := *friends{fr: $id, to}
            ?[to, age, cmpl_pct, gender] := l1[to], *user{uid: to, age, cmpl_pct, gender}
            ?[to, age, cmpl_pct, gender] := l1[fr], *friends{fr, to}, *user{uid: to, age, cmpl_pct, gender}
            "#,
            BTreeMap::from([("id".to_string(), DataValue::from(i as i64))]),
        )
        .unwrap();
}

fn neighbours_2_filter_data() {
    let mut rng = rand::thread_rng();
    let i = rng.gen_range(1..SIZES.0);
    TEST_DB
        .run_script(
            r#"
            l1[to] := *friends{fr: $id, to}
            ?[to] := l1[to], *user{uid: to, age, cmpl_pct, gender}, age ~ 0 >= 18
            ?[to] := l1[fr], *friends{fr, to}, *user{uid: to, age, cmpl_pct, gender}, age ~ 0 >= 18
            "#,
            BTreeMap::from([("id".to_string(), DataValue::from(i as i64))]),
        )
        .unwrap();
}

fn pattern_cycle() {
    let mut rng = rand::thread_rng();
    let i = rng.gen_range(1..SIZES.0);
    TEST_DB
        .run_script(
            r#"
            ?[n, m] := n = $id, *friends{fr: n, to: m}, *friends.rev{fr: m, to: n}
            "#,
            BTreeMap::from([("id".to_string(), DataValue::from(i as i64))]),
        )
        .unwrap();
}

fn pattern_long() {
    let mut rng = rand::thread_rng();
    let i = rng.gen_range(1..SIZES.0);
    TEST_DB
        .run_script(
            r#"
                ?[n] := *friends{fr: $id, to: n2},
                        *friends{fr: n2, to: n3},
                        *friends{fr: n3, to: n4},
                        *friends.rev{to: n4, fr: n}

                :limit 1
            "#,
            BTreeMap::from([("id".to_string(), DataValue::from(i as i64))]),
        )
        .unwrap();
}

fn pattern_short() {
    let mut rng = rand::thread_rng();
    let i = rng.gen_range(1..SIZES.0);
    TEST_DB
        .run_script(
            r#"
            ?[to] := *friends{fr: $id, to}

            :limit 1
            "#,
            BTreeMap::from([("id".to_string(), DataValue::from(i as i64))]),
        )
        .unwrap();
}

#[bench]
fn nothing(_: &mut Bencher) {
    initialize(&TEST_DB);
}

#[bench]
fn empty(_: &mut Bencher) {}

#[bench]
fn backup_db(_: &mut Bencher) {
    initialize(&TEST_DB);
    let data_size = env::var("COZO_BENCH_POKEC_SIZE").unwrap_or("medium".to_string());
    let backup_taken = Instant::now();
    TEST_DB
        .backup_db(format!("backup-{}.db", data_size))
        .unwrap();
    dbg!(backup_taken.elapsed());
    dbg!(((SIZES.0 + 2 * SIZES.1) as f64) / backup_taken.elapsed().as_secs_f64());
}

#[bench]
fn bench_aggregation_group(b: &mut Bencher) {
    initialize(&TEST_DB);
    b.iter(aggregation_group)
}

#[bench]
fn bench_aggregation_distinct(b: &mut Bencher) {
    initialize(&TEST_DB);
    b.iter(aggregation_count)
}

#[bench]
fn bench_aggregation_filter(b: &mut Bencher) {
    initialize(&TEST_DB);
    b.iter(aggregation_filter)
}

#[bench]
fn bench_aggregation_min_max(b: &mut Bencher) {
    initialize(&TEST_DB);
    b.iter(aggregation_min_max)
}

#[bench]
fn bench_expansion_1(b: &mut Bencher) {
    initialize(&TEST_DB);
    b.iter(expansion_1_plain)
}

#[bench]
fn bench_expansion_1_filter(b: &mut Bencher) {
    initialize(&TEST_DB);
    b.iter(expansion_1_filter)
}

#[bench]
fn bench_expansion_2(b: &mut Bencher) {
    initialize(&TEST_DB);
    b.iter(expansion_2_plain)
}

#[bench]
fn bench_expansion_2_filter(b: &mut Bencher) {
    initialize(&TEST_DB);
    b.iter(expansion_2_filter)
}

#[bench]
fn bench_expansion_3(b: &mut Bencher) {
    initialize(&TEST_DB);
    b.iter(expansion_3_plain)
}

#[bench]
fn bench_expansion_3_filter(b: &mut Bencher) {
    initialize(&TEST_DB);
    b.iter(expansion_3_filter)
}

#[bench]
fn bench_expansion_4(b: &mut Bencher) {
    initialize(&TEST_DB);
    b.iter(expansion_4_plain)
}

#[bench]
fn bench_expansion_4_filter(b: &mut Bencher) {
    initialize(&TEST_DB);
    b.iter(expansion_4_filter)
}

#[bench]
fn bench_neighbours_2(b: &mut Bencher) {
    initialize(&TEST_DB);
    b.iter(neighbours_2_plain)
}

#[bench]
fn bench_neighbours_2_filter(b: &mut Bencher) {
    initialize(&TEST_DB);
    b.iter(neighbours_2_filter_only)
}

#[bench]
fn bench_neighbours_2_data(b: &mut Bencher) {
    initialize(&TEST_DB);
    b.iter(neighbours_2_data_only)
}

#[bench]
fn bench_neighbours_2_filter_data(b: &mut Bencher) {
    initialize(&TEST_DB);
    b.iter(neighbours_2_filter_data)
}

#[bench]
fn bench_pattern_cycle(b: &mut Bencher) {
    initialize(&TEST_DB);
    b.iter(pattern_cycle)
}

#[bench]
fn bench_pattern_long(b: &mut Bencher) {
    initialize(&TEST_DB);
    b.iter(pattern_long)
}

#[bench]
fn bench_pattern_short(b: &mut Bencher) {
    initialize(&TEST_DB);
    b.iter(pattern_short)
}

#[bench]
fn bench_single_vertex(b: &mut Bencher) {
    initialize(&TEST_DB);
    b.iter(single_vertex_read)
}

#[bench]
fn qps_single_vertex_read(_b: &mut Bencher) {
    use rayon::prelude::*;

    initialize(&TEST_DB);
    let count = 1_000_000;
    let qps_single_vertex_read_time = Instant::now();
    (0..count).into_par_iter().for_each(|_| {
        single_vertex_read();
    });
    dbg!((count as f64) / qps_single_vertex_read_time.elapsed().as_secs_f64());
}

#[bench]
fn qps_single_vertex_write(_b: &mut Bencher) {
    use rayon::prelude::*;

    initialize(&TEST_DB);
    let count = 1_000_000;
    let qps_single_vertex_write_time = Instant::now();
    (0..count).into_par_iter().for_each(|_| {
        single_vertex_write();
    });
    dbg!((count as f64) / qps_single_vertex_write_time.elapsed().as_secs_f64());
}

#[bench]
fn bench_single_vertex_write(b: &mut Bencher) {
    initialize(&TEST_DB);
    b.iter(single_vertex_write)
}

#[bench]
fn bench_single_edge_write(b: &mut Bencher) {
    initialize(&TEST_DB);
    b.iter(single_edge_write)
}

#[bench]
fn bench_single_vertex_update(b: &mut Bencher) {
    initialize(&TEST_DB);
    b.iter(single_vertex_update)
}

#[bench]
fn tp_expansion_1_plain(_b: &mut Bencher) {
    initialize(&TEST_DB);
    let expansion_1_time = Instant::now();
    (0..*ITERATIONS).into_par_iter().for_each(|_| {
        expansion_1_plain();
    });
    dbg!((*ITERATIONS as f64) / expansion_1_time.elapsed().as_secs_f64());
}

#[bench]
fn tp_expansion_1_filter(_b: &mut Bencher) {
    initialize(&TEST_DB);
    let expansion_1_filter_time = Instant::now();
    (0..*ITERATIONS).into_par_iter().for_each(|_| {
        expansion_1_filter();
    });
    dbg!((*ITERATIONS as f64) / expansion_1_filter_time.elapsed().as_secs_f64());
}

#[bench]
fn tp_expansion_2_plain(_b: &mut Bencher) {
    initialize(&TEST_DB);
    let expansion_2_time = Instant::now();
    (0..*ITERATIONS).into_par_iter().for_each(|_| {
        expansion_2_plain();
    });
    dbg!((*ITERATIONS as f64) / expansion_2_time.elapsed().as_secs_f64());
}

#[bench]
fn tp_expansion_2_filter(_b: &mut Bencher) {
    initialize(&TEST_DB);
    let expansion_2_filter_time = Instant::now();
    (0..*ITERATIONS).into_par_iter().for_each(|_| {
        expansion_2_filter();
    });
    dbg!((*ITERATIONS as f64) / expansion_2_filter_time.elapsed().as_secs_f64());
}

#[bench]
fn tp_expansion_3_plain(_b: &mut Bencher) {
    initialize(&TEST_DB);
    let expansion_3_time = Instant::now();
    (0..*ITERATIONS).into_par_iter().for_each(|_| {
        expansion_3_plain();
    });
    dbg!((*ITERATIONS as f64) / expansion_3_time.elapsed().as_secs_f64());
}

#[bench]
fn tp_expansion_3_filter(_b: &mut Bencher) {
    initialize(&TEST_DB);
    let expansion_3_filter_time = Instant::now();
    (0..*ITERATIONS).into_par_iter().for_each(|_| {
        expansion_3_filter();
    });
    dbg!((*ITERATIONS as f64) / expansion_3_filter_time.elapsed().as_secs_f64());
}

#[bench]
fn tp_expansion_4_plain(_b: &mut Bencher) {
    initialize(&TEST_DB);
    let expansion_4_time = Instant::now();
    (0..*ITERATIONS).into_par_iter().for_each(|_| {
        expansion_4_plain();
    });
    dbg!((*ITERATIONS as f64) / expansion_4_time.elapsed().as_secs_f64());
}

#[bench]
fn tp_expansion_4_filter(_b: &mut Bencher) {
    initialize(&TEST_DB);
    let expansion_4_filter_time = Instant::now();
    (0..*ITERATIONS).into_par_iter().for_each(|_| {
        expansion_4_filter();
    });
    dbg!((*ITERATIONS as f64) / expansion_4_filter_time.elapsed().as_secs_f64());
}

#[bench]
fn tp_neighbours_2_plain(_b: &mut Bencher) {
    initialize(&TEST_DB);
    let neighbours_2_time = Instant::now();
    (0..*ITERATIONS).into_par_iter().for_each(|_| {
        neighbours_2_plain();
    });
    dbg!((*ITERATIONS as f64) / neighbours_2_time.elapsed().as_secs_f64());
}

#[bench]
fn tp_neighbours_2_filter_only(_b: &mut Bencher) {
    initialize(&TEST_DB);
    let neighbours_2_filter_time = Instant::now();
    (0..*ITERATIONS).into_par_iter().for_each(|_| {
        neighbours_2_filter_only();
    });
    dbg!((*ITERATIONS as f64) / neighbours_2_filter_time.elapsed().as_secs_f64());
}

#[bench]
fn tp_neighbours_2_data_only(_b: &mut Bencher) {
    initialize(&TEST_DB);
    let neighbours_2_data_time = Instant::now();
    (0..*ITERATIONS).into_par_iter().for_each(|_| {
        neighbours_2_data_only();
    });
    dbg!((*ITERATIONS as f64) / neighbours_2_data_time.elapsed().as_secs_f64());
}

#[bench]
fn tp_neighbours_2_filter_data(_b: &mut Bencher) {
    initialize(&TEST_DB);
    let neighbours_2_filter_data_time = Instant::now();
    (0..*ITERATIONS).into_par_iter().for_each(|_| {
        neighbours_2_filter_data();
    });
    dbg!((*ITERATIONS as f64) / neighbours_2_filter_data_time.elapsed().as_secs_f64());
}

#[bench]
fn tp_pattern_cycle(_b: &mut Bencher) {
    initialize(&TEST_DB);
    let pattern_cycle_time = Instant::now();
    (0..*ITERATIONS).into_par_iter().for_each(|_| {
        pattern_cycle();
    });
    dbg!((*ITERATIONS as f64) / pattern_cycle_time.elapsed().as_secs_f64());
}

#[bench]
fn tp_pattern_long(_b: &mut Bencher) {
    initialize(&TEST_DB);
    let pattern_long_time = Instant::now();
    (0..*ITERATIONS).into_par_iter().for_each(|_| {
        pattern_long();
    });
    dbg!((*ITERATIONS as f64) / pattern_long_time.elapsed().as_secs_f64());
}

#[bench]
fn tp_pattern_short(_b: &mut Bencher) {
    initialize(&TEST_DB);
    let pattern_short_time = Instant::now();
    (0..*ITERATIONS).into_par_iter().for_each(|_| {
        pattern_short();
    });
    dbg!((*ITERATIONS as f64) / pattern_short_time.elapsed().as_secs_f64());
}

#[bench]
fn tp_aggregation_group(_b: &mut Bencher) {
    initialize(&TEST_DB);
    let aggregation_group_time = Instant::now();
    (0..*ITERATIONS).into_par_iter().for_each(|_| {
        aggregation_group();
    });
    dbg!((*ITERATIONS as f64) / aggregation_group_time.elapsed().as_secs_f64());
}

#[bench]
fn tp_aggregation_count(_b: &mut Bencher) {
    initialize(&TEST_DB);
    let aggregation_count_time = Instant::now();
    (0..*ITERATIONS).into_par_iter().for_each(|_| {
        aggregation_count();
    });
    dbg!((*ITERATIONS as f64) / aggregation_count_time.elapsed().as_secs_f64());
}

#[bench]
fn tp_aggregation_filter(_b: &mut Bencher) {
    initialize(&TEST_DB);
    let aggregation_filter_time = Instant::now();
    (0..*ITERATIONS).into_par_iter().for_each(|_| {
        aggregation_filter();
    });
    dbg!((*ITERATIONS as f64) / aggregation_filter_time.elapsed().as_secs_f64());
}

#[bench]
fn tp_aggregation_min_max(_b: &mut Bencher) {
    initialize(&TEST_DB);
    let aggregation_min_max_time = Instant::now();
    (0..*ITERATIONS).into_par_iter().for_each(|_| {
        aggregation_min_max();
    });
    dbg!((*ITERATIONS as f64) / aggregation_min_max_time.elapsed().as_secs_f64());
}

#[bench]
fn tp_single_vertex_read(_b: &mut Bencher) {
    initialize(&TEST_DB);
    let single_vertex_read_time = Instant::now();
    (0..*ITERATIONS).into_par_iter().for_each(|_| {
        single_vertex_read();
    });
    dbg!((*ITERATIONS as f64) / single_vertex_read_time.elapsed().as_secs_f64());
}

#[bench]
fn tp_single_vertex_write(_b: &mut Bencher) {
    initialize(&TEST_DB);
    let single_vertex_write_time = Instant::now();
    (0..*ITERATIONS).into_par_iter().for_each(|_| {
        single_vertex_write();
    });
    dbg!((*ITERATIONS as f64) / single_vertex_write_time.elapsed().as_secs_f64());
}

#[bench]
fn tp_single_edge_write(_b: &mut Bencher) {
    initialize(&TEST_DB);
    let single_edge_write_time = Instant::now();
    (0..*ITERATIONS).into_par_iter().for_each(|_| {
        single_vertex_write();
    });
    dbg!((*ITERATIONS as f64) / single_edge_write_time.elapsed().as_secs_f64());
}

#[bench]
fn tp_single_vertex_update(_b: &mut Bencher) {
    initialize(&TEST_DB);
    let single_vertex_update_time = Instant::now();
    (0..*ITERATIONS).into_par_iter().for_each(|_| {
        single_vertex_update();
    });
    dbg!((*ITERATIONS as f64) / single_vertex_update_time.elapsed().as_secs_f64());
}

#[bench]
fn tp_pagerank(_b: &mut Bencher) {
    initialize(&TEST_DB);
    let pagerank_time = Instant::now();
    (0..*ITERATIONS).into_par_iter().for_each(|_| {
        pagerank();
    });
    dbg!((*ITERATIONS as f64) / pagerank_time.elapsed().as_secs_f64());
}

fn wrap(mixed_pct: f64, f: QueryFn) {
    use rand::prelude::*;

    let mut gen = rand::thread_rng();
    if gen.gen_bool(mixed_pct) {
        let wtr = WRITE_QUERIES.choose(&mut gen).unwrap();
        wtr();
    } else {
        f();
    }
}

#[bench]
fn realistic(_: &mut Bencher) {
    use rand::prelude::*;
    use rayon::prelude::*;

    println!("realistic benchmarks");
    dbg!(rayon::current_num_threads());
    let init_time = Instant::now();
    initialize(&TEST_DB);
    dbg!(init_time.elapsed());

    let percentages = [
        [0.0, 0.9, 0.05, 0.05],
        [0.0, 0.7, 0.15, 0.15],
        [0.0, 0.5, 0.25, 0.25],
        [0.0, 0.3, 0.35, 0.35],
    ];

    for [analytical, read, update, write] in percentages {
        dbg!((analytical, read, update, write));
        let taken = Instant::now();
        (0..*ITERATIONS).into_par_iter().for_each(|_| {
            let mut gen = thread_rng();
            let p = gen.gen::<f64>();
            let f = if p < analytical {
                ANALYTICAL_QUERIES.choose(&mut gen)
            } else if p < analytical + read {
                READ_QUERIES.choose(&mut gen)
            } else if p < analytical + read + update {
                UPDATE_QUERIES.choose(&mut gen)
            } else {
                WRITE_QUERIES.choose(&mut gen)
            };
            f.unwrap()()
        });
        dbg!((*ITERATIONS as f64) / taken.elapsed().as_secs_f64());
    }
}

#[bench]
fn mixed(_: &mut Bencher) {
    use rayon::prelude::*;

    println!("mixed benchmarks");
    dbg!(rayon::current_num_threads());
    let init_time = Instant::now();
    initialize(&TEST_DB);
    dbg!(init_time.elapsed());

    let mixed_pct = env::var("COZO_BENCH_POKEC_MIX_PCT").unwrap_or("0.3".to_string());
    let mixed_pct = mixed_pct.parse::<f64>().unwrap();
    dbg!(mixed_pct);
    assert!(mixed_pct >= 0.);
    assert!(mixed_pct <= 1.);

    let expansion_1_time = Instant::now();
    let count = 100;
    (0..count).into_par_iter().for_each(|_| {
        wrap(mixed_pct, expansion_1_plain);
    });
    dbg!((count as f64) / expansion_1_time.elapsed().as_secs_f64());

    let expansion_1_filter_time = Instant::now();
    (0..count).into_par_iter().for_each(|_| {
        wrap(mixed_pct, expansion_1_filter);
    });
    dbg!((count as f64) / expansion_1_filter_time.elapsed().as_secs_f64());

    let expansion_2_time = Instant::now();

    (0..count).into_par_iter().for_each(|_| {
        wrap(mixed_pct, expansion_2_plain);
    });
    dbg!((count as f64) / expansion_2_time.elapsed().as_secs_f64());

    let expansion_2_filter_time = Instant::now();
    (0..count).into_par_iter().for_each(|_| {
        wrap(mixed_pct, expansion_2_filter);
    });
    dbg!((count as f64) / expansion_2_filter_time.elapsed().as_secs_f64());

    let expansion_3_time = Instant::now();

    (0..count).into_par_iter().for_each(|_| {
        wrap(mixed_pct, expansion_3_plain);
    });
    dbg!((count as f64) / expansion_3_time.elapsed().as_secs_f64());

    let expansion_3_filter_time = Instant::now();
    (0..count).into_par_iter().for_each(|_| {
        wrap(mixed_pct, expansion_3_filter);
    });
    dbg!((count as f64) / expansion_3_filter_time.elapsed().as_secs_f64());

    let expansion_4_time = Instant::now();

    (0..count).into_par_iter().for_each(|_| {
        wrap(mixed_pct, expansion_4_plain);
    });
    dbg!((count as f64) / expansion_4_time.elapsed().as_secs_f64());

    let expansion_4_filter_time = Instant::now();
    (0..count).into_par_iter().for_each(|_| {
        wrap(mixed_pct, expansion_4_filter);
    });
    dbg!((count as f64) / expansion_4_filter_time.elapsed().as_secs_f64());

    let neighbours_2_time = Instant::now();
    (0..count).into_par_iter().for_each(|_| {
        wrap(mixed_pct, neighbours_2_plain);
    });
    dbg!((count as f64) / neighbours_2_time.elapsed().as_secs_f64());

    let neighbours_2_filter_time = Instant::now();
    (0..count).into_par_iter().for_each(|_| {
        wrap(mixed_pct, neighbours_2_filter_only);
    });
    dbg!((count as f64) / neighbours_2_filter_time.elapsed().as_secs_f64());

    let neighbours_2_data_time = Instant::now();
    (0..count).into_par_iter().for_each(|_| {
        wrap(mixed_pct, neighbours_2_data_only);
    });
    dbg!((count as f64) / neighbours_2_data_time.elapsed().as_secs_f64());

    let neighbours_2_filter_data_time = Instant::now();
    (0..count).into_par_iter().for_each(|_| {
        wrap(mixed_pct, neighbours_2_filter_data);
    });
    dbg!((count as f64) / neighbours_2_filter_data_time.elapsed().as_secs_f64());

    let pattern_cycle_time = Instant::now();
    (0..count).into_par_iter().for_each(|_| {
        wrap(mixed_pct, pattern_cycle);
    });
    dbg!((count as f64) / pattern_cycle_time.elapsed().as_secs_f64());

    let pattern_long_time = Instant::now();
    (0..count).into_par_iter().for_each(|_| {
        wrap(mixed_pct, pattern_long);
    });
    dbg!((count as f64) / pattern_long_time.elapsed().as_secs_f64());

    let pattern_short_time = Instant::now();
    (0..count).into_par_iter().for_each(|_| {
        wrap(mixed_pct, pattern_short);
    });
    dbg!((count as f64) / pattern_short_time.elapsed().as_secs_f64());

    let aggregation_time = Instant::now();

    (0..count).into_par_iter().for_each(|_| {
        wrap(mixed_pct, aggregation_group);
    });
    dbg!((count as f64) / aggregation_time.elapsed().as_secs_f64());

    let aggregation_distinct_time = Instant::now();

    (0..count).into_par_iter().for_each(|_| {
        wrap(mixed_pct, aggregation_count);
    });
    dbg!((count as f64) / aggregation_distinct_time.elapsed().as_secs_f64());

    let aggregation_filter_time = Instant::now();

    (0..count).into_par_iter().for_each(|_| {
        wrap(mixed_pct, aggregation_filter);
    });
    dbg!((count as f64) / aggregation_filter_time.elapsed().as_secs_f64());

    let min_max_time = Instant::now();

    (0..count).into_par_iter().for_each(|_| {
        wrap(mixed_pct, aggregation_min_max);
    });
    dbg!((count as f64) / min_max_time.elapsed().as_secs_f64());

    let single_vertex_time = Instant::now();
    (0..count).into_par_iter().for_each(|_| {
        wrap(mixed_pct, single_vertex_read);
    });
    dbg!((count as f64) / single_vertex_time.elapsed().as_secs_f64());

    let single_vertex_update_time = Instant::now();
    (0..count).into_par_iter().for_each(|_| {
        wrap(mixed_pct, single_vertex_update);
    });
    dbg!((count as f64) / single_vertex_update_time.elapsed().as_secs_f64());
}
