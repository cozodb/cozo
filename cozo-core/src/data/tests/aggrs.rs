/*
 * Copyright 2022, The Cozo Project Authors.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file,
 * You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use approx::AbsDiffEq;
use itertools::Itertools;

use crate::data::aggr::parse_aggr;
use crate::data::value::DataValue;

#[test]
fn test_and() {
    let mut aggr = parse_aggr("and").unwrap().clone();
    aggr.normal_init(&[]).unwrap();
    aggr.meet_init(&[]).unwrap();
    let mut and_aggr = aggr.normal_op.unwrap();
    assert_eq!(and_aggr.get().unwrap(), DataValue::from(true));

    and_aggr.set(&DataValue::from(true)).unwrap();
    and_aggr.set(&DataValue::from(true)).unwrap();

    assert_eq!(and_aggr.get().unwrap(), DataValue::from(true));
    and_aggr.set(&DataValue::from(false)).unwrap();

    assert_eq!(and_aggr.get().unwrap(), DataValue::from(false));

    let m_and_aggr = aggr.meet_op.unwrap();
    let mut v = DataValue::from(true);

    m_and_aggr.update(&mut v, &DataValue::from(true)).unwrap();
    assert_eq!(v, DataValue::from(true));

    m_and_aggr.update(&mut v, &DataValue::from(false)).unwrap();
    assert_eq!(v, DataValue::from(false));

    m_and_aggr.update(&mut v, &DataValue::from(true)).unwrap();
    assert_eq!(v, DataValue::from(false));
}

#[test]
fn test_or() {
    let mut aggr = parse_aggr("or").unwrap().clone();
    aggr.normal_init(&[]).unwrap();
    aggr.meet_init(&[]).unwrap();

    let mut or_aggr = aggr.normal_op.unwrap();
    assert_eq!(or_aggr.get().unwrap(), DataValue::from(false));

    or_aggr.set(&DataValue::from(false)).unwrap();
    or_aggr.set(&DataValue::from(false)).unwrap();

    assert_eq!(or_aggr.get().unwrap(), DataValue::from(false));
    or_aggr.set(&DataValue::from(true)).unwrap();

    assert_eq!(or_aggr.get().unwrap(), DataValue::from(true));

    let m_or_aggr = aggr.meet_op.unwrap();
    let mut v = DataValue::from(false);

    m_or_aggr.update(&mut v, &DataValue::from(false)).unwrap();
    assert_eq!(v, DataValue::from(false));

    m_or_aggr.update(&mut v, &DataValue::from(true)).unwrap();
    assert_eq!(v, DataValue::from(true));

    m_or_aggr.update(&mut v, &DataValue::from(false)).unwrap();
    assert_eq!(v, DataValue::from(true));
}

#[test]
fn test_unique() {
    let mut aggr = parse_aggr("unique").unwrap().clone();
    aggr.normal_init(&[]).unwrap();
    let mut unique_aggr = aggr.normal_op.unwrap();

    unique_aggr.set(&DataValue::from(true)).unwrap();
    unique_aggr.set(&DataValue::from(1)).unwrap();
    unique_aggr.set(&DataValue::from(2)).unwrap();
    unique_aggr.set(&DataValue::from(1)).unwrap();
    assert_eq!(
        unique_aggr.get().unwrap(),
        DataValue::List(vec![
            DataValue::from(true),
            DataValue::from(1),
            DataValue::from(2),
        ])
    );
}

#[test]
fn test_group_count() {
    let mut aggr = parse_aggr("group_count").unwrap().clone();
    aggr.normal_init(&[]).unwrap();

    let mut group_count_aggr = aggr.normal_op.unwrap();
    group_count_aggr.set(&DataValue::from(1.)).unwrap();
    group_count_aggr.set(&DataValue::from(2.)).unwrap();
    group_count_aggr.set(&DataValue::from(3.)).unwrap();
    group_count_aggr.set(&DataValue::from(3.)).unwrap();
    group_count_aggr.set(&DataValue::from(1.)).unwrap();
    group_count_aggr.set(&DataValue::from(3.)).unwrap();
    assert_eq!(
        group_count_aggr.get().unwrap(),
        DataValue::List(vec![
            DataValue::List(vec![DataValue::from(1.), DataValue::from(2)]),
            DataValue::List(vec![DataValue::from(2.), DataValue::from(1)]),
            DataValue::List(vec![DataValue::from(3.), DataValue::from(3)]),
        ])
    )
}

#[test]
fn test_union() {
    let mut aggr = parse_aggr("union").unwrap().clone();
    aggr.normal_init(&[]).unwrap();
    aggr.meet_init(&[]).unwrap();

    let mut union_aggr = aggr.normal_op.unwrap();
    union_aggr
        .set(&DataValue::List(
            [1, 3, 5, 2].into_iter().map(DataValue::from).collect_vec(),
        ))
        .unwrap();
    union_aggr
        .set(&DataValue::List(
            [10, 2, 4, 6].into_iter().map(DataValue::from).collect_vec(),
        ))
        .unwrap();
    assert_eq!(
        union_aggr.get().unwrap(),
        DataValue::List(
            [1, 2, 3, 4, 5, 6, 10]
                .into_iter()
                .map(DataValue::from)
                .collect_vec()
        )
    );
    let mut v = DataValue::List([1, 3, 5, 2].into_iter().map(DataValue::from).collect_vec());

    let m_aggr_union = aggr.meet_op.unwrap();
    m_aggr_union
        .update(
            &mut v,
            &DataValue::List([10, 2, 4, 6].into_iter().map(DataValue::from).collect_vec()),
        )
        .unwrap();
    assert_eq!(
        v,
        DataValue::Set(
            [1, 2, 3, 4, 5, 6, 10]
                .into_iter()
                .map(DataValue::from)
                .collect()
        )
    );
}

#[test]
fn test_intersection() {
    let mut aggr = parse_aggr("intersection").unwrap().clone();
    aggr.normal_init(&[]).unwrap();
    aggr.meet_init(&[]).unwrap();

    let mut intersection_aggr = aggr.normal_op.unwrap();
    intersection_aggr
        .set(&DataValue::List(
            [1, 3, 5, 2].into_iter().map(DataValue::from).collect_vec(),
        ))
        .unwrap();
    intersection_aggr
        .set(&DataValue::List(
            [10, 2, 4, 6].into_iter().map(DataValue::from).collect_vec(),
        ))
        .unwrap();
    assert_eq!(
        intersection_aggr.get().unwrap(),
        DataValue::List([2].into_iter().map(DataValue::from).collect_vec())
    );
    let mut v = DataValue::List([1, 3, 5, 2].into_iter().map(DataValue::from).collect_vec());

    let m_aggr_intersection = aggr.meet_op.unwrap();
    m_aggr_intersection
        .update(
            &mut v,
            &DataValue::List([10, 2, 4, 6].into_iter().map(DataValue::from).collect_vec()),
        )
        .unwrap();
    assert_eq!(
        v,
        DataValue::Set([2].into_iter().map(DataValue::from).collect())
    );
}

#[test]
fn test_count_unique() {
    let mut aggr = parse_aggr("count_unique").unwrap().clone();
    aggr.normal_init(&[]).unwrap();

    let mut count_unique_aggr = aggr.normal_op.unwrap();
    count_unique_aggr.set(&DataValue::from(1)).unwrap();
    count_unique_aggr.set(&DataValue::from(2)).unwrap();
    count_unique_aggr.set(&DataValue::from(3)).unwrap();
    count_unique_aggr.set(&DataValue::from(1)).unwrap();
    count_unique_aggr.set(&DataValue::from(2)).unwrap();
    count_unique_aggr.set(&DataValue::from(1)).unwrap();
    assert_eq!(count_unique_aggr.get().unwrap(), DataValue::from(3));
}

#[test]
fn test_collect() {
    let mut aggr = parse_aggr("collect").unwrap().clone();
    aggr.normal_init(&[]).unwrap();

    let mut collect_aggr = aggr.normal_op.unwrap();
    collect_aggr.set(&DataValue::from(1)).unwrap();
    collect_aggr.set(&DataValue::from(2)).unwrap();
    collect_aggr.set(&DataValue::from(3)).unwrap();
    collect_aggr.set(&DataValue::from(1)).unwrap();
    collect_aggr.set(&DataValue::from(2)).unwrap();
    collect_aggr.set(&DataValue::from(1)).unwrap();
    assert_eq!(
        collect_aggr.get().unwrap(),
        DataValue::List(
            [1, 2, 3, 1, 2, 1]
                .into_iter()
                .map(DataValue::from)
                .collect()
        )
    );
}

#[test]
fn test_count() {
    let mut aggr = parse_aggr("count").unwrap().clone();
    aggr.normal_init(&[]).unwrap();

    let mut count_aggr = aggr.normal_op.unwrap();
    count_aggr.set(&DataValue::Null).unwrap();
    count_aggr.set(&DataValue::Null).unwrap();
    count_aggr.set(&DataValue::Null).unwrap();
    count_aggr.set(&DataValue::Null).unwrap();
    count_aggr.set(&DataValue::from(true)).unwrap();
    count_aggr.set(&DataValue::from(true)).unwrap();
    assert_eq!(count_aggr.get().unwrap(), DataValue::from(6));
}

#[test]
fn test_variance() {
    let mut aggr = parse_aggr("variance").unwrap().clone();
    aggr.normal_init(&[]).unwrap();

    let mut variance_aggr = aggr.normal_op.unwrap();
    variance_aggr.set(&DataValue::from(1)).unwrap();
    variance_aggr.set(&DataValue::from(2)).unwrap();
    assert_eq!(variance_aggr.get().unwrap(), DataValue::from(0.5))
}

#[test]
fn test_std_dev() {
    let mut aggr = parse_aggr("std_dev").unwrap().clone();
    aggr.normal_init(&[]).unwrap();

    let mut std_dev_aggr = aggr.normal_op.unwrap();
    std_dev_aggr.set(&DataValue::from(1)).unwrap();
    std_dev_aggr.set(&DataValue::from(2)).unwrap();
    let v = std_dev_aggr.get().unwrap().get_float().unwrap();
    assert!(v.abs_diff_eq(&(0.5_f64).sqrt(), 1e-10));
}

#[test]
fn test_mean() {
    let mut aggr = parse_aggr("mean").unwrap().clone();
    aggr.normal_init(&[]).unwrap();

    let mut mean_aggr = aggr.normal_op.unwrap();
    mean_aggr.set(&DataValue::from(1)).unwrap();
    mean_aggr.set(&DataValue::from(2)).unwrap();
    mean_aggr.set(&DataValue::from(3)).unwrap();
    mean_aggr.set(&DataValue::from(4)).unwrap();
    mean_aggr.set(&DataValue::from(5)).unwrap();
    assert_eq!(mean_aggr.get().unwrap(), DataValue::from(3.));
}

#[test]
fn test_sum() {
    let mut aggr = parse_aggr("sum").unwrap().clone();
    aggr.normal_init(&[]).unwrap();

    let mut sum_aggr = aggr.normal_op.unwrap();
    sum_aggr.set(&DataValue::from(1)).unwrap();
    sum_aggr.set(&DataValue::from(2)).unwrap();
    sum_aggr.set(&DataValue::from(3)).unwrap();
    sum_aggr.set(&DataValue::from(4)).unwrap();
    sum_aggr.set(&DataValue::from(5)).unwrap();
    assert_eq!(sum_aggr.get().unwrap(), DataValue::from(15.));
}

#[test]
fn test_product() {
    let mut aggr = parse_aggr("product").unwrap().clone();
    aggr.normal_init(&[]).unwrap();

    let mut product_aggr = aggr.normal_op.unwrap();
    product_aggr.set(&DataValue::from(1)).unwrap();
    product_aggr.set(&DataValue::from(2)).unwrap();
    product_aggr.set(&DataValue::from(3)).unwrap();
    product_aggr.set(&DataValue::from(4)).unwrap();
    product_aggr.set(&DataValue::from(5)).unwrap();
    assert_eq!(product_aggr.get().unwrap(), DataValue::from(120.));
}

#[test]
fn test_min() {
    let mut aggr = parse_aggr("min").unwrap().clone();
    aggr.normal_init(&[]).unwrap();
    aggr.meet_init(&[]).unwrap();

    let mut min_aggr = aggr.normal_op.unwrap();
    min_aggr.set(&DataValue::from(10)).unwrap();
    min_aggr.set(&DataValue::from(9)).unwrap();
    min_aggr.set(&DataValue::from(1)).unwrap();
    min_aggr.set(&DataValue::from(2)).unwrap();
    min_aggr.set(&DataValue::from(3)).unwrap();
    assert_eq!(min_aggr.get().unwrap(), DataValue::from(1));

    let m_min_aggr = aggr.meet_op.unwrap();
    let mut v = DataValue::from(5);
    m_min_aggr.update(&mut v, &DataValue::from(10)).unwrap();
    m_min_aggr.update(&mut v, &DataValue::from(9)).unwrap();
    m_min_aggr.update(&mut v, &DataValue::from(1)).unwrap();
    m_min_aggr.update(&mut v, &DataValue::from(2)).unwrap();
    m_min_aggr.update(&mut v, &DataValue::from(3)).unwrap();
    assert_eq!(v, DataValue::from(1));
}

#[test]
fn test_max() {
    let mut aggr = parse_aggr("max").unwrap().clone();
    aggr.normal_init(&[]).unwrap();
    aggr.meet_init(&[]).unwrap();

    let mut max_aggr = aggr.normal_op.unwrap();
    max_aggr.set(&DataValue::from(10)).unwrap();
    max_aggr.set(&DataValue::from(9)).unwrap();
    max_aggr.set(&DataValue::from(1)).unwrap();
    max_aggr.set(&DataValue::from(2)).unwrap();
    max_aggr.set(&DataValue::from(3)).unwrap();
    assert_eq!(max_aggr.get().unwrap(), DataValue::from(10));

    let m_max_aggr = aggr.meet_op.unwrap();
    let mut v = DataValue::from(5);
    m_max_aggr.update(&mut v, &DataValue::from(10)).unwrap();
    m_max_aggr.update(&mut v, &DataValue::from(9)).unwrap();
    m_max_aggr.update(&mut v, &DataValue::from(1)).unwrap();
    m_max_aggr.update(&mut v, &DataValue::from(2)).unwrap();
    m_max_aggr.update(&mut v, &DataValue::from(3)).unwrap();
    assert_eq!(v, DataValue::from(10));
}

#[test]
fn test_choice_rand() {
    let mut aggr = parse_aggr("choice_rand").unwrap().clone();
    aggr.normal_init(&[]).unwrap();

    let mut choice_aggr = aggr.normal_op.unwrap();
    choice_aggr.set(&DataValue::from(1)).unwrap();
    choice_aggr.set(&DataValue::from(2)).unwrap();
    choice_aggr.set(&DataValue::from(3)).unwrap();
    let v = choice_aggr.get().unwrap().get_int().unwrap();
    assert!(v == 1 || v == 2 || v == 3);
}

#[test]
fn test_min_cost() {
    let mut aggr = parse_aggr("min_cost").unwrap().clone();
    aggr.normal_init(&[]).unwrap();
    aggr.meet_init(&[]).unwrap();

    let mut min_cost_aggr = aggr.normal_op.unwrap();
    min_cost_aggr
        .set(&DataValue::List(vec![DataValue::Null, DataValue::from(3)]))
        .unwrap();
    min_cost_aggr
        .set(&DataValue::List(vec![
            DataValue::from(true),
            DataValue::from(1),
        ]))
        .unwrap();
    min_cost_aggr
        .set(&DataValue::List(vec![
            DataValue::from(false),
            DataValue::from(2),
        ]))
        .unwrap();
    assert_eq!(
        min_cost_aggr.get().unwrap(),
        DataValue::List(vec![DataValue::from(true), DataValue::from(1.)])
    );

    let m_min_cost_aggr = aggr.meet_op.unwrap();
    let mut v = DataValue::List(vec![DataValue::Null, DataValue::from(3)]);
    m_min_cost_aggr
        .update(
            &mut v,
            &DataValue::List(vec![DataValue::from(true), DataValue::from(1)]),
        )
        .unwrap();
    m_min_cost_aggr
        .update(
            &mut v,
            &DataValue::List(vec![DataValue::from(false), DataValue::from(2)]),
        )
        .unwrap();
    assert_eq!(
        v,
        DataValue::List(vec![DataValue::from(true), DataValue::from(1)])
    );
}

#[test]
fn test_latest_by() {
    let mut aggr = parse_aggr("latest_by").unwrap().clone();
    aggr.normal_init(&[]).unwrap();

    let mut latest_by_aggr = aggr.normal_op.unwrap();
    latest_by_aggr
        .set(&DataValue::List(vec![DataValue::Null, DataValue::from(3)]))
        .unwrap();
    latest_by_aggr
        .set(&DataValue::List(vec![
            DataValue::from(true),
            DataValue::from(1),
        ]))
        .unwrap();
    latest_by_aggr
        .set(&DataValue::List(vec![
            DataValue::from(false),
            DataValue::from(2),
        ]))
        .unwrap();
    assert_eq!(latest_by_aggr.get().unwrap(), DataValue::Null);
}

#[test]
fn test_shortest() {
    let mut aggr = parse_aggr("shortest").unwrap().clone();
    aggr.normal_init(&[]).unwrap();
    aggr.meet_init(&[]).unwrap();

    let mut shortest_aggr = aggr.normal_op.unwrap();
    shortest_aggr
        .set(&DataValue::List(
            [1, 2, 3].into_iter().map(DataValue::from).collect(),
        ))
        .unwrap();
    shortest_aggr
        .set(&DataValue::List(
            [2].into_iter().map(DataValue::from).collect(),
        ))
        .unwrap();
    shortest_aggr
        .set(&DataValue::List(
            [2, 3].into_iter().map(DataValue::from).collect(),
        ))
        .unwrap();
    assert_eq!(
        shortest_aggr.get().unwrap(),
        DataValue::List([2].into_iter().map(DataValue::from).collect())
    );

    let m_shortest_aggr = aggr.meet_op.unwrap();
    let mut v = DataValue::List([1, 2, 3].into_iter().map(DataValue::from).collect());
    m_shortest_aggr
        .update(
            &mut v,
            &DataValue::List([2].into_iter().map(DataValue::from).collect()),
        )
        .unwrap();
    m_shortest_aggr
        .update(
            &mut v,
            &DataValue::List([2, 3].into_iter().map(DataValue::from).collect()),
        )
        .unwrap();
    assert_eq!(
        v,
        DataValue::List([2].into_iter().map(DataValue::from).collect())
    );
}

#[test]
fn test_choice() {
    let mut aggr = parse_aggr("choice").unwrap().clone();
    aggr.normal_init(&[]).unwrap();
    aggr.meet_init(&[]).unwrap();

    let mut choice_aggr = aggr.normal_op.unwrap();
    choice_aggr.set(&DataValue::Null).unwrap();
    choice_aggr.set(&DataValue::from(1)).unwrap();
    choice_aggr.set(&DataValue::from(2)).unwrap();
    assert_eq!(choice_aggr.get().unwrap(), DataValue::from(1));

    let m_coalesce_aggr = aggr.meet_op.unwrap();
    let mut v = DataValue::Null;
    m_coalesce_aggr
        .update(
            &mut v,
            &DataValue::List([2].into_iter().map(DataValue::from).collect()),
        )
        .unwrap();
    m_coalesce_aggr
        .update(
            &mut v,
            &DataValue::List([2, 3].into_iter().map(DataValue::from).collect()),
        )
        .unwrap();
    assert_eq!(
        v,
        DataValue::List([2].into_iter().map(DataValue::from).collect())
    );
}

#[test]
fn test_bit_and() {
    let mut aggr = parse_aggr("bit_and").unwrap().clone();
    aggr.normal_init(&[]).unwrap();
    aggr.meet_init(&[]).unwrap();

    let mut bit_and_aggr = aggr.normal_op.unwrap();
    bit_and_aggr.set(&DataValue::Bytes(vec![0b11100])).unwrap();
    bit_and_aggr.set(&DataValue::Bytes(vec![0b01011])).unwrap();
    assert_eq!(bit_and_aggr.get().unwrap(), DataValue::Bytes(vec![0b01000]));

    let m_bit_and_aggr = aggr.meet_op.unwrap();
    let mut v = DataValue::Bytes(vec![0b11100]);
    m_bit_and_aggr
        .update(&mut v, &DataValue::Bytes(vec![0b01011]))
        .unwrap();
    assert_eq!(v, DataValue::Bytes(vec![0b01000]));
}

#[test]
fn test_bit_or() {
    let mut aggr = parse_aggr("bit_or").unwrap().clone();
    aggr.normal_init(&[]).unwrap();
    aggr.meet_init(&[]).unwrap();

    let mut bit_or_aggr = aggr.normal_op.unwrap();
    bit_or_aggr.set(&DataValue::Bytes(vec![0b11100])).unwrap();
    bit_or_aggr.set(&DataValue::Bytes(vec![0b01011])).unwrap();
    assert_eq!(bit_or_aggr.get().unwrap(), DataValue::Bytes(vec![0b11111]));

    let m_bit_or_aggr = aggr.meet_op.unwrap();
    let mut v = DataValue::Bytes(vec![0b11100]);
    m_bit_or_aggr
        .update(&mut v, &DataValue::Bytes(vec![0b01011]))
        .unwrap();
    assert_eq!(v, DataValue::Bytes(vec![0b11111]));
}

#[test]
fn test_bit_xor() {
    let mut aggr = parse_aggr("bit_xor").unwrap().clone();
    aggr.normal_init(&[]).unwrap();

    let mut bit_xor_aggr = aggr.normal_op.unwrap();
    bit_xor_aggr.set(&DataValue::Bytes(vec![0b11100])).unwrap();
    bit_xor_aggr.set(&DataValue::Bytes(vec![0b01011])).unwrap();
    assert_eq!(bit_xor_aggr.get().unwrap(), DataValue::Bytes(vec![0b10111]));
}
