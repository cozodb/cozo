use itertools::Itertools;

use crate::data::aggr::{
    AggrAnd, AggrGroupCount, AggrIntersection, AggrOr, AggrUnion, AggrUnique, MeetAggrAnd,
    MeetAggrIntersection, MeetAggrObj, MeetAggrOr, MeetAggrUnion, NormalAggrObj,
};
use crate::data::value::DataValue;

#[test]
fn test_and() {
    let mut and_aggr = AggrAnd::default();
    assert_eq!(and_aggr.get().unwrap(), DataValue::Bool(true));

    and_aggr.set(&DataValue::Bool(true)).unwrap();
    and_aggr.set(&DataValue::Bool(true)).unwrap();

    assert_eq!(and_aggr.get().unwrap(), DataValue::Bool(true));
    and_aggr.set(&DataValue::Bool(false)).unwrap();

    assert_eq!(and_aggr.get().unwrap(), DataValue::Bool(false));

    let m_and_aggr = MeetAggrAnd;
    let mut v = DataValue::Bool(true);

    m_and_aggr.update(&mut v, &DataValue::Bool(true)).unwrap();
    assert_eq!(v, DataValue::Bool(true));

    m_and_aggr.update(&mut v, &DataValue::Bool(false)).unwrap();
    assert_eq!(v, DataValue::Bool(false));

    m_and_aggr.update(&mut v, &DataValue::Bool(true)).unwrap();
    assert_eq!(v, DataValue::Bool(false));
}

#[test]
fn test_or() {
    let mut or_aggr = AggrOr::default();
    assert_eq!(or_aggr.get().unwrap(), DataValue::Bool(false));

    or_aggr.set(&DataValue::Bool(false)).unwrap();
    or_aggr.set(&DataValue::Bool(false)).unwrap();

    assert_eq!(or_aggr.get().unwrap(), DataValue::Bool(false));
    or_aggr.set(&DataValue::Bool(true)).unwrap();

    assert_eq!(or_aggr.get().unwrap(), DataValue::Bool(true));

    let m_or_aggr = MeetAggrOr;
    let mut v = DataValue::Bool(false);

    m_or_aggr.update(&mut v, &DataValue::Bool(false)).unwrap();
    assert_eq!(v, DataValue::Bool(false));

    m_or_aggr.update(&mut v, &DataValue::Bool(true)).unwrap();
    assert_eq!(v, DataValue::Bool(true));

    m_or_aggr.update(&mut v, &DataValue::Bool(false)).unwrap();
    assert_eq!(v, DataValue::Bool(true));
}

#[test]
fn test_unique() {
    let mut unique_aggr = AggrUnique::default();
    unique_aggr.set(&DataValue::Bool(true)).unwrap();
    unique_aggr.set(&DataValue::from(1)).unwrap();
    unique_aggr.set(&DataValue::from(2)).unwrap();
    unique_aggr.set(&DataValue::from(1)).unwrap();
    assert_eq!(
        unique_aggr.get().unwrap(),
        DataValue::List(vec![
            DataValue::Bool(true),
            DataValue::from(1),
            DataValue::from(2)
        ])
    );
}

#[test]
fn test_group_count() {
    let mut group_count_aggr = AggrGroupCount::default();
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
            DataValue::List(vec![DataValue::from(3.), DataValue::from(3)])
        ])
    )
}

#[test]
fn test_union() {
    let mut union_aggr = AggrUnion::default();
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
    let m_aggr_union = MeetAggrUnion;
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
    let mut intersection_aggr = AggrIntersection::default();
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
    let m_aggr_intersection = MeetAggrIntersection;
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
