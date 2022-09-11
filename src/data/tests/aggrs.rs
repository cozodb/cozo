use crate::data::aggr::{
    AggrAnd, AggrGroupCount, AggrOr, AggrUnique, MeetAggrAnd, MeetAggrObj, MeetAggrOr,
    NormalAggrObj,
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
