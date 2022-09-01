use crate::data::functions::*;
use crate::data::value::DataValue;

#[test]
fn test_add() {
    assert_eq!(op_add(&[]).unwrap(), DataValue::from(0));
    assert_eq!(op_add(&[DataValue::from(1)]).unwrap(), DataValue::from(1));
    assert_eq!(
        op_add(&[DataValue::from(1), DataValue::from(2)]).unwrap(),
        DataValue::from(3)
    );
    assert_eq!(
        op_add(&[DataValue::from(1), DataValue::from(2.5)]).unwrap(),
        DataValue::from(3.5)
    );
    assert_eq!(
        op_add(&[DataValue::from(1.5), DataValue::from(2.5)]).unwrap(),
        DataValue::from(4.0)
    );
}

#[test]
fn test_sub() {
    assert_eq!(
        op_sub(&[DataValue::from(1), DataValue::from(2)]).unwrap(),
        DataValue::from(-1)
    );
    assert_eq!(
        op_sub(&[DataValue::from(1), DataValue::from(2.5)]).unwrap(),
        DataValue::from(-1.5)
    );
    assert_eq!(
        op_sub(&[DataValue::from(1.5), DataValue::from(2.5)]).unwrap(),
        DataValue::from(-1.0)
    );
}

#[test]
fn test_mul() {
    assert_eq!(op_mul(&[]).unwrap(), DataValue::from(1));
    assert_eq!(
        op_mul(&[DataValue::from(2), DataValue::from(3)]).unwrap(),
        DataValue::from(6)
    );
    assert_eq!(
        op_mul(&[DataValue::from(0.5), DataValue::from(0.25)]).unwrap(),
        DataValue::from(0.125)
    );
    assert_eq!(
        op_mul(&[DataValue::from(0.5), DataValue::from(3)]).unwrap(),
        DataValue::from(1.5)
    );
}

#[test]
fn test_div() {
    assert_eq!(
        op_div(&[DataValue::from(1), DataValue::from(1)]).unwrap(),
        DataValue::from(1.0)
    );
    assert_eq!(
        op_div(&[DataValue::from(1), DataValue::from(2)]).unwrap(),
        DataValue::from(0.5)
    );
    assert_eq!(
        op_div(&[DataValue::from(0), DataValue::from(0)]).unwrap(),
        DataValue::from(f64::NAN)
    );
    assert_eq!(
        op_div(&[DataValue::from(7.0), DataValue::from(0.5)]).unwrap(),
        DataValue::from(14.0)
    );
}
