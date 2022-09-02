use approx::AbsDiffEq;
use num_traits::FloatConst;
use regex::Regex;

use crate::data::functions::*;
use crate::data::value::{DataValue, RegexWrapper};

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
        op_div(&[DataValue::from(7.0), DataValue::from(0.5)]).unwrap(),
        DataValue::from(14.0)
    );
}

#[test]
fn test_eq_neq() {
    assert_eq!(
        op_eq(&[DataValue::from(1), DataValue::from(1.0)]).unwrap(),
        DataValue::Bool(true)
    );
    assert_eq!(
        op_eq(&[DataValue::from(123), DataValue::from(123)]).unwrap(),
        DataValue::Bool(true)
    );
    assert_eq!(
        op_neq(&[DataValue::from(1), DataValue::from(1.0)]).unwrap(),
        DataValue::Bool(false)
    );
    assert_eq!(
        op_neq(&[DataValue::from(123), DataValue::from(123.0)]).unwrap(),
        DataValue::Bool(false)
    );
    assert_eq!(
        op_eq(&[DataValue::from(123), DataValue::from(123.1)]).unwrap(),
        DataValue::Bool(false)
    );
}

#[test]
fn test_list() {
    assert_eq!(op_list(&[]).unwrap(), DataValue::List(vec![]));
    assert_eq!(
        op_list(&[DataValue::from(1)]).unwrap(),
        DataValue::List(vec![DataValue::from(1)])
    );
    assert_eq!(
        op_list(&[DataValue::from(1), DataValue::List(vec![])]).unwrap(),
        DataValue::List(vec![DataValue::from(1), DataValue::List(vec![])])
    );
}

#[test]
fn test_is_in() {
    assert_eq!(
        op_is_in(&[
            DataValue::from(1),
            DataValue::List(vec![DataValue::from(1), DataValue::from(2)])
        ])
        .unwrap(),
        DataValue::Bool(true)
    );
    assert_eq!(
        op_is_in(&[
            DataValue::from(3),
            DataValue::List(vec![DataValue::from(1), DataValue::from(2)])
        ])
        .unwrap(),
        DataValue::Bool(false)
    );
    assert_eq!(
        op_is_in(&[DataValue::from(3), DataValue::List(vec![])]).unwrap(),
        DataValue::Bool(false)
    );
}

#[test]
fn test_comparators() {
    assert_eq!(
        op_ge(&[DataValue::from(2), DataValue::from(1)]).unwrap(),
        DataValue::Bool(true)
    );
    assert_eq!(
        op_ge(&[DataValue::from(1), DataValue::from(1)]).unwrap(),
        DataValue::Bool(true)
    );
    assert_eq!(
        op_ge(&[DataValue::from(1), DataValue::from(1.0)]).unwrap(),
        DataValue::Bool(true)
    );
    assert_eq!(
        op_ge(&[DataValue::from(1), DataValue::from(2)]).unwrap(),
        DataValue::Bool(false)
    );
    assert!(op_ge(&[DataValue::Null, DataValue::Bool(true)]).is_err());
    assert_eq!(
        op_gt(&[DataValue::from(2), DataValue::from(1)]).unwrap(),
        DataValue::Bool(true)
    );
    assert_eq!(
        op_gt(&[DataValue::from(1), DataValue::from(1)]).unwrap(),
        DataValue::Bool(false)
    );
    assert_eq!(
        op_gt(&[DataValue::from(1), DataValue::from(1.0)]).unwrap(),
        DataValue::Bool(false)
    );
    assert_eq!(
        op_gt(&[DataValue::from(1), DataValue::from(2)]).unwrap(),
        DataValue::Bool(false)
    );
    assert!(op_gt(&[DataValue::Null, DataValue::Bool(true)]).is_err());
    assert_eq!(
        op_le(&[DataValue::from(2), DataValue::from(1)]).unwrap(),
        DataValue::Bool(false)
    );
    assert_eq!(
        op_le(&[DataValue::from(1), DataValue::from(1)]).unwrap(),
        DataValue::Bool(true)
    );
    assert_eq!(
        op_le(&[DataValue::from(1), DataValue::from(1.0)]).unwrap(),
        DataValue::Bool(true)
    );
    assert_eq!(
        op_le(&[DataValue::from(1), DataValue::from(2)]).unwrap(),
        DataValue::Bool(true)
    );
    assert!(op_le(&[DataValue::Null, DataValue::Bool(true)]).is_err());
    assert_eq!(
        op_lt(&[DataValue::from(2), DataValue::from(1)]).unwrap(),
        DataValue::Bool(false)
    );
    assert_eq!(
        op_lt(&[DataValue::from(1), DataValue::from(1)]).unwrap(),
        DataValue::Bool(false)
    );
    assert_eq!(
        op_lt(&[DataValue::from(1), DataValue::from(1.0)]).unwrap(),
        DataValue::Bool(false)
    );
    assert_eq!(
        op_lt(&[DataValue::from(1), DataValue::from(2)]).unwrap(),
        DataValue::Bool(true)
    );
    assert!(op_lt(&[DataValue::Null, DataValue::Bool(true)]).is_err());
}

#[test]
fn test_max_min() {
    assert_eq!(op_max(&[DataValue::from(1),]).unwrap(), DataValue::from(1));
    assert_eq!(
        op_max(&[
            DataValue::from(1),
            DataValue::from(2),
            DataValue::from(3),
            DataValue::from(4)
        ])
        .unwrap(),
        DataValue::from(4)
    );
    assert_eq!(
        op_max(&[
            DataValue::from(1.0),
            DataValue::from(2),
            DataValue::from(3),
            DataValue::from(4)
        ])
        .unwrap(),
        DataValue::from(4)
    );
    assert_eq!(
        op_max(&[
            DataValue::from(1),
            DataValue::from(2),
            DataValue::from(3),
            DataValue::from(4.0)
        ])
        .unwrap(),
        DataValue::from(4.0)
    );
    assert!(op_max(&[]).is_err());
    assert!(op_max(&[DataValue::Bool(true)]).is_err());

    assert_eq!(op_min(&[DataValue::from(1),]).unwrap(), DataValue::from(1));
    assert_eq!(
        op_min(&[
            DataValue::from(1),
            DataValue::from(2),
            DataValue::from(3),
            DataValue::from(4)
        ])
        .unwrap(),
        DataValue::from(1)
    );
    assert_eq!(
        op_min(&[
            DataValue::from(1.0),
            DataValue::from(2),
            DataValue::from(3),
            DataValue::from(4)
        ])
        .unwrap(),
        DataValue::from(1.0)
    );
    assert_eq!(
        op_min(&[
            DataValue::from(1),
            DataValue::from(2),
            DataValue::from(3),
            DataValue::from(4.0)
        ])
        .unwrap(),
        DataValue::from(1)
    );
    assert!(op_max(&[]).is_err());
    assert!(op_max(&[DataValue::Bool(true)]).is_err());
}

#[test]
fn test_minus() {
    assert_eq!(
        op_minus(&[DataValue::from(-1)]).unwrap(),
        DataValue::from(1)
    );
    assert_eq!(
        op_minus(&[DataValue::from(1)]).unwrap(),
        DataValue::from(-1)
    );
    assert_eq!(
        op_minus(&[DataValue::from(f64::INFINITY)]).unwrap(),
        DataValue::from(f64::NEG_INFINITY)
    );
    assert_eq!(
        op_minus(&[DataValue::from(f64::NEG_INFINITY)]).unwrap(),
        DataValue::from(f64::INFINITY)
    );
}

#[test]
fn test_abs() {
    assert_eq!(op_abs(&[DataValue::from(-1)]).unwrap(), DataValue::from(1));
    assert_eq!(op_abs(&[DataValue::from(1)]).unwrap(), DataValue::from(1));
    assert_eq!(
        op_abs(&[DataValue::from(-1.5)]).unwrap(),
        DataValue::from(1.5)
    );
}

#[test]
fn test_signum() {
    assert_eq!(
        op_signum(&[DataValue::from(0.1)]).unwrap(),
        DataValue::from(1)
    );
    assert_eq!(
        op_signum(&[DataValue::from(-0.1)]).unwrap(),
        DataValue::from(-1)
    );
    assert_eq!(
        op_signum(&[DataValue::from(0.0)]).unwrap(),
        DataValue::from(0)
    );
    assert_eq!(
        op_signum(&[DataValue::from(-0.0)]).unwrap(),
        DataValue::from(-1)
    );
    assert_eq!(
        op_signum(&[DataValue::from(-3)]).unwrap(),
        DataValue::from(-1)
    );
    assert_eq!(
        op_signum(&[DataValue::from(f64::NEG_INFINITY)]).unwrap(),
        DataValue::from(-1)
    );
    assert!(op_signum(&[DataValue::from(f64::NAN)])
        .unwrap()
        .get_float()
        .unwrap()
        .is_nan());
}

#[test]
fn test_floor_ceil() {
    assert_eq!(
        op_floor(&[DataValue::from(-1)]).unwrap(),
        DataValue::from(-1)
    );
    assert_eq!(
        op_floor(&[DataValue::from(-1.5)]).unwrap(),
        DataValue::from(-2.0)
    );
    assert_eq!(
        op_floor(&[DataValue::from(1.5)]).unwrap(),
        DataValue::from(1.0)
    );
    assert_eq!(
        op_ceil(&[DataValue::from(-1)]).unwrap(),
        DataValue::from(-1)
    );
    assert_eq!(
        op_ceil(&[DataValue::from(-1.5)]).unwrap(),
        DataValue::from(-1.0)
    );
    assert_eq!(
        op_ceil(&[DataValue::from(1.5)]).unwrap(),
        DataValue::from(2.0)
    );
}

#[test]
fn test_round() {
    assert_eq!(
        op_round(&[DataValue::from(0.6)]).unwrap(),
        DataValue::from(1.0)
    );
    assert_eq!(
        op_round(&[DataValue::from(0.5)]).unwrap(),
        DataValue::from(1.0)
    );
    assert_eq!(
        op_round(&[DataValue::from(1.5)]).unwrap(),
        DataValue::from(2.0)
    );
    assert_eq!(
        op_round(&[DataValue::from(-0.6)]).unwrap(),
        DataValue::from(-1.0)
    );
    assert_eq!(
        op_round(&[DataValue::from(-0.5)]).unwrap(),
        DataValue::from(-1.0)
    );
    assert_eq!(
        op_round(&[DataValue::from(-1.5)]).unwrap(),
        DataValue::from(-2.0)
    );
}

#[test]
fn test_exp() {
    let n = op_exp(&[DataValue::from(1)]).unwrap().get_float().unwrap();
    assert!(n.abs_diff_eq(&f64::E(), 1E-5));

    let n = op_exp(&[DataValue::from(50.1)])
        .unwrap()
        .get_float()
        .unwrap();
    assert!(n.abs_diff_eq(&(50.1_f64.exp()), 1E-5));
}

#[test]
fn test_exp2() {
    let n = op_exp2(&[DataValue::from(10.)])
        .unwrap()
        .get_float()
        .unwrap();
    assert_eq!(n, 1024.);
}

#[test]
fn test_ln() {
    assert_eq!(
        op_ln(&[DataValue::from(f64::E())]).unwrap(),
        DataValue::from(1.0)
    );
}

#[test]
fn test_log2() {
    assert_eq!(
        op_log2(&[DataValue::from(1024)]).unwrap(),
        DataValue::from(10.)
    );
}

#[test]
fn test_log10() {
    assert_eq!(
        op_log10(&[DataValue::from(1000)]).unwrap(),
        DataValue::from(3.0)
    );
}

#[test]
fn test_trig() {
    assert!(op_sin(&[DataValue::from(f64::PI() / 2.)])
        .unwrap()
        .get_float()
        .unwrap()
        .abs_diff_eq(&1.0, 1e-5));
    assert!(op_cos(&[DataValue::from(f64::PI() / 2.)])
        .unwrap()
        .get_float()
        .unwrap()
        .abs_diff_eq(&0.0, 1e-5));
    assert!(op_tan(&[DataValue::from(f64::PI() / 4.)])
        .unwrap()
        .get_float()
        .unwrap()
        .abs_diff_eq(&1.0, 1e-5));
}

#[test]
fn test_inv_trig() {
    assert!(op_asin(&[DataValue::from(1.0)])
        .unwrap()
        .get_float()
        .unwrap()
        .abs_diff_eq(&(f64::PI() / 2.), 1e-5));
    assert!(op_acos(&[DataValue::from(0)])
        .unwrap()
        .get_float()
        .unwrap()
        .abs_diff_eq(&(f64::PI() / 2.), 1e-5));
    assert!(op_atan(&[DataValue::from(1)])
        .unwrap()
        .get_float()
        .unwrap()
        .abs_diff_eq(&(f64::PI() / 4.), 1e-5));
    assert!(op_atan2(&[DataValue::from(-1), DataValue::from(-1)])
        .unwrap()
        .get_float()
        .unwrap()
        .abs_diff_eq(&(-3. * f64::PI() / 4.), 1e-5));
}

#[test]
fn test_pow() {
    assert_eq!(
        op_pow(&[DataValue::from(2), DataValue::from(10)]).unwrap(),
        DataValue::from(1024.0)
    );
}

#[test]
fn test_mod() {
    assert_eq!(
        op_mod(&[DataValue::from(-10), DataValue::from(7)]).unwrap(),
        DataValue::from(-3)
    );
}

#[test]
fn test_boolean() {
    assert_eq!(op_and(&[]).unwrap(), DataValue::Bool(true));
    assert_eq!(
        op_and(&[DataValue::Bool(true), DataValue::Bool(false)]).unwrap(),
        DataValue::Bool(false)
    );
    assert_eq!(op_or(&[]).unwrap(), DataValue::Bool(false));
    assert_eq!(
        op_or(&[DataValue::Bool(true), DataValue::Bool(false)]).unwrap(),
        DataValue::Bool(true)
    );
    assert_eq!(
        op_negate(&[DataValue::Bool(false)]).unwrap(),
        DataValue::Bool(true)
    );
}

#[test]
fn test_bits() {
    assert_eq!(
        op_bit_and(&[
            DataValue::Bytes([0b111000].into()),
            DataValue::Bytes([0b010101].into())
        ])
        .unwrap(),
        DataValue::Bytes([0b010000].into())
    );
    assert_eq!(
        op_bit_or(&[
            DataValue::Bytes([0b111000].into()),
            DataValue::Bytes([0b010101].into())
        ])
        .unwrap(),
        DataValue::Bytes([0b111101].into())
    );
    assert_eq!(
        op_bit_not(&[DataValue::Bytes([0b00111000].into())]).unwrap(),
        DataValue::Bytes([0b11000111].into())
    );
    assert_eq!(
        op_bit_xor(&[
            DataValue::Bytes([0b111000].into()),
            DataValue::Bytes([0b010101].into())
        ])
        .unwrap(),
        DataValue::Bytes([0b101101].into())
    );
}

#[test]
fn test_pack_bits() {
    assert_eq!(
        op_pack_bits(&[DataValue::List(vec![DataValue::Bool(true)])]).unwrap(),
        DataValue::Bytes([0b10000000].into())
    )
}

#[test]
fn test_unpack_bits() {
    assert_eq!(
        op_unpack_bits(&[DataValue::Bytes([0b10101010].into())]).unwrap(),
        DataValue::List(
            [true, false, true, false, true, false, true, false]
                .into_iter()
                .map(DataValue::Bool)
                .collect()
        )
    )
}

#[test]
fn test_concat() {
    assert_eq!(
        op_concat(&[
            DataValue::String("abc".into()),
            DataValue::String("def".into())
        ])
        .unwrap(),
        DataValue::String("abcdef".into())
    );

    assert_eq!(
        op_concat(&[
            DataValue::List(vec![DataValue::Bool(true), DataValue::Bool(false)]),
            DataValue::List(vec![DataValue::Bool(true)])
        ])
        .unwrap(),
        DataValue::List(vec![
            DataValue::Bool(true),
            DataValue::Bool(false),
            DataValue::Bool(true)
        ])
    );
}

#[test]
fn test_str_includes() {
    assert_eq!(
        op_str_includes(&[
            DataValue::String("abcdef".into()),
            DataValue::String("bcd".into())
        ])
        .unwrap(),
        DataValue::Bool(true)
    );
    assert_eq!(
        op_str_includes(&[
            DataValue::String("abcdef".into()),
            DataValue::String("bd".into())
        ])
        .unwrap(),
        DataValue::Bool(false)
    );
}

#[test]
fn test_casings() {
    assert_eq!(
        op_lowercase(&[DataValue::String("NAÏVE".into())]).unwrap(),
        DataValue::String("naïve".into())
    );
    assert_eq!(
        op_uppercase(&[DataValue::String("naïve".into())]).unwrap(),
        DataValue::String("NAÏVE".into())
    );
}

#[test]
fn test_trim() {
    assert_eq!(
        op_trim(&[DataValue::String(" a ".into())]).unwrap(),
        DataValue::String("a".into())
    );
    assert_eq!(
        op_trim_start(&[DataValue::String(" a ".into())]).unwrap(),
        DataValue::String("a ".into())
    );
    assert_eq!(
        op_trim_end(&[DataValue::String(" a ".into())]).unwrap(),
        DataValue::String(" a".into())
    );
}

#[test]
fn test_starts_ends_with() {
    assert_eq!(
        op_starts_with(&[
            DataValue::String("abcdef".into()),
            DataValue::String("abc".into())
        ])
        .unwrap(),
        DataValue::Bool(true)
    );
    assert_eq!(
        op_starts_with(&[
            DataValue::String("abcdef".into()),
            DataValue::String("bc".into())
        ])
        .unwrap(),
        DataValue::Bool(false)
    );
    assert_eq!(
        op_ends_with(&[
            DataValue::String("abcdef".into()),
            DataValue::String("def".into())
        ])
        .unwrap(),
        DataValue::Bool(true)
    );
    assert_eq!(
        op_ends_with(&[
            DataValue::String("abcdef".into()),
            DataValue::String("bc".into())
        ])
        .unwrap(),
        DataValue::Bool(false)
    );
}

#[test]
fn test_regex() {
    assert_eq!(
        op_regex_matches(&[
            DataValue::String("abcdef".into()),
            DataValue::Regex(RegexWrapper(Regex::new("c.e").unwrap()))
        ])
        .unwrap(),
        DataValue::Bool(true)
    );

    assert_eq!(
        op_regex_matches(&[
            DataValue::String("abcdef".into()),
            DataValue::Regex(RegexWrapper(Regex::new("c.ef$").unwrap()))
        ])
        .unwrap(),
        DataValue::Bool(true)
    );

    assert_eq!(
        op_regex_matches(&[
            DataValue::String("abcdef".into()),
            DataValue::Regex(RegexWrapper(Regex::new("c.e$").unwrap()))
        ])
        .unwrap(),
        DataValue::Bool(false)
    );

    assert_eq!(
        op_regex_replace(&[
            DataValue::String("abcdef".into()),
            DataValue::Regex(RegexWrapper(Regex::new("[be]").unwrap())),
            DataValue::String("x".into())
        ])
        .unwrap(),
        DataValue::String("axcdef".into())
    );

    assert_eq!(
        op_regex_replace_all(&[
            DataValue::String("abcdef".into()),
            DataValue::Regex(RegexWrapper(Regex::new("[be]").unwrap())),
            DataValue::String("x".into())
        ])
        .unwrap(),
        DataValue::String("axcdxf".into())
    );
    assert_eq!(
        op_regex_extract(&[
            DataValue::String("abCDefGH".into()),
            DataValue::Regex(RegexWrapper(Regex::new("[xayef]|(GH)").unwrap()))
        ])
        .unwrap(),
        DataValue::List(vec![
            DataValue::String("a".into()),
            DataValue::String("e".into()),
            DataValue::String("f".into()),
            DataValue::String("GH".into()),
        ])
    );
    assert_eq!(
        op_regex_extract_first(&[
            DataValue::String("abCDefGH".into()),
            DataValue::Regex(RegexWrapper(Regex::new("[xayef]|(GH)").unwrap()))
        ])
        .unwrap(),
        DataValue::String("a".into()),
    );
    assert_eq!(
        op_regex_extract(&[
            DataValue::String("abCDefGH".into()),
            DataValue::Regex(RegexWrapper(Regex::new("xyz").unwrap()))
        ])
        .unwrap(),
        DataValue::List(vec![])
    );

    assert_eq!(
        op_regex_extract_first(&[
            DataValue::String("abCDefGH".into()),
            DataValue::Regex(RegexWrapper(Regex::new("xyz").unwrap()))
        ])
        .unwrap(),
        DataValue::Null
    );
}

#[test]
fn test_predicates() {
    assert_eq!(
        op_is_null(&[DataValue::Null]).unwrap(),
        DataValue::Bool(true)
    );
    assert_eq!(
        op_is_null(&[DataValue::Bottom]).unwrap(),
        DataValue::Bool(false)
    );
    assert_eq!(
        op_is_int(&[DataValue::from(1)]).unwrap(),
        DataValue::Bool(true)
    );
    assert_eq!(
        op_is_int(&[DataValue::from(1.0)]).unwrap(),
        DataValue::Bool(false)
    );
    assert_eq!(
        op_is_float(&[DataValue::from(1)]).unwrap(),
        DataValue::Bool(false)
    );
    assert_eq!(
        op_is_float(&[DataValue::from(1.0)]).unwrap(),
        DataValue::Bool(true)
    );
    assert_eq!(
        op_is_num(&[DataValue::from(1)]).unwrap(),
        DataValue::Bool(true)
    );
    assert_eq!(
        op_is_num(&[DataValue::from(1.0)]).unwrap(),
        DataValue::Bool(true)
    );
    assert_eq!(
        op_is_num(&[DataValue::Null]).unwrap(),
        DataValue::Bool(false)
    );
    assert_eq!(
        op_is_bytes(&[DataValue::Bytes([0b1].into())]).unwrap(),
        DataValue::Bool(true)
    );
    assert_eq!(
        op_is_bytes(&[DataValue::Null]).unwrap(),
        DataValue::Bool(false)
    );
    assert_eq!(
        op_is_list(&[DataValue::List(vec![])]).unwrap(),
        DataValue::Bool(true)
    );
    assert_eq!(
        op_is_list(&[DataValue::Null]).unwrap(),
        DataValue::Bool(false)
    );
    assert_eq!(
        op_is_string(&[DataValue::String("".into())]).unwrap(),
        DataValue::Bool(true)
    );
    assert_eq!(
        op_is_string(&[DataValue::Null]).unwrap(),
        DataValue::Bool(false)
    );
    assert_eq!(
        op_is_finite(&[DataValue::from(1.0)]).unwrap(),
        DataValue::Bool(true)
    );
    assert_eq!(
        op_is_finite(&[DataValue::from(f64::INFINITY)]).unwrap(),
        DataValue::Bool(false)
    );
    assert_eq!(
        op_is_finite(&[DataValue::from(f64::NAN)]).unwrap(),
        DataValue::Bool(false)
    );
    assert_eq!(
        op_is_infinite(&[DataValue::from(1.0)]).unwrap(),
        DataValue::Bool(false)
    );
    assert_eq!(
        op_is_infinite(&[DataValue::from(f64::INFINITY)]).unwrap(),
        DataValue::Bool(true)
    );
    assert_eq!(
        op_is_infinite(&[DataValue::from(f64::NEG_INFINITY)]).unwrap(),
        DataValue::Bool(true)
    );
    assert_eq!(
        op_is_infinite(&[DataValue::from(f64::NAN)]).unwrap(),
        DataValue::Bool(false)
    );
    assert_eq!(
        op_is_nan(&[DataValue::from(1.0)]).unwrap(),
        DataValue::Bool(false)
    );
    assert_eq!(
        op_is_nan(&[DataValue::from(f64::INFINITY)]).unwrap(),
        DataValue::Bool(false)
    );
    assert_eq!(
        op_is_nan(&[DataValue::from(f64::NEG_INFINITY)]).unwrap(),
        DataValue::Bool(false)
    );
    assert_eq!(
        op_is_nan(&[DataValue::from(f64::NAN)]).unwrap(),
        DataValue::Bool(true)
    );
}

#[test]
fn test_prepend_append() {
    assert_eq!(
        op_prepend(&[
            DataValue::List(vec![DataValue::from(1), DataValue::from(2)]),
            DataValue::Null,
        ])
        .unwrap(),
        DataValue::List(vec![
            DataValue::Null,
            DataValue::from(1),
            DataValue::from(2)
        ]),
    );
    assert_eq!(
        op_append(&[
            DataValue::List(vec![DataValue::from(1), DataValue::from(2)]),
            DataValue::Null,
        ])
        .unwrap(),
        DataValue::List(vec![
            DataValue::from(1),
            DataValue::from(2),
            DataValue::Null,
        ]),
    );
}

#[test]
fn test_length() {
    assert_eq!(
        op_length(&[DataValue::String("abc".into())]).unwrap(),
        DataValue::from(3)
    );
    assert_eq!(
        op_length(&[DataValue::List(vec![])]).unwrap(),
        DataValue::from(0)
    );
    assert_eq!(
        op_length(&[DataValue::Bytes([].into())]).unwrap(),
        DataValue::from(0)
    );
}

#[test]
fn test_unicode_normalize() {
    assert_eq!(
        op_unicode_normalize(&[
            DataValue::String("abc".into()),
            DataValue::String("nfc".into())
        ])
        .unwrap(),
        DataValue::String("abc".into())
    )
}

#[test]
fn test_sort_reverse() {
    assert_eq!(
        op_sorted(&[DataValue::List(vec![
            DataValue::from(2.0),
            DataValue::from(1),
            DataValue::from(2),
            DataValue::Null
        ])])
        .unwrap(),
        DataValue::List(vec![
            DataValue::Null,
            DataValue::from(1),
            DataValue::from(2),
            DataValue::from(2.0),
        ])
    );
    assert_eq!(
        op_reverse(&[DataValue::List(vec![
            DataValue::from(2.0),
            DataValue::from(1),
            DataValue::from(2),
            DataValue::Null
        ])])
        .unwrap(),
        DataValue::List(vec![
            DataValue::Null,
            DataValue::from(2),
            DataValue::from(1),
            DataValue::from(2.0),
        ])
    )
}

#[test]
fn test_haversine() {
    let d = op_haversine_deg_input(&[
        DataValue::from(0),
        DataValue::from(0),
        DataValue::from(0),
        DataValue::from(180),
    ])
    .unwrap()
    .get_float()
    .unwrap();
    assert!(d.abs_diff_eq(&f64::PI(), 1e-5));

    let d = op_haversine_deg_input(&[
        DataValue::from(90),
        DataValue::from(0),
        DataValue::from(0),
        DataValue::from(123),
    ])
    .unwrap()
    .get_float()
    .unwrap();
    assert!(d.abs_diff_eq(&(f64::PI() / 2.), 1e-5));

    let d = op_haversine(&[
        DataValue::from(0),
        DataValue::from(0),
        DataValue::from(0),
        DataValue::from(f64::PI()),
    ])
    .unwrap()
    .get_float()
    .unwrap();
    assert!(d.abs_diff_eq(&f64::PI(), 1e-5));
}

#[test]
fn test_deg_rad() {
    assert_eq!(
        op_deg_to_rad(&[DataValue::from(180)]).unwrap(),
        DataValue::from(f64::PI())
    );
    assert_eq!(
        op_rad_to_deg(&[DataValue::from(f64::PI())]).unwrap(),
        DataValue::from(180.0)
    );
}

#[test]
fn test_first_last() {
    assert_eq!(
        op_first(&[DataValue::List(vec![])]).unwrap(),
        DataValue::Null,
    );
    assert_eq!(
        op_last(&[DataValue::List(vec![])]).unwrap(),
        DataValue::Null,
    );
    assert_eq!(
        op_first(&[DataValue::List(vec![
            DataValue::from(1),
            DataValue::from(2)
        ])])
        .unwrap(),
        DataValue::from(1),
    );
    assert_eq!(
        op_last(&[DataValue::List(vec![
            DataValue::from(1),
            DataValue::from(2)
        ])])
        .unwrap(),
        DataValue::from(2),
    );
}

#[test]
fn test_chunks() {
    assert_eq!(
        op_chunks(&[
            DataValue::List(vec![
                DataValue::from(1),
                DataValue::from(2),
                DataValue::from(3),
                DataValue::from(4),
                DataValue::from(5),
            ]),
            DataValue::from(2),
        ])
        .unwrap(),
        DataValue::List(vec![
            DataValue::List(vec![DataValue::from(1), DataValue::from(2),]),
            DataValue::List(vec![DataValue::from(3), DataValue::from(4),]),
            DataValue::List(vec![DataValue::from(5)]),
        ])
    );
    assert_eq!(
        op_chunks_exact(&[
            DataValue::List(vec![
                DataValue::from(1),
                DataValue::from(2),
                DataValue::from(3),
                DataValue::from(4),
                DataValue::from(5),
            ]),
            DataValue::from(2),
        ])
        .unwrap(),
        DataValue::List(vec![
            DataValue::List(vec![DataValue::from(1), DataValue::from(2),]),
            DataValue::List(vec![DataValue::from(3), DataValue::from(4),]),
        ])
    );
    assert_eq!(
        op_windows(&[
            DataValue::List(vec![
                DataValue::from(1),
                DataValue::from(2),
                DataValue::from(3),
                DataValue::from(4),
                DataValue::from(5),
            ]),
            DataValue::from(3),
        ])
        .unwrap(),
        DataValue::List(vec![
            DataValue::List(vec![
                DataValue::from(1),
                DataValue::from(2),
                DataValue::from(3)
            ]),
            DataValue::List(vec![
                DataValue::from(2),
                DataValue::from(3),
                DataValue::from(4)
            ]),
            DataValue::List(vec![
                DataValue::from(3),
                DataValue::from(4),
                DataValue::from(5)
            ]),
        ])
    )
}

#[test]
fn test_get() {
    assert!(op_get(&[DataValue::List(vec![]), DataValue::from(0)]).is_err());
    assert_eq!(
        op_get(&[
            DataValue::List(vec![
                DataValue::from(1),
                DataValue::from(2),
                DataValue::from(3)
            ]),
            DataValue::from(1)
        ])
        .unwrap(),
        DataValue::from(2)
    );
    assert_eq!(
        op_maybe_get(&[DataValue::List(vec![]), DataValue::from(0)]).unwrap(),
        DataValue::Null
    );
    assert_eq!(
        op_maybe_get(&[
            DataValue::List(vec![
                DataValue::from(1),
                DataValue::from(2),
                DataValue::from(3)
            ]),
            DataValue::from(1)
        ])
        .unwrap(),
        DataValue::from(2)
    );
}

#[test]
fn test_slice() {
    assert!(op_slice(&[
        DataValue::List(vec![
            DataValue::from(1),
            DataValue::from(2),
            DataValue::from(3)
        ]),
        DataValue::from(1),
        DataValue::from(4)
    ])
    .is_err());

    assert_eq!(
        op_slice(&[
            DataValue::List(vec![
                DataValue::from(1),
                DataValue::from(2),
                DataValue::from(3)
            ]),
            DataValue::from(1),
            DataValue::from(-1)
        ])
        .unwrap(),
        DataValue::List(vec![DataValue::from(2)])
    );
}

#[test]
fn test_chars() {
    assert_eq!(
        op_from_substrings(&[op_chars(&[DataValue::String("abc".into())]).unwrap()]).unwrap(),
        DataValue::String("abc".into())
    )
}

#[test]
fn test_encode_decode() {
    assert_eq!(
        op_decode_base64(&[op_encode_base64(&[DataValue::Bytes([1, 2, 3].into())]).unwrap()])
            .unwrap(),
        DataValue::Bytes([1, 2, 3].into())
    )
}

#[test]
fn test_to_float() {
    assert_eq!(
        op_to_float(&[DataValue::from(1)]).unwrap(),
        DataValue::from(1.0)
    );
    assert_eq!(
        op_to_float(&[DataValue::from(1.0)]).unwrap(),
        DataValue::from(1.0)
    );
    assert!(op_to_float(&[DataValue::String("NAN".into())])
        .unwrap()
        .get_float()
        .unwrap()
        .is_nan());
    assert!(op_to_float(&[DataValue::String("INF".into())])
        .unwrap()
        .get_float()
        .unwrap()
        .is_infinite());
    assert!(op_to_float(&[DataValue::String("NEG_INF".into())])
        .unwrap()
        .get_float()
        .unwrap()
        .is_infinite());
    assert_eq!(
        op_to_float(&[DataValue::String("3".into())])
            .unwrap()
            .get_float()
            .unwrap(),
        3.
    );
}

#[test]
fn test_rand() {
    let n = op_rand_float(&[]).unwrap().get_float().unwrap();
    assert!(n >= 0.);
    assert!(n <= 1.);
    assert_eq!(
        op_rand_bernoulli(&[DataValue::from(0)]).unwrap(),
        DataValue::Bool(false)
    );
    assert_eq!(
        op_rand_bernoulli(&[DataValue::from(1)]).unwrap(),
        DataValue::Bool(true)
    );
    assert!(op_rand_bernoulli(&[DataValue::from(2)]).is_err());
    let n = op_rand_int(&[DataValue::from(100), DataValue::from(200)])
        .unwrap()
        .get_int()
        .unwrap();
    assert!(n >= 100);
    assert!(n <= 200);
    assert_eq!(
        op_rand_choose(&[DataValue::List(vec![])]).unwrap(),
        DataValue::Null
    );
    assert_eq!(
        op_rand_choose(&[DataValue::List(vec![DataValue::from(123)])]).unwrap(),
        DataValue::from(123)
    );
}
