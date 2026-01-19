// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use serde_json::json;
use uni_query::query::expr_eval::{eval_scalar_function, is_scalar_function};

#[test]
fn test_math_functions() {
    // LOG
    let res = eval_scalar_function("LOG", &[json!(std::f64::consts::E)]).unwrap();
    assert!((res.as_f64().unwrap() - 1.0).abs() < 1e-10);

    // LOG10
    let res = eval_scalar_function("LOG10", &[json!(100.0)]).unwrap();
    assert!((res.as_f64().unwrap() - 2.0).abs() < 1e-10);

    // EXP
    let res = eval_scalar_function("EXP", &[json!(1.0)]).unwrap();
    assert!((res.as_f64().unwrap() - std::f64::consts::E).abs() < 1e-10);

    // POWER
    let res = eval_scalar_function("POWER", &[json!(2.0), json!(3.0)]).unwrap();
    assert_eq!(res.as_f64().unwrap(), 8.0);

    let res = eval_scalar_function("POW", &[json!(3.0), json!(2.0)]).unwrap();
    assert_eq!(res.as_f64().unwrap(), 9.0);

    // SIN
    let res = eval_scalar_function("SIN", &[json!(0.0)]).unwrap();
    assert_eq!(res.as_f64().unwrap(), 0.0);

    // COS
    let res = eval_scalar_function("COS", &[json!(0.0)]).unwrap();
    assert_eq!(res.as_f64().unwrap(), 1.0);

    // TAN
    let res = eval_scalar_function("TAN", &[json!(0.0)]).unwrap();
    assert_eq!(res.as_f64().unwrap(), 0.0);
}

#[test]
fn test_string_functions_pad() {
    // LPAD
    let res = eval_scalar_function("LPAD", &[json!("abc"), json!(5)]).unwrap();
    assert_eq!(res.as_str().unwrap(), "  abc");

    let res = eval_scalar_function("LPAD", &[json!("abc"), json!(5), json!("x")]).unwrap();
    assert_eq!(res.as_str().unwrap(), "xxabc");

    let res = eval_scalar_function("LPAD", &[json!("abc"), json!(6), json!("xy")]).unwrap();
    assert_eq!(res.as_str().unwrap(), "xyxabc");

    // RPAD
    let res = eval_scalar_function("RPAD", &[json!("abc"), json!(5)]).unwrap();
    assert_eq!(res.as_str().unwrap(), "abc  ");

    let res = eval_scalar_function("RPAD", &[json!("abc"), json!(5), json!("x")]).unwrap();
    assert_eq!(res.as_str().unwrap(), "abcxx");

    let res = eval_scalar_function("RPAD", &[json!("abc"), json!(6), json!("xy")]).unwrap();
    assert_eq!(res.as_str().unwrap(), "abcxyx");

    // Truncation behavior
    let res = eval_scalar_function("LPAD", &[json!("abc"), json!(2)]).unwrap();
    assert_eq!(res.as_str().unwrap(), "ab");

    let res = eval_scalar_function("RPAD", &[json!("abc"), json!(2)]).unwrap();
    assert_eq!(res.as_str().unwrap(), "ab");
}

#[test]
fn test_is_scalar_function() {
    assert!(is_scalar_function("log"));
    assert!(is_scalar_function("power"));
    assert!(is_scalar_function("lpad"));
    assert!(is_scalar_function("rpad"));
    assert!(!is_scalar_function("non_existent"));
}
