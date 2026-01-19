// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Aggregate function implementations for vectorized execution.
//!
//! This module provides aggregate function helpers that can be used
//! for computing aggregates on groups of values.

use serde_json::{Value, json};

use crate::query::expr::Operator;

/// Compare two values using the given comparison operator.
/// Returns `Value::Bool(result)` for successful comparisons, `Value::Null` for incompatible types.
pub fn compare_values(l: &Value, r: &Value, op: &Operator) -> Value {
    match (l, r) {
        (Value::Number(ln), Value::Number(rn)) => {
            if let (Some(lf), Some(rf)) = (ln.as_f64(), rn.as_f64()) {
                let res = match op {
                    Operator::Gt => lf > rf,
                    Operator::GtEq => lf >= rf,
                    Operator::Lt => lf < rf,
                    Operator::LtEq => lf <= rf,
                    _ => false,
                };
                Value::Bool(res)
            } else {
                Value::Null
            }
        }
        (Value::String(ls), Value::String(rs)) => {
            let res = match op {
                Operator::Gt => ls > rs,
                Operator::GtEq => ls >= rs,
                Operator::Lt => ls < rs,
                Operator::LtEq => ls <= rs,
                _ => false,
            };
            Value::Bool(res)
        }
        _ => Value::Null,
    }
}

/// Compute COUNT aggregate on pre-evaluated values.
/// If `count_star` is true, counts all rows. Otherwise counts non-null values.
pub fn count_aggregate(values: &[Value], count_star: bool) -> Value {
    if count_star {
        json!(values.len())
    } else {
        let count = values.iter().filter(|v| !v.is_null()).count();
        json!(count)
    }
}

/// Compute SUM aggregate on pre-evaluated numeric values.
/// Returns `Value::Null` if all values are null.
pub fn sum_aggregate(values: &[Value]) -> Value {
    let mut sum = 0.0;
    let mut all_null = true;

    for val in values {
        if let Some(f) = val.as_f64() {
            sum += f;
            all_null = false;
        } else if let Some(i) = val.as_i64() {
            sum += i as f64;
            all_null = false;
        }
    }

    if all_null { Value::Null } else { json!(sum) }
}

/// Compute AVG aggregate on pre-evaluated numeric values.
/// Returns `Value::Null` if no numeric values are present.
pub fn avg_aggregate(values: &[Value]) -> Value {
    let mut sum = 0.0;
    let mut count = 0;

    for val in values {
        if let Some(f) = val.as_f64() {
            sum += f;
            count += 1;
        } else if let Some(i) = val.as_i64() {
            sum += i as f64;
            count += 1;
        }
    }

    if count == 0 {
        Value::Null
    } else {
        json!(sum / count as f64)
    }
}

/// Compute MIN aggregate on pre-evaluated values.
/// Supports numeric and string values. Returns `Value::Null` if all values are null.
pub fn min_aggregate(values: &[Value]) -> Value {
    let mut min_val: Option<&Value> = None;

    for val in values {
        if val.is_null() {
            continue;
        }
        if let Some(current) = min_val {
            if compare_values(val, current, &Operator::Lt) == Value::Bool(true) {
                min_val = Some(val);
            }
        } else {
            min_val = Some(val);
        }
    }

    min_val.cloned().unwrap_or(Value::Null)
}

/// Compute MAX aggregate on pre-evaluated values.
/// Supports numeric and string values. Returns `Value::Null` if all values are null.
pub fn max_aggregate(values: &[Value]) -> Value {
    let mut max_val: Option<&Value> = None;

    for val in values {
        if val.is_null() {
            continue;
        }
        if let Some(current) = max_val {
            if compare_values(val, current, &Operator::Gt) == Value::Bool(true) {
                max_val = Some(val);
            }
        } else {
            max_val = Some(val);
        }
    }

    max_val.cloned().unwrap_or(Value::Null)
}

/// Compute COLLECT aggregate on pre-evaluated values.
/// Collects all non-null values into an array.
pub fn collect_aggregate(values: &[Value]) -> Value {
    let list: Vec<Value> = values.iter().filter(|v| !v.is_null()).cloned().collect();
    Value::Array(list)
}

/// Compute an aggregate function by name on pre-evaluated values.
/// This is useful when values have already been evaluated.
pub fn compute_aggregate_on_values(
    name: &str,
    values: &[Value],
    count_star: bool,
) -> Option<Value> {
    match name.to_uppercase().as_str() {
        "COUNT" => Some(count_aggregate(values, count_star)),
        "SUM" => Some(sum_aggregate(values)),
        "AVG" => Some(avg_aggregate(values)),
        "MIN" => Some(min_aggregate(values)),
        "MAX" => Some(max_aggregate(values)),
        "COLLECT" => Some(collect_aggregate(values)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_count_star() {
        let values = vec![json!(1), json!(2), Value::Null, json!(4)];
        assert_eq!(count_aggregate(&values, true), json!(4));
    }

    #[test]
    fn test_count_non_null() {
        let values = vec![json!(1), json!(2), Value::Null, json!(4)];
        assert_eq!(count_aggregate(&values, false), json!(3));
    }

    #[test]
    fn test_sum() {
        let values = vec![json!(1), json!(2), json!(3), json!(4)];
        assert_eq!(sum_aggregate(&values), json!(10.0));
    }

    #[test]
    fn test_sum_with_nulls() {
        let values = vec![json!(1), Value::Null, json!(3)];
        assert_eq!(sum_aggregate(&values), json!(4.0));
    }

    #[test]
    fn test_sum_all_nulls() {
        let values = vec![Value::Null, Value::Null];
        assert_eq!(sum_aggregate(&values), Value::Null);
    }

    #[test]
    fn test_avg() {
        let values = vec![json!(2), json!(4), json!(6)];
        assert_eq!(avg_aggregate(&values), json!(4.0));
    }

    #[test]
    fn test_min() {
        let values = vec![json!(5), json!(2), json!(8), json!(1)];
        assert_eq!(min_aggregate(&values), json!(1));
    }

    #[test]
    fn test_max() {
        let values = vec![json!(5), json!(2), json!(8), json!(1)];
        assert_eq!(max_aggregate(&values), json!(8));
    }

    #[test]
    fn test_min_strings() {
        let values = vec![json!("banana"), json!("apple"), json!("cherry")];
        assert_eq!(min_aggregate(&values), json!("apple"));
    }

    #[test]
    fn test_max_strings() {
        let values = vec![json!("banana"), json!("apple"), json!("cherry")];
        assert_eq!(max_aggregate(&values), json!("cherry"));
    }

    #[test]
    fn test_collect() {
        let values = vec![json!(1), Value::Null, json!(3)];
        assert_eq!(collect_aggregate(&values), json!([1, 3]));
    }

    #[test]
    fn test_compare_values_numbers() {
        assert_eq!(
            compare_values(&json!(5), &json!(3), &Operator::Gt),
            Value::Bool(true)
        );
        assert_eq!(
            compare_values(&json!(3), &json!(5), &Operator::Lt),
            Value::Bool(true)
        );
    }

    #[test]
    fn test_compare_values_strings() {
        assert_eq!(
            compare_values(&json!("b"), &json!("a"), &Operator::Gt),
            Value::Bool(true)
        );
    }
}
