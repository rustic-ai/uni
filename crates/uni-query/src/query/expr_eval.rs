// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Expression evaluation helper functions.
//!
//! This module extracts high-complexity expression evaluation logic from the main executor
//! to reduce cognitive complexity and improve maintainability.

use anyhow::{Result, anyhow};
use serde_json::{Value, json};
use std::cmp::Ordering;

use crate::query::datetime::{eval_datetime_function, parse_datetime_utc};
use crate::query::expr::Operator;

/// Evaluate a binary operation on two already-evaluated values.
///
/// This function handles all binary operators (Eq, NotEq, And, Or, Gt, Lt, etc.)
/// and returns the result of the operation.
pub fn eval_binary_op(left: &Value, op: &Operator, right: &Value) -> Result<Value> {
    match op {
        Operator::Eq => Ok(Value::Bool(left == right)),
        Operator::NotEq => Ok(Value::Bool(left != right)),
        Operator::And => {
            let l = left
                .as_bool()
                .ok_or_else(|| anyhow!("Expected bool for AND left operand"))?;
            let r = right
                .as_bool()
                .ok_or_else(|| anyhow!("Expected bool for AND right operand"))?;
            Ok(Value::Bool(l && r))
        }
        Operator::Or => {
            let l = left
                .as_bool()
                .ok_or_else(|| anyhow!("Expected bool for OR left operand"))?;
            let r = right
                .as_bool()
                .ok_or_else(|| anyhow!("Expected bool for OR right operand"))?;
            Ok(Value::Bool(l || r))
        }
        Operator::Gt => eval_comparison(left, right, |ordering| ordering.is_gt()),
        Operator::Lt => eval_comparison(left, right, |ordering| ordering.is_lt()),
        Operator::GtEq => eval_comparison(left, right, |ordering| ordering.is_ge()),
        Operator::LtEq => eval_comparison(left, right, |ordering| ordering.is_le()),
        Operator::In => {
            if let Value::Array(arr) = right {
                Ok(Value::Bool(arr.contains(left)))
            } else {
                Err(anyhow!("Right side of IN must be a list"))
            }
        }
        Operator::Contains => {
            let l = left
                .as_str()
                .ok_or_else(|| anyhow!("Left side of CONTAINS must be a string"))?;
            let r = right
                .as_str()
                .ok_or_else(|| anyhow!("Right side of CONTAINS must be a string"))?;
            Ok(Value::Bool(l.contains(r)))
        }
        Operator::StartsWith => {
            let l = left
                .as_str()
                .ok_or_else(|| anyhow!("Left side of STARTS WITH must be a string"))?;
            let r = right
                .as_str()
                .ok_or_else(|| anyhow!("Right side of STARTS WITH must be a string"))?;
            Ok(Value::Bool(l.starts_with(r)))
        }
        Operator::EndsWith => {
            let l = left
                .as_str()
                .ok_or_else(|| anyhow!("Left side of ENDS WITH must be a string"))?;
            let r = right
                .as_str()
                .ok_or_else(|| anyhow!("Right side of ENDS WITH must be a string"))?;
            Ok(Value::Bool(l.ends_with(r)))
        }
        Operator::Add => {
            if let (Some(l), Some(r)) = (left.as_f64(), right.as_f64()) {
                // Determine if result should be int or float
                if left.is_i64() && right.is_i64() {
                    Ok(json!(left.as_i64().unwrap() + right.as_i64().unwrap()))
                } else {
                    Ok(json!(l + r))
                }
            } else if let (Value::String(l), Value::String(r)) = (left, right) {
                Ok(Value::String(format!("{}{}", l, r)))
            } else {
                Err(anyhow!("Invalid types for addition"))
            }
        }
        Operator::Sub => eval_numeric_op(left, right, |a, b| a - b),
        Operator::Mul => eval_numeric_op(left, right, |a, b| a * b),
        Operator::Div => eval_numeric_op(left, right, |a, b| a / b),
        Operator::Mod => eval_numeric_op(left, right, |a, b| a % b),
        Operator::Pow => eval_numeric_op(left, right, |a, b| a.powf(b)),
        Operator::ApproxEq => Err(anyhow!(
            "ApproxEq (~=) operator requires vector index optimization"
        )),
    }
}

fn eval_numeric_op<F>(left: &Value, right: &Value, op: F) -> Result<Value>
where
    F: Fn(f64, f64) -> f64,
{
    if let (Some(l), Some(r)) = (left.as_f64(), right.as_f64()) {
        let result = op(l, r);
        // If result is integer-like and inputs were integers, we might want to return int?
        // But for Div/Pow/Mod/Sub with float op, result is float.
        // For simple math, let's return float if op returns float.
        // If inputs were ints and op is integer-safe (like sub/mul), we could return int.
        // But `op` takes f64.

        // Special case: if result is integer and inputs were integer?
        if result.fract() == 0.0 && left.is_i64() && right.is_i64() {
            // Maybe return int?
            // json! macro handles this if we pass integer.
            Ok(json!(result as i64))
        } else {
            Ok(json!(result))
        }
    } else {
        Err(anyhow!("Arithmetic operation requires numbers"))
    }
}

/// Helper for comparisons.
fn eval_comparison<F>(left: &Value, right: &Value, check: F) -> Result<Value>
where
    F: Fn(Ordering) -> bool,
{
    if let (Some(l), Some(r)) = (left.as_i64(), right.as_i64()) {
        return Ok(Value::Bool(check(l.cmp(&r))));
    }
    if let (Some(l), Some(r)) = (left.as_f64(), right.as_f64()) {
        let ord = l
            .partial_cmp(&r)
            .ok_or_else(|| anyhow!("Cannot compare NaN"))?;
        return Ok(Value::Bool(check(ord)));
    }
    if let (Some(l), Some(r)) = (left.as_str(), right.as_str()) {
        return Ok(Value::Bool(check(l.cmp(r))));
    }
    Err(anyhow!("Comparison only supported for numbers and strings"))
}

// ============================================================================
// List/Collection function helpers
// ============================================================================

fn eval_size(arg: &Value) -> Result<Value> {
    match arg {
        Value::Array(arr) => Ok(json!(arr.len())),
        Value::Object(map) => Ok(json!(map.len())),
        Value::String(s) => Ok(json!(s.len())),
        Value::Null => Ok(Value::Null),
        _ => Err(anyhow!("size() expects a List, Map, or String")),
    }
}

fn eval_keys(arg: &Value) -> Result<Value> {
    match arg {
        Value::Object(map) => Ok(json!(map.keys().collect::<Vec<_>>())),
        Value::Null => Ok(Value::Null),
        _ => Err(anyhow!("keys() expects a Map")),
    }
}

fn eval_head(arg: &Value) -> Result<Value> {
    match arg {
        Value::Array(arr) => Ok(arr.first().cloned().unwrap_or(Value::Null)),
        Value::Null => Ok(Value::Null),
        _ => Err(anyhow!("head() expects a List")),
    }
}

fn eval_tail(arg: &Value) -> Result<Value> {
    match arg {
        Value::Array(arr) => {
            if arr.is_empty() {
                Ok(json!([]))
            } else {
                Ok(json!(arr[1..]))
            }
        }
        Value::Null => Ok(Value::Null),
        _ => Err(anyhow!("tail() expects a List")),
    }
}

fn eval_last(arg: &Value) -> Result<Value> {
    match arg {
        Value::Array(arr) => Ok(arr.last().cloned().unwrap_or(Value::Null)),
        Value::Null => Ok(Value::Null),
        _ => Err(anyhow!("last() expects a List")),
    }
}

fn eval_length(arg: &Value) -> Result<Value> {
    match arg {
        Value::Array(arr) => Ok(json!(arr.len())),
        Value::String(s) => Ok(json!(s.len())),
        Value::Object(map) => {
            // Path object?
            if map.contains_key("nodes")
                && map.contains_key("relationships")
                && let Some(Value::Array(rels)) = map.get("relationships")
            {
                return Ok(json!(rels.len()));
            }
            Ok(Value::Null)
        }
        Value::Null => Ok(Value::Null),
        _ => Err(anyhow!("length() expects a List, String, or Path")),
    }
}

fn eval_nodes(arg: &Value) -> Result<Value> {
    match arg {
        Value::Object(map) => {
            if let Some(nodes) = map.get("nodes") {
                Ok(nodes.clone())
            } else {
                Ok(Value::Null)
            }
        }
        Value::Null => Ok(Value::Null),
        _ => Err(anyhow!("nodes() expects a Path")),
    }
}

fn eval_relationships(arg: &Value) -> Result<Value> {
    match arg {
        Value::Object(map) => {
            if let Some(rels) = map.get("relationships") {
                Ok(rels.clone())
            } else {
                Ok(Value::Null)
            }
        }
        Value::Null => Ok(Value::Null),
        _ => Err(anyhow!("relationships() expects a Path")),
    }
}

/// Evaluate list/collection functions: SIZE, KEYS, HEAD, TAIL, LAST, LENGTH, NODES, RELATIONSHIPS
fn eval_list_function(name: &str, args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(anyhow!("{}() requires 1 argument", name));
    }
    match name {
        "SIZE" => eval_size(&args[0]),
        "KEYS" => eval_keys(&args[0]),
        "HEAD" => eval_head(&args[0]),
        "TAIL" => eval_tail(&args[0]),
        "LAST" => eval_last(&args[0]),
        "LENGTH" => eval_length(&args[0]),
        "NODES" => eval_nodes(&args[0]),
        "RELATIONSHIPS" => eval_relationships(&args[0]),
        _ => Err(anyhow!("Unknown list function: {}", name)),
    }
}

// ============================================================================
// Type conversion function helpers
// ============================================================================

fn eval_tointeger(arg: &Value) -> Result<Value> {
    match arg {
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(json!(i))
            } else if let Some(f) = n.as_f64() {
                Ok(json!(f as i64))
            } else {
                Ok(Value::Null)
            }
        }
        Value::String(s) => Ok(s.parse::<i64>().map(|i| json!(i)).unwrap_or(Value::Null)),
        Value::Null => Ok(Value::Null),
        _ => Ok(Value::Null),
    }
}

fn eval_tofloat(arg: &Value) -> Result<Value> {
    match arg {
        Value::Number(n) => Ok(n.as_f64().map(|f| json!(f)).unwrap_or(Value::Null)),
        Value::String(s) => Ok(s.parse::<f64>().map(|f| json!(f)).unwrap_or(Value::Null)),
        Value::Null => Ok(Value::Null),
        _ => Ok(Value::Null),
    }
}

fn eval_tostring(arg: &Value) -> Result<Value> {
    match arg {
        Value::String(s) => Ok(Value::String(s.clone())),
        Value::Number(n) => Ok(Value::String(n.to_string())),
        Value::Bool(b) => Ok(Value::String(b.to_string())),
        Value::Null => Ok(Value::Null),
        other => Ok(Value::String(other.to_string())),
    }
}

fn eval_toboolean(arg: &Value) -> Result<Value> {
    match arg {
        Value::Bool(b) => Ok(Value::Bool(*b)),
        Value::String(s) => {
            let lower = s.to_lowercase();
            if lower == "true" {
                Ok(Value::Bool(true))
            } else if lower == "false" {
                Ok(Value::Bool(false))
            } else {
                Ok(Value::Null)
            }
        }
        Value::Null => Ok(Value::Null),
        _ => Ok(Value::Null),
    }
}

/// Evaluate type conversion functions: TOINTEGER, TOFLOAT, TOSTRING, TOBOOLEAN
fn eval_type_function(name: &str, args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(anyhow!("{}() requires 1 argument", name));
    }
    match name {
        "TOINTEGER" | "TOINT" => eval_tointeger(&args[0]),
        "TOFLOAT" => eval_tofloat(&args[0]),
        "TOSTRING" => eval_tostring(&args[0]),
        "TOBOOLEAN" | "TOBOOL" => eval_toboolean(&args[0]),
        _ => Err(anyhow!("Unknown type function: {}", name)),
    }
}

// ============================================================================
// Math function helpers
// ============================================================================

fn eval_abs(arg: &Value) -> Result<Value> {
    match arg {
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(json!(i.abs()))
            } else if let Some(f) = n.as_f64() {
                Ok(json!(f.abs()))
            } else {
                Ok(Value::Null)
            }
        }
        Value::Null => Ok(Value::Null),
        _ => Err(anyhow!("abs() expects a number")),
    }
}

fn eval_ceil(arg: &Value) -> Result<Value> {
    match arg {
        Value::Number(n) => Ok(n.as_f64().map(|f| json!(f.ceil())).unwrap_or(arg.clone())),
        Value::Null => Ok(Value::Null),
        _ => Err(anyhow!("ceil() expects a number")),
    }
}

fn eval_floor(arg: &Value) -> Result<Value> {
    match arg {
        Value::Number(n) => Ok(n.as_f64().map(|f| json!(f.floor())).unwrap_or(arg.clone())),
        Value::Null => Ok(Value::Null),
        _ => Err(anyhow!("floor() expects a number")),
    }
}

fn eval_round(arg: &Value) -> Result<Value> {
    match arg {
        Value::Number(n) => Ok(n.as_f64().map(|f| json!(f.round())).unwrap_or(arg.clone())),
        Value::Null => Ok(Value::Null),
        _ => Err(anyhow!("round() expects a number")),
    }
}

fn eval_sqrt(arg: &Value) -> Result<Value> {
    match arg {
        Value::Number(n) => {
            if let Some(f) = n.as_f64() {
                if f < 0.0 {
                    Ok(Value::Null)
                } else {
                    Ok(json!(f.sqrt()))
                }
            } else {
                Ok(Value::Null)
            }
        }
        Value::Null => Ok(Value::Null),
        _ => Err(anyhow!("sqrt() expects a number")),
    }
}

fn eval_sign(arg: &Value) -> Result<Value> {
    match arg {
        Value::Number(n) => {
            if let Some(f) = n.as_f64() {
                if f > 0.0 {
                    Ok(json!(1))
                } else if f < 0.0 {
                    Ok(json!(-1))
                } else {
                    Ok(json!(0))
                }
            } else {
                Ok(Value::Null)
            }
        }
        Value::Null => Ok(Value::Null),
        _ => Err(anyhow!("sign() expects a number")),
    }
}

fn eval_log(arg: &Value) -> Result<Value> {
    match arg {
        Value::Number(n) => {
            if let Some(f) = n.as_f64() {
                Ok(json!(f.ln()))
            } else {
                Ok(Value::Null)
            }
        }
        Value::Null => Ok(Value::Null),
        _ => Err(anyhow!("log() expects a number")),
    }
}

fn eval_log10(arg: &Value) -> Result<Value> {
    match arg {
        Value::Number(n) => {
            if let Some(f) = n.as_f64() {
                Ok(json!(f.log10()))
            } else {
                Ok(Value::Null)
            }
        }
        Value::Null => Ok(Value::Null),
        _ => Err(anyhow!("log10() expects a number")),
    }
}

fn eval_exp(arg: &Value) -> Result<Value> {
    match arg {
        Value::Number(n) => {
            if let Some(f) = n.as_f64() {
                Ok(json!(f.exp()))
            } else {
                Ok(Value::Null)
            }
        }
        Value::Null => Ok(Value::Null),
        _ => Err(anyhow!("exp() expects a number")),
    }
}

fn eval_power(args: &[Value]) -> Result<Value> {
    if args.len() != 2 {
        return Err(anyhow!("power() requires 2 arguments"));
    }
    match (&args[0], &args[1]) {
        (Value::Number(base), Value::Number(exp)) => {
            if let (Some(b), Some(e)) = (base.as_f64(), exp.as_f64()) {
                Ok(json!(b.powf(e)))
            } else {
                Ok(Value::Null)
            }
        }
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        _ => Err(anyhow!("power() expects numeric arguments")),
    }
}

fn eval_sin(arg: &Value) -> Result<Value> {
    match arg {
        Value::Number(n) => {
            if let Some(f) = n.as_f64() {
                Ok(json!(f.sin()))
            } else {
                Ok(Value::Null)
            }
        }
        Value::Null => Ok(Value::Null),
        _ => Err(anyhow!("sin() expects a number")),
    }
}

fn eval_cos(arg: &Value) -> Result<Value> {
    match arg {
        Value::Number(n) => {
            if let Some(f) = n.as_f64() {
                Ok(json!(f.cos()))
            } else {
                Ok(Value::Null)
            }
        }
        Value::Null => Ok(Value::Null),
        _ => Err(anyhow!("cos() expects a number")),
    }
}

fn eval_tan(arg: &Value) -> Result<Value> {
    match arg {
        Value::Number(n) => {
            if let Some(f) = n.as_f64() {
                Ok(json!(f.tan()))
            } else {
                Ok(Value::Null)
            }
        }
        Value::Null => Ok(Value::Null),
        _ => Err(anyhow!("tan() expects a number")),
    }
}

/// Evaluate math functions: ABS, CEIL, FLOOR, ROUND, SQRT, SIGN, LOG, LOG10, EXP, POWER, SIN, COS, TAN
fn eval_math_function(name: &str, args: &[Value]) -> Result<Value> {
    match name {
        "ABS" => {
            if args.len() != 1 {
                return Err(anyhow!("ABS requires 1 argument"));
            }
            eval_abs(&args[0])
        }
        "CEIL" => {
            if args.len() != 1 {
                return Err(anyhow!("CEIL requires 1 argument"));
            }
            eval_ceil(&args[0])
        }
        "FLOOR" => {
            if args.len() != 1 {
                return Err(anyhow!("FLOOR requires 1 argument"));
            }
            eval_floor(&args[0])
        }
        "ROUND" => {
            if args.len() != 1 {
                return Err(anyhow!("ROUND requires 1 argument"));
            }
            eval_round(&args[0])
        }
        "SQRT" => {
            if args.len() != 1 {
                return Err(anyhow!("SQRT requires 1 argument"));
            }
            eval_sqrt(&args[0])
        }
        "SIGN" => {
            if args.len() != 1 {
                return Err(anyhow!("SIGN requires 1 argument"));
            }
            eval_sign(&args[0])
        }
        "LOG" => {
            if args.len() != 1 {
                return Err(anyhow!("LOG requires 1 argument"));
            }
            eval_log(&args[0])
        }
        "LOG10" => {
            if args.len() != 1 {
                return Err(anyhow!("LOG10 requires 1 argument"));
            }
            eval_log10(&args[0])
        }
        "EXP" => {
            if args.len() != 1 {
                return Err(anyhow!("EXP requires 1 argument"));
            }
            eval_exp(&args[0])
        }
        "POWER" | "POW" => eval_power(args),
        "SIN" => {
            if args.len() != 1 {
                return Err(anyhow!("SIN requires 1 argument"));
            }
            eval_sin(&args[0])
        }
        "COS" => {
            if args.len() != 1 {
                return Err(anyhow!("COS requires 1 argument"));
            }
            eval_cos(&args[0])
        }
        "TAN" => {
            if args.len() != 1 {
                return Err(anyhow!("TAN requires 1 argument"));
            }
            eval_tan(&args[0])
        }
        _ => Err(anyhow!("Unknown math function: {}", name)),
    }
}

// ============================================================================
// String function helpers
// ============================================================================

fn require_one_arg<'a>(name: &str, args: &'a [Value]) -> Result<&'a Value> {
    if args.len() != 1 {
        return Err(anyhow!("{}() requires 1 argument", name));
    }
    Ok(&args[0])
}

fn eval_toupper(args: &[Value]) -> Result<Value> {
    let arg = require_one_arg("toUpper", args)?;
    match arg {
        Value::String(s) => Ok(Value::String(s.to_uppercase())),
        Value::Null => Ok(Value::Null),
        _ => Err(anyhow!("toUpper() expects a string")),
    }
}

fn eval_tolower(args: &[Value]) -> Result<Value> {
    let arg = require_one_arg("toLower", args)?;
    match arg {
        Value::String(s) => Ok(Value::String(s.to_lowercase())),
        Value::Null => Ok(Value::Null),
        _ => Err(anyhow!("toLower() expects a string")),
    }
}

fn eval_trim(args: &[Value]) -> Result<Value> {
    let arg = require_one_arg("trim", args)?;
    match arg {
        Value::String(s) => Ok(Value::String(s.trim().to_string())),
        Value::Null => Ok(Value::Null),
        _ => Err(anyhow!("trim() expects a string")),
    }
}

fn eval_ltrim(args: &[Value]) -> Result<Value> {
    let arg = require_one_arg("ltrim", args)?;
    match arg {
        Value::String(s) => Ok(Value::String(s.trim_start().to_string())),
        Value::Null => Ok(Value::Null),
        _ => Err(anyhow!("ltrim() expects a string")),
    }
}

fn eval_rtrim(args: &[Value]) -> Result<Value> {
    let arg = require_one_arg("rtrim", args)?;
    match arg {
        Value::String(s) => Ok(Value::String(s.trim_end().to_string())),
        Value::Null => Ok(Value::Null),
        _ => Err(anyhow!("rtrim() expects a string")),
    }
}

fn eval_reverse(args: &[Value]) -> Result<Value> {
    let arg = require_one_arg("reverse", args)?;
    match arg {
        Value::String(s) => Ok(Value::String(s.chars().rev().collect())),
        Value::Array(arr) => Ok(Value::Array(arr.iter().rev().cloned().collect())),
        Value::Null => Ok(Value::Null),
        _ => Err(anyhow!("reverse() expects a string or list")),
    }
}

fn eval_replace(args: &[Value]) -> Result<Value> {
    if args.len() != 3 {
        return Err(anyhow!("replace() requires 3 arguments"));
    }
    match (&args[0], &args[1], &args[2]) {
        (Value::String(s), Value::String(search), Value::String(replacement)) => Ok(Value::String(
            s.replace(search.as_str(), replacement.as_str()),
        )),
        (Value::Null, _, _) => Ok(Value::Null),
        _ => Err(anyhow!("replace() expects string arguments")),
    }
}

fn eval_split(args: &[Value]) -> Result<Value> {
    if args.len() != 2 {
        return Err(anyhow!("split() requires 2 arguments"));
    }
    match (&args[0], &args[1]) {
        (Value::String(s), Value::String(delimiter)) => {
            let parts: Vec<Value> = s
                .split(delimiter.as_str())
                .map(|p| Value::String(p.to_string()))
                .collect();
            Ok(Value::Array(parts))
        }
        (Value::Null, _) => Ok(Value::Null),
        _ => Err(anyhow!("split() expects string arguments")),
    }
}

fn eval_substring(args: &[Value]) -> Result<Value> {
    if args.len() < 2 || args.len() > 3 {
        return Err(anyhow!("substring() requires 2 or 3 arguments"));
    }
    match &args[0] {
        Value::String(s) => {
            let start = args[1]
                .as_i64()
                .ok_or_else(|| anyhow!("substring() start must be an integer"))?
                as usize;
            let len = if args.len() == 3 {
                args[2]
                    .as_i64()
                    .ok_or_else(|| anyhow!("substring() length must be an integer"))?
                    as usize
            } else {
                s.len().saturating_sub(start)
            };
            let chars: Vec<char> = s.chars().collect();
            let end = (start + len).min(chars.len());
            let result: String = chars[start.min(chars.len())..end].iter().collect();
            Ok(Value::String(result))
        }
        Value::Null => Ok(Value::Null),
        _ => Err(anyhow!("substring() expects a string")),
    }
}

fn eval_left(args: &[Value]) -> Result<Value> {
    if args.len() != 2 {
        return Err(anyhow!("left() requires 2 arguments"));
    }
    match (&args[0], &args[1]) {
        (Value::String(s), Value::Number(n)) => {
            let len = n.as_i64().unwrap_or(0) as usize;
            Ok(Value::String(s.chars().take(len).collect()))
        }
        (Value::Null, _) => Ok(Value::Null),
        _ => Err(anyhow!("left() expects a string and integer")),
    }
}

fn eval_right(args: &[Value]) -> Result<Value> {
    if args.len() != 2 {
        return Err(anyhow!("right() requires 2 arguments"));
    }
    match (&args[0], &args[1]) {
        (Value::String(s), Value::Number(n)) => {
            let len = n.as_i64().unwrap_or(0) as usize;
            let chars: Vec<char> = s.chars().collect();
            let start = chars.len().saturating_sub(len);
            Ok(Value::String(chars[start..].iter().collect()))
        }
        (Value::Null, _) => Ok(Value::Null),
        _ => Err(anyhow!("right() expects a string and integer")),
    }
}

fn eval_lpad(args: &[Value]) -> Result<Value> {
    if args.len() < 2 || args.len() > 3 {
        return Err(anyhow!("lpad() requires 2 or 3 arguments"));
    }
    let s = match &args[0] {
        Value::String(s) => s,
        Value::Null => return Ok(Value::Null),
        _ => return Err(anyhow!("lpad() expects a string as first argument")),
    };
    let len = match &args[1] {
        Value::Number(n) => n.as_i64().unwrap_or(0) as usize,
        Value::Null => return Ok(Value::Null),
        _ => return Err(anyhow!("lpad() expects an integer as second argument")),
    };
    // Limit max length to prevent OOM
    if len > 1_000_000 {
        return Err(anyhow!("lpad() length exceeds maximum limit of 1,000,000"));
    }
    let pad_str = if args.len() == 3 {
        match &args[2] {
            Value::String(p) => p.as_str(),
            Value::Null => return Ok(Value::Null),
            _ => return Err(anyhow!("lpad() expects a string as third argument")),
        }
    } else {
        " "
    };

    let s_chars: Vec<char> = s.chars().collect();
    if s_chars.len() >= len {
        Ok(Value::String(s_chars.into_iter().take(len).collect()))
    } else {
        let pad_chars: Vec<char> = pad_str.chars().collect();
        if pad_chars.is_empty() {
            // If pad string is empty, we can't pad. Return truncated or original?
            // Postgres returns original string if pad is empty? No, it probably does nothing or errors.
            // Let's assume standard behavior: return original if len > s.len but pad is empty?
            // Actually, if pad is empty, we can't reach target length.
            // Return original string?
            return Ok(Value::String(s.clone()));
        }
        let needed = len - s_chars.len();
        let mut result = String::with_capacity(len);

        let full_pads = needed / pad_chars.len();
        let partial_pad = needed % pad_chars.len();

        for _ in 0..full_pads {
            result.push_str(pad_str);
        }
        result.extend(pad_chars.into_iter().take(partial_pad));
        result.push_str(s);

        Ok(Value::String(result))
    }
}

fn eval_rpad(args: &[Value]) -> Result<Value> {
    if args.len() < 2 || args.len() > 3 {
        return Err(anyhow!("rpad() requires 2 or 3 arguments"));
    }
    let s = match &args[0] {
        Value::String(s) => s,
        Value::Null => return Ok(Value::Null),
        _ => return Err(anyhow!("rpad() expects a string as first argument")),
    };
    let len = match &args[1] {
        Value::Number(n) => n.as_i64().unwrap_or(0) as usize,
        Value::Null => return Ok(Value::Null),
        _ => return Err(anyhow!("rpad() expects an integer as second argument")),
    };
    // Limit max length to prevent OOM
    if len > 1_000_000 {
        return Err(anyhow!("rpad() length exceeds maximum limit of 1,000,000"));
    }
    let pad_str = if args.len() == 3 {
        match &args[2] {
            Value::String(p) => p.as_str(),
            Value::Null => return Ok(Value::Null),
            _ => return Err(anyhow!("rpad() expects a string as third argument")),
        }
    } else {
        " "
    };

    let s_chars: Vec<char> = s.chars().collect();
    if s_chars.len() >= len {
        Ok(Value::String(s_chars.into_iter().take(len).collect()))
    } else {
        let mut result = String::from(s);
        let pad_chars: Vec<char> = pad_str.chars().collect();
        if pad_chars.is_empty() {
            return Ok(Value::String(s.clone()));
        }

        let needed = len - s_chars.len();
        let full_pads = needed / pad_chars.len();
        let partial_pad = needed % pad_chars.len();

        for _ in 0..full_pads {
            result.push_str(pad_str);
        }
        result.extend(pad_chars.into_iter().take(partial_pad));

        Ok(Value::String(result))
    }
}

/// Evaluate string functions: TOUPPER, TOLOWER, TRIM, LTRIM, RTRIM, REVERSE, REPLACE, SPLIT, SUBSTRING, LEFT, RIGHT, LPAD, RPAD
fn eval_string_function(name: &str, args: &[Value]) -> Result<Value> {
    match name {
        "TOUPPER" | "UPPER" => eval_toupper(args),
        "TOLOWER" | "LOWER" => eval_tolower(args),
        "TRIM" => eval_trim(args),
        "LTRIM" => eval_ltrim(args),
        "RTRIM" => eval_rtrim(args),
        "REVERSE" => eval_reverse(args),
        "REPLACE" => eval_replace(args),
        "SPLIT" => eval_split(args),
        "SUBSTRING" => eval_substring(args),
        "LEFT" => eval_left(args),
        "RIGHT" => eval_right(args),
        "LPAD" => eval_lpad(args),
        "RPAD" => eval_rpad(args),
        _ => Err(anyhow!("Unknown string function: {}", name)),
    }
}

/// Evaluate the RANGE function
fn eval_range_function(args: &[Value]) -> Result<Value> {
    if args.len() < 2 || args.len() > 3 {
        return Err(anyhow!("range() requires 2 or 3 arguments"));
    }
    let start = args[0]
        .as_i64()
        .ok_or_else(|| anyhow!("range() start must be an integer"))?;
    let end = args[1]
        .as_i64()
        .ok_or_else(|| anyhow!("range() end must be an integer"))?;
    let step = if args.len() == 3 {
        args[2]
            .as_i64()
            .ok_or_else(|| anyhow!("range() step must be an integer"))?
    } else {
        1
    };
    if step == 0 {
        return Err(anyhow!("range() step cannot be zero"));
    }
    let mut result = Vec::new();
    let mut i = start;
    if step > 0 {
        while i <= end {
            result.push(json!(i));
            i += step;
        }
    } else {
        while i >= end {
            result.push(json!(i));
            i += step;
        }
    }
    Ok(Value::Array(result))
}

/// Evaluate a built-in scalar function.
///
/// This handles functions like COALESCE, NULLIF, SIZE, KEYS, HEAD, TAIL, etc.
/// Functions that require argument evaluation (like COALESCE) take pre-evaluated args.
pub fn eval_scalar_function(name: &str, args: &[Value]) -> Result<Value> {
    let name_upper = name.to_uppercase();

    // Null-handling functions
    match name_upper.as_str() {
        "COALESCE" => {
            for arg in args {
                if !arg.is_null() {
                    return Ok(arg.clone());
                }
            }
            return Ok(Value::Null);
        }
        "NULLIF" => {
            if args.len() != 2 {
                return Err(anyhow!("NULLIF requires 2 arguments"));
            }
            return if args[0] == args[1] {
                Ok(Value::Null)
            } else {
                Ok(args[0].clone())
            };
        }
        _ => {}
    }

    // List/Collection functions
    if matches!(
        name_upper.as_str(),
        "SIZE" | "KEYS" | "HEAD" | "TAIL" | "LAST" | "LENGTH" | "NODES" | "RELATIONSHIPS"
    ) {
        return eval_list_function(&name_upper, args);
    }

    // Type conversion functions
    if matches!(
        name_upper.as_str(),
        "TOINTEGER" | "TOINT" | "TOFLOAT" | "TOSTRING" | "TOBOOLEAN" | "TOBOOL"
    ) {
        return eval_type_function(&name_upper, args);
    }

    // Math functions
    if matches!(
        name_upper.as_str(),
        "ABS"
            | "CEIL"
            | "FLOOR"
            | "ROUND"
            | "SQRT"
            | "SIGN"
            | "LOG"
            | "LOG10"
            | "EXP"
            | "POWER"
            | "POW"
            | "SIN"
            | "COS"
            | "TAN"
    ) {
        return eval_math_function(&name_upper, args);
    }

    // String functions
    if matches!(
        name_upper.as_str(),
        "TOUPPER"
            | "UPPER"
            | "TOLOWER"
            | "LOWER"
            | "TRIM"
            | "LTRIM"
            | "RTRIM"
            | "REVERSE"
            | "REPLACE"
            | "SPLIT"
            | "SUBSTRING"
            | "LEFT"
            | "RIGHT"
            | "LPAD"
            | "RPAD"
    ) {
        return eval_string_function(&name_upper, args);
    }

    // Date/Time functions
    if matches!(
        name_upper.as_str(),
        "DATE"
            | "TIME"
            | "DATETIME"
            | "DURATION"
            | "YEAR"
            | "MONTH"
            | "DAY"
            | "HOUR"
            | "MINUTE"
            | "SECOND"
    ) {
        return eval_datetime_function(&name_upper, args);
    }

    // Range function
    if name_upper == "RANGE" {
        return eval_range_function(args);
    }

    if name_upper == "UNI.VALIDAT" || name_upper == "VALIDAT" {
        return eval_valid_at(args);
    }

    if name_upper == "VECTOR_DISTANCE" {
        if args.len() < 2 || args.len() > 3 {
            return Err(anyhow!("vector_distance requires 2 or 3 arguments"));
        }
        let metric = if args.len() == 3 {
            args[2].as_str().ok_or(anyhow!("metric must be string"))?
        } else {
            "cosine"
        };
        return eval_vector_distance(&args[0], &args[1], metric);
    }

    Err(anyhow!("Function {} not implemented or is aggregate", name))
}

/// Evaluate uni.validAt(node, start_prop, end_prop, time)
///
/// Checks if a node/edge was valid at a given point in time using half-open interval
/// semantics: `[valid_from, valid_to)` where `valid_from <= time < valid_to`.
///
/// If `valid_to` is NULL or missing, the interval is open-ended (valid indefinitely).
/// If `valid_from` is NULL or missing, the entity is considered invalid.
fn eval_valid_at(args: &[Value]) -> Result<Value> {
    if args.len() != 4 {
        return Err(anyhow!(
            "validAt requires 4 arguments: node, start_prop, end_prop, time"
        ));
    }

    let node_map = match &args[0] {
        Value::Object(map) => map,
        Value::Null => return Ok(Value::Bool(false)),
        _ => {
            return Err(anyhow!(
                "validAt expects a Node or Edge (Object) as first argument"
            ));
        }
    };

    let start_prop = args[1]
        .as_str()
        .ok_or_else(|| anyhow!("start_prop must be a string"))?;
    let end_prop = args[2]
        .as_str()
        .ok_or_else(|| anyhow!("end_prop must be a string"))?;

    let time_str = match &args[3] {
        Value::String(s) => s,
        _ => return Err(anyhow!("time argument must be a datetime string")),
    };

    let query_time = parse_datetime_utc(time_str)
        .map_err(|_| anyhow!("Invalid query time format: {}", time_str))?;

    let valid_from_val = node_map.get(start_prop);
    let valid_from = match valid_from_val {
        Some(Value::String(s)) => parse_datetime_utc(s)
            .map_err(|_| anyhow!("Invalid datetime in property {}: {}", start_prop, s))?,
        Some(Value::Null) | None => return Ok(Value::Bool(false)),
        _ => return Err(anyhow!("Property {} must be a datetime string", start_prop)),
    };

    let valid_to_val = node_map.get(end_prop);
    let valid_to = match valid_to_val {
        Some(Value::String(s)) => Some(
            parse_datetime_utc(s)
                .map_err(|_| anyhow!("Invalid datetime in property {}: {}", end_prop, s))?,
        ),
        Some(Value::Null) | None => None,
        _ => {
            return Err(anyhow!(
                "Property {} must be a datetime string or null",
                end_prop
            ));
        }
    };

    // Half-open interval: [valid_from, valid_to)
    let is_valid = valid_from <= query_time && valid_to.map(|vt| query_time < vt).unwrap_or(true);

    Ok(Value::Bool(is_valid))
}

/// Evaluate vector similarity between two vectors (cosine similarity).
pub fn eval_vector_similarity(v1: &Value, v2: &Value) -> Result<Value> {
    let (arr1, arr2) = match (v1, v2) {
        (Value::Array(a1), Value::Array(a2)) => (a1, a2),
        _ => return Err(anyhow!("vector_similarity arguments must be arrays")),
    };

    if arr1.len() != arr2.len() {
        return Err(anyhow!(
            "Vector dimensions mismatch: {} vs {}",
            arr1.len(),
            arr2.len()
        ));
    }

    let mut dot = 0.0;
    let mut norm1_sq = 0.0;
    let mut norm2_sq = 0.0;

    for (v1_elem, v2_elem) in arr1.iter().zip(arr2.iter()) {
        let f1 = v1_elem
            .as_f64()
            .ok_or_else(|| anyhow!("Vector element not a number"))?;
        let f2 = v2_elem
            .as_f64()
            .ok_or_else(|| anyhow!("Vector element not a number"))?;
        dot += f1 * f2;
        norm1_sq += f1 * f1;
        norm2_sq += f2 * f2;
    }

    let mag1 = norm1_sq.sqrt();
    let mag2 = norm2_sq.sqrt();

    let sim = if mag1 == 0.0 || mag2 == 0.0 {
        0.0
    } else {
        dot / (mag1 * mag2)
    };

    Ok(json!(sim))
}

/// Evaluate vector distance between two vectors.
pub fn eval_vector_distance(v1: &Value, v2: &Value, metric: &str) -> Result<Value> {
    let (arr1, arr2) = match (v1, v2) {
        (Value::Array(a1), Value::Array(a2)) => (a1, a2),
        _ => return Err(anyhow!("vector_distance arguments must be arrays")),
    };

    if arr1.len() != arr2.len() {
        return Err(anyhow!(
            "Vector dimensions mismatch: {} vs {}",
            arr1.len(),
            arr2.len()
        ));
    }

    // Helper to get f64 iterator
    let iter1 = arr1
        .iter()
        .map(|v| v.as_f64().ok_or(anyhow!("Vector element not a number")));
    let iter2 = arr2
        .iter()
        .map(|v| v.as_f64().ok_or(anyhow!("Vector element not a number")));

    match metric.to_lowercase().as_str() {
        "cosine" => {
            // Cosine distance = 1 - cosine similarity
            let mut dot = 0.0;
            let mut norm1_sq = 0.0;
            let mut norm2_sq = 0.0;

            for (r1, r2) in iter1.zip(iter2) {
                let f1 = r1?;
                let f2 = r2?;
                dot += f1 * f2;
                norm1_sq += f1 * f1;
                norm2_sq += f2 * f2;
            }

            let mag1 = norm1_sq.sqrt();
            let mag2 = norm2_sq.sqrt();

            if mag1 == 0.0 || mag2 == 0.0 {
                Ok(json!(1.0))
            } else {
                let sim = dot / (mag1 * mag2);
                // Clamp to [-1, 1] to avoid numerical errors
                let sim = sim.clamp(-1.0, 1.0);
                Ok(json!(1.0 - sim))
            }
        }
        "euclidean" | "l2" => {
            let mut sum_sq_diff = 0.0;
            for (r1, r2) in iter1.zip(iter2) {
                let f1 = r1?;
                let f2 = r2?;
                let diff = f1 - f2;
                sum_sq_diff += diff * diff;
            }
            Ok(json!(sum_sq_diff.sqrt()))
        }
        "dot" | "inner_product" => {
            let mut dot = 0.0;
            for (r1, r2) in iter1.zip(iter2) {
                let f1 = r1?;
                let f2 = r2?;
                dot += f1 * f2;
            }
            Ok(json!(1.0 - dot))
        }
        _ => Err(anyhow!("Unknown metric: {}", metric)),
    }
}

/// Check if a function name is a known scalar function (not aggregate).
pub fn is_scalar_function(name: &str) -> bool {
    let name_upper = name.to_uppercase();
    matches!(
        name_upper.as_str(),
        "COALESCE"
            | "NULLIF"
            | "SIZE"
            | "KEYS"
            | "HEAD"
            | "TAIL"
            | "LAST"
            | "LENGTH"
            | "NODES"
            | "RELATIONSHIPS"
            | "TOINTEGER"
            | "TOINT"
            | "TOFLOAT"
            | "TOSTRING"
            | "TOBOOLEAN"
            | "TOBOOL"
            | "ABS"
            | "CEIL"
            | "FLOOR"
            | "ROUND"
            | "SQRT"
            | "SIGN"
            | "LOG"
            | "LOG10"
            | "EXP"
            | "POWER"
            | "POW"
            | "SIN"
            | "COS"
            | "TAN"
            | "TOUPPER"
            | "UPPER"
            | "TOLOWER"
            | "LOWER"
            | "TRIM"
            | "LTRIM"
            | "RTRIM"
            | "REVERSE"
            | "REPLACE"
            | "SPLIT"
            | "SUBSTRING"
            | "LEFT"
            | "RIGHT"
            | "LPAD"
            | "RPAD"
            | "RANGE"
            | "UNI.VALIDAT"
            | "VALIDAT"
            | "VECTOR_SIMILARITY"
            | "VECTOR_DISTANCE"
            | "DATE"
            | "TIME"
            | "DATETIME"
            | "DURATION"
            | "YEAR"
            | "MONTH"
            | "DAY"
            | "HOUR"
            | "MINUTE"
            | "SECOND"
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_binary_op_eq() {
        assert_eq!(
            eval_binary_op(&json!(1), &Operator::Eq, &json!(1)).unwrap(),
            Value::Bool(true)
        );
        assert_eq!(
            eval_binary_op(&json!(1), &Operator::Eq, &json!(2)).unwrap(),
            Value::Bool(false)
        );
    }

    #[test]
    fn test_binary_op_comparison() {
        assert_eq!(
            eval_binary_op(&json!(5), &Operator::Gt, &json!(3)).unwrap(),
            Value::Bool(true)
        );
        assert_eq!(
            eval_binary_op(&json!(5), &Operator::Lt, &json!(3)).unwrap(),
            Value::Bool(false)
        );
    }

    #[test]
    fn test_binary_op_contains() {
        assert_eq!(
            eval_binary_op(&json!("hello world"), &Operator::Contains, &json!("world")).unwrap(),
            Value::Bool(true)
        );
    }

    #[test]
    fn test_scalar_function_size() {
        assert_eq!(
            eval_scalar_function("SIZE", &[json!([1, 2, 3])]).unwrap(),
            json!(3)
        );
    }

    #[test]
    fn test_scalar_function_head() {
        assert_eq!(
            eval_scalar_function("HEAD", &[json!([1, 2, 3])]).unwrap(),
            json!(1)
        );
    }

    #[test]
    fn test_scalar_function_coalesce() {
        assert_eq!(
            eval_scalar_function("COALESCE", &[Value::Null, json!(1), json!(2)]).unwrap(),
            json!(1)
        );
    }

    #[test]
    fn test_vector_similarity() {
        let v1 = json!([1.0, 0.0]);
        let v2 = json!([1.0, 0.0]);
        let result = eval_vector_similarity(&v1, &v2).unwrap();
        assert_eq!(result.as_f64().unwrap(), 1.0);
    }
}
