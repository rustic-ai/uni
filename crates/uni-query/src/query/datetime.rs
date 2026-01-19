// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use anyhow::{Result, anyhow};
use chrono::{DateTime, Datelike, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, Timelike, Utc};
use serde_json::{Value, json};

/// Parse a datetime string into a `DateTime<Utc>`.
///
/// Supports multiple formats:
/// - RFC3339 (e.g., "2023-01-01T00:00:00Z")
/// - "%Y-%m-%d %H:%M:%S %z" (e.g., "2023-01-01 00:00:00 +0000")
/// - "%Y-%m-%d %H:%M:%S" naive (assumed UTC)
///
/// This is the canonical datetime parsing function for temporal operations
/// like `validAt`. Using a single implementation ensures consistent behavior.
pub fn parse_datetime_utc(s: &str) -> Result<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(s)
        .map(|dt: DateTime<FixedOffset>| dt.with_timezone(&Utc))
        .or_else(|_| {
            DateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S %z")
                .map(|dt: DateTime<FixedOffset>| dt.with_timezone(&Utc))
        })
        .or_else(|_| {
            NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
                .map(|ndt| DateTime::<Utc>::from_naive_utc_and_offset(ndt, Utc))
        })
        .map_err(|_| anyhow!("Invalid datetime format: {}", s))
}

/// Evaluate date/time functions: DATE, TIME, DATETIME, DURATION, and extraction functions.
pub fn eval_datetime_function(name: &str, args: &[Value]) -> Result<Value> {
    match name {
        "DATE" => eval_date(args),
        "TIME" => eval_time(args),
        "DATETIME" => eval_datetime(args),
        "DURATION" => eval_duration(args),
        "YEAR" => eval_extract(args, Component::Year),
        "MONTH" => eval_extract(args, Component::Month),
        "DAY" => eval_extract(args, Component::Day),
        "HOUR" => eval_extract(args, Component::Hour),
        "MINUTE" => eval_extract(args, Component::Minute),
        "SECOND" => eval_extract(args, Component::Second),
        _ => Err(anyhow!("Unknown datetime function: {}", name)),
    }
}

enum Component {
    Year,
    Month,
    Day,
    Hour,
    Minute,
    Second,
}

fn eval_date(args: &[Value]) -> Result<Value> {
    if args.is_empty() {
        // Current date
        let now = Utc::now();
        return Ok(Value::String(now.date_naive().to_string()));
    }
    match &args[0] {
        Value::String(s) => {
            let date = NaiveDate::parse_from_str(s, "%Y-%m-%d")
                .or_else(|_| {
                    NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S").map(|dt| dt.date())
                }) // Handle timestamp string
                .map_err(|e| anyhow!("Invalid date format: {}", e))?;
            Ok(Value::String(date.to_string()))
        }
        Value::Null => Ok(Value::Null),
        _ => Err(anyhow!("date() expects a string argument")),
    }
}

fn eval_time(args: &[Value]) -> Result<Value> {
    if args.is_empty() {
        // Current time
        let now = Utc::now();
        return Ok(Value::String(now.time().to_string()));
    }
    match &args[0] {
        Value::String(s) => {
            // Try parsing just time, or extract time from datetime string
            let time = NaiveTime::parse_from_str(s, "%H:%M:%S")
                .or_else(|_| NaiveTime::parse_from_str(s, "%H:%M:%S%.f"))
                .or_else(|_| {
                    NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S").map(|dt| dt.time())
                })
                .or_else(|_| {
                    NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f").map(|dt| dt.time())
                })
                .or_else(|_| DateTime::parse_from_rfc3339(s).map(|dt| dt.time()))
                .map_err(|_| anyhow!("Invalid time format"))?;
            Ok(Value::String(time.to_string()))
        }
        Value::Null => Ok(Value::Null),
        _ => Err(anyhow!("time() expects a string argument")),
    }
}

fn eval_datetime(args: &[Value]) -> Result<Value> {
    if args.is_empty() {
        // Current datetime
        return Ok(Value::String(Utc::now().to_rfc3339()));
    }
    match &args[0] {
        Value::String(s) => {
            // Validate parsing
            let _ = DateTime::parse_from_rfc3339(s)
                .or_else(|_| DateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S %z"))
                .or_else(|_| {
                    // Try naive and assume UTC
                    NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
                        .map(|ndt| DateTime::<Utc>::from_naive_utc_and_offset(ndt, Utc).into())
                })
                .map_err(|e| anyhow!("Invalid datetime format: {}", e))?;
            Ok(Value::String(s.clone()))
        }
        Value::Null => Ok(Value::Null),
        _ => Err(anyhow!("datetime() expects a string argument")),
    }
}

fn eval_duration(args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(anyhow!("duration() requires 1 argument"));
    }
    match &args[0] {
        Value::String(s) => {
            // Basic ISO 8601 duration parsing (P1DT1H etc) is complex.
            // For now, let's support a simple format like "{days}d {hours}h" or just pass string through if valid?
            // Actually, `chrono` has `Duration` but it doesn't parse ISO strings directly.
            // We can return the string as a placeholder for a real Duration type implementation later.
            Ok(Value::String(s.clone()))
        }
        Value::Object(map) => {
            // duration({days: 1, hours: 2})
            // Serialize back to JSON for now
            Ok(Value::Object(map.clone()))
        }
        Value::Null => Ok(Value::Null),
        _ => Err(anyhow!("duration() expects a string or map")),
    }
}

fn eval_extract(args: &[Value], component: Component) -> Result<Value> {
    if args.len() != 1 {
        return Err(anyhow!("Extract function requires 1 argument"));
    }
    match &args[0] {
        Value::String(s) => {
            // Try parsing as DateTime, then NaiveDateTime, then NaiveDate (for Year/Month/Day), then NaiveTime (for Hour/Min/Sec)
            if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
                return Ok(json!(extract_from_datetime(&dt, &component)));
            }
            if let Ok(dt) = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S") {
                return Ok(json!(extract_from_naive_datetime(&dt, &component)));
            }

            match component {
                Component::Year | Component::Month | Component::Day => {
                    if let Ok(d) = NaiveDate::parse_from_str(s, "%Y-%m-%d") {
                        return Ok(json!(match component {
                            Component::Year => d.year(),
                            Component::Month => d.month() as i32,
                            Component::Day => d.day() as i32,
                            _ => unreachable!(),
                        }));
                    }
                }
                Component::Hour | Component::Minute | Component::Second => {
                    if let Ok(t) = NaiveTime::parse_from_str(s, "%H:%M:%S") {
                        return Ok(json!(match component {
                            Component::Hour => t.hour() as i32,
                            Component::Minute => t.minute() as i32,
                            Component::Second => t.second() as i32,
                            _ => unreachable!(),
                        }));
                    }
                }
            }

            Err(anyhow!("Could not parse date/time string for extraction"))
        }
        Value::Null => Ok(Value::Null),
        _ => Err(anyhow!("Extract function expects a string argument")),
    }
}

fn extract_from_datetime<T: Datelike + Timelike>(dt: &T, component: &Component) -> i32 {
    match component {
        Component::Year => dt.year(),
        Component::Month => dt.month() as i32,
        Component::Day => dt.day() as i32,
        Component::Hour => dt.hour() as i32,
        Component::Minute => dt.minute() as i32,
        Component::Second => dt.second() as i32,
    }
}

fn extract_from_naive_datetime(dt: &NaiveDateTime, component: &Component) -> i32 {
    match component {
        Component::Year => dt.year(),
        Component::Month => dt.month() as i32,
        Component::Day => dt.day() as i32,
        Component::Hour => dt.hour() as i32,
        Component::Minute => dt.minute() as i32,
        Component::Second => dt.second() as i32,
    }
}
