// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! CRDT function implementations for vectorized execution.
//!
//! This module provides CRDT (Conflict-free Replicated Data Types) function
//! implementations that can be used in Cypher queries.

use anyhow::{Result, anyhow};
use serde_json::{Value, json};
use uni_crdt::{Crdt, GCounter, GSet, LWWMap, LWWRegister, ORSet, Rga};

// ============================================================================
// GCounter helper functions
// ============================================================================

fn eval_gcounter_new() -> Result<Value> {
    Ok(serde_json::to_value(Crdt::GCounter(GCounter::new()))?)
}

fn eval_gcounter_increment(args: &[Value]) -> Result<Value> {
    if args.len() < 2 || args.len() > 3 {
        return Err(anyhow!(
            "crdt.increment requires 2 or 3 arguments: (crdt, value) or (crdt, actor, value)"
        ));
    }
    let mut crdt: Crdt = serde_json::from_value(args[0].clone())
        .map_err(|_| anyhow!("First argument must be a valid CRDT"))?;

    let (actor, value) = if args.len() == 2 {
        ("default".to_string(), &args[1])
    } else {
        (args[1].as_str().unwrap_or("default").to_string(), &args[2])
    };

    let inc = value
        .as_u64()
        .ok_or_else(|| anyhow!("Increment value must be a positive integer"))?;

    if let Crdt::GCounter(ref mut gc) = crdt {
        gc.increment(&actor, inc);
        Ok(serde_json::to_value(crdt)?)
    } else {
        Err(anyhow!("crdt.increment expects a GCounter"))
    }
}

fn eval_gcounter_value(args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(anyhow!("crdt.value requires 1 argument"));
    }
    let crdt: Crdt = serde_json::from_value(args[0].clone())
        .map_err(|_| anyhow!("Argument must be a valid CRDT"))?;

    match crdt {
        Crdt::GCounter(gc) => Ok(json!(gc.value())),
        _ => Err(anyhow!("crdt.value expects a GCounter")),
    }
}

// ============================================================================
// GSet helper functions
// ============================================================================

fn eval_gset_new() -> Result<Value> {
    Ok(serde_json::to_value(Crdt::GSet(GSet::new()))?)
}

fn eval_gset_add(args: &[Value]) -> Result<Value> {
    if args.len() != 2 {
        return Err(anyhow!(
            "crdt.gset_add requires 2 arguments: (crdt, element)"
        ));
    }
    let mut crdt: Crdt = serde_json::from_value(args[0].clone())
        .map_err(|_| anyhow!("First argument must be a valid CRDT"))?;
    let elem_str = args[1]
        .as_str()
        .ok_or_else(|| anyhow!("Element must be a string"))?
        .to_string();

    if let Crdt::GSet(ref mut gs) = crdt {
        gs.add(elem_str);
        Ok(serde_json::to_value(crdt)?)
    } else {
        Err(anyhow!("crdt.gset_add expects a GSet"))
    }
}

// ============================================================================
// ORSet helper functions
// ============================================================================

fn eval_orset_new() -> Result<Value> {
    Ok(serde_json::to_value(Crdt::ORSet(ORSet::new()))?)
}

fn eval_orset_add(args: &[Value]) -> Result<Value> {
    if args.len() != 2 {
        return Err(anyhow!(
            "crdt.orset_add requires 2 arguments: (crdt, element)"
        ));
    }
    let mut crdt: Crdt = serde_json::from_value(args[0].clone())
        .map_err(|_| anyhow!("First argument must be a valid CRDT"))?;
    let elem_str = args[1]
        .as_str()
        .ok_or_else(|| anyhow!("Element must be a string"))?
        .to_string();

    if let Crdt::ORSet(ref mut os) = crdt {
        os.add(elem_str);
        Ok(serde_json::to_value(crdt)?)
    } else {
        Err(anyhow!("crdt.orset_add expects an ORSet"))
    }
}

fn eval_orset_remove(args: &[Value]) -> Result<Value> {
    if args.len() != 2 {
        return Err(anyhow!(
            "crdt.orset_remove requires 2 arguments: (crdt, element)"
        ));
    }
    let mut crdt: Crdt = serde_json::from_value(args[0].clone())
        .map_err(|_| anyhow!("First argument must be a valid CRDT"))?;
    let elem_str = args[1]
        .as_str()
        .ok_or_else(|| anyhow!("Element must be a string"))?
        .to_string();

    if let Crdt::ORSet(ref mut os) = crdt {
        os.remove(&elem_str);
        Ok(serde_json::to_value(crdt)?)
    } else {
        Err(anyhow!("crdt.orset_remove expects an ORSet"))
    }
}

// ============================================================================
// Set common helper functions (GSet and ORSet)
// ============================================================================

fn eval_set_contains(args: &[Value]) -> Result<Value> {
    if args.len() != 2 {
        return Err(anyhow!(
            "crdt.contains requires 2 arguments: (crdt, element)"
        ));
    }
    let crdt: Crdt = serde_json::from_value(args[0].clone())
        .map_err(|_| anyhow!("First argument must be a valid CRDT"))?;
    let elem_str = args[1]
        .as_str()
        .ok_or_else(|| anyhow!("Element must be a string"))?
        .to_string();

    match crdt {
        Crdt::GSet(gs) => Ok(json!(gs.contains(&elem_str))),
        Crdt::ORSet(os) => Ok(json!(os.contains(&elem_str))),
        _ => Err(anyhow!("crdt.contains expects a Set type (GSet or ORSet)")),
    }
}

fn eval_set_elements(args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(anyhow!("crdt.elements requires 1 argument"));
    }
    let crdt: Crdt = serde_json::from_value(args[0].clone())
        .map_err(|_| anyhow!("First argument must be a valid CRDT"))?;

    match crdt {
        Crdt::GSet(gs) => Ok(json!(gs.elements().collect::<Vec<_>>())),
        Crdt::ORSet(os) => Ok(json!(os.elements())),
        _ => Err(anyhow!("crdt.elements expects a Set type (GSet or ORSet)")),
    }
}

// ============================================================================
// LWWRegister helper functions
// ============================================================================

fn eval_lww_new(args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(anyhow!("crdt.lww requires 1 argument: (value)"));
    }
    let val = args[0].clone();
    let ts = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
    Ok(serde_json::to_value(Crdt::LWWRegister(LWWRegister::new(
        val, ts,
    )))?)
}

fn eval_lww_set(args: &[Value]) -> Result<Value> {
    if args.len() != 2 {
        return Err(anyhow!("crdt.lww_set requires 2 arguments: (crdt, value)"));
    }
    let mut crdt: Crdt = serde_json::from_value(args[0].clone())
        .map_err(|_| anyhow!("First argument must be a valid CRDT"))?;
    let val = args[1].clone();
    let ts = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);

    if let Crdt::LWWRegister(ref mut reg) = crdt {
        reg.set(val, ts);
        Ok(serde_json::to_value(crdt)?)
    } else {
        Err(anyhow!("crdt.lww_set expects an LWWRegister"))
    }
}

fn eval_lww_get(args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(anyhow!("crdt.lww_get requires 1 argument"));
    }
    let crdt: Crdt = serde_json::from_value(args[0].clone())
        .map_err(|_| anyhow!("Argument must be a valid CRDT"))?;

    match crdt {
        Crdt::LWWRegister(reg) => Ok(reg.get().clone()),
        _ => Err(anyhow!("crdt.lww_get expects an LWWRegister")),
    }
}

// ============================================================================
// LWWMap helper functions
// ============================================================================

fn eval_lww_map_new() -> Result<Value> {
    Ok(serde_json::to_value(Crdt::LWWMap(LWWMap::new()))?)
}

fn eval_map_put(args: &[Value]) -> Result<Value> {
    if args.len() != 3 {
        return Err(anyhow!(
            "crdt.map_put requires 3 arguments: (crdt, key, value)"
        ));
    }
    let mut crdt: Crdt = serde_json::from_value(args[0].clone())
        .map_err(|_| anyhow!("First argument must be a valid CRDT"))?;
    let key_str = args[1]
        .as_str()
        .ok_or_else(|| anyhow!("Key must be a string"))?
        .to_string();
    let val = args[2].clone();
    let ts = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);

    if let Crdt::LWWMap(ref mut map) = crdt {
        map.put(key_str, val, ts);
        Ok(serde_json::to_value(crdt)?)
    } else {
        Err(anyhow!("crdt.map_put expects an LWWMap"))
    }
}

fn eval_map_get(args: &[Value]) -> Result<Value> {
    if args.len() != 2 {
        return Err(anyhow!("crdt.map_get requires 2 arguments: (crdt, key)"));
    }
    let crdt: Crdt = serde_json::from_value(args[0].clone())
        .map_err(|_| anyhow!("First argument must be a valid CRDT"))?;
    let key_str = args[1]
        .as_str()
        .ok_or_else(|| anyhow!("Key must be a string"))?;

    if let Crdt::LWWMap(map) = crdt {
        Ok(map
            .get(&key_str.to_string())
            .cloned()
            .unwrap_or(Value::Null))
    } else {
        Err(anyhow!("crdt.map_get expects an LWWMap"))
    }
}

// ============================================================================
// RGA helper functions
// ============================================================================

fn eval_rga_new() -> Result<Value> {
    Ok(serde_json::to_value(Crdt::Rga(Rga::new()))?)
}

fn eval_rga_to_list(args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(anyhow!("crdt.rga_to_list requires 1 argument"));
    }
    let crdt: Crdt = serde_json::from_value(args[0].clone())
        .map_err(|_| anyhow!("Argument must be a valid CRDT"))?;

    if let Crdt::Rga(rga) = crdt {
        let vec = rga.to_vec();
        Ok(json!(vec))
    } else {
        Err(anyhow!("crdt.rga_to_list expects an RGA"))
    }
}

// ============================================================================
// Main dispatcher
// ============================================================================

/// Execute a CRDT function with the given name and evaluated arguments.
/// Returns Ok(Some(result)) if the function was handled, Ok(None) if not a CRDT function.
pub fn eval_crdt_function(name: &str, args: &[Value]) -> Result<Option<Value>> {
    let result = match name {
        // GCounter functions
        "crdt.gcounter" => eval_gcounter_new()?,
        "crdt.increment" => eval_gcounter_increment(args)?,
        "crdt.value" => eval_gcounter_value(args)?,
        // GSet functions
        "crdt.gset" => eval_gset_new()?,
        "crdt.gset_add" => eval_gset_add(args)?,
        // ORSet functions
        "crdt.orset" => eval_orset_new()?,
        "crdt.orset_add" => eval_orset_add(args)?,
        "crdt.orset_remove" => eval_orset_remove(args)?,
        // Set common functions
        "crdt.contains" => eval_set_contains(args)?,
        "crdt.elements" => eval_set_elements(args)?,
        // LWWRegister functions
        "crdt.lww" => eval_lww_new(args)?,
        "crdt.lww_set" => eval_lww_set(args)?,
        "crdt.lww_get" => eval_lww_get(args)?,
        // LWWMap functions
        "crdt.lww_map" => eval_lww_map_new()?,
        "crdt.map_put" => eval_map_put(args)?,
        "crdt.map_get" => eval_map_get(args)?,
        // RGA functions
        "crdt.rga" => eval_rga_new()?,
        "crdt.rga_to_list" => eval_rga_to_list(args)?,
        // Not a CRDT function
        _ => return Ok(None),
    };
    Ok(Some(result))
}

/// Check if a function name is a CRDT function.
pub fn is_crdt_function(name: &str) -> bool {
    name.starts_with("crdt.")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_gcounter_create() {
        let result = eval_crdt_function("crdt.gcounter", &[]).unwrap().unwrap();
        assert!(result.is_object());
    }

    #[test]
    fn test_gset_add() {
        let gset = eval_crdt_function("crdt.gset", &[]).unwrap().unwrap();
        let result = eval_crdt_function("crdt.gset_add", &[gset, json!("item1")])
            .unwrap()
            .unwrap();
        assert!(result.is_object());

        // Check contains
        let contains = eval_crdt_function("crdt.contains", &[result, json!("item1")])
            .unwrap()
            .unwrap();
        assert_eq!(contains, json!(true));
    }

    #[test]
    fn test_lww_register() {
        let reg = eval_crdt_function("crdt.lww", &[json!("initial")])
            .unwrap()
            .unwrap();
        let updated = eval_crdt_function("crdt.lww_set", &[reg, json!("updated")])
            .unwrap()
            .unwrap();
        let value = eval_crdt_function("crdt.lww_get", &[updated])
            .unwrap()
            .unwrap();
        assert_eq!(value, json!("updated"));
    }
}
