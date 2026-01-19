// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Arrow type conversion utilities for reducing cognitive complexity.
//!
//! This module provides shared helper functions and macros for converting
//! between Arrow arrays and JSON Values, reducing code duplication across
//! vertex.rs, delta.rs, and executor.rs.

use anyhow::{Result, anyhow};
use arrow_array::builder::{
    BinaryBuilder, BooleanBuilder, FixedSizeBinaryBuilder, FixedSizeListBuilder, Float32Builder,
    Float64Builder, Int32Builder, Int64Builder, ListBuilder, StringBuilder, StructBuilder,
    UInt64Builder,
};
use arrow_array::{
    Array, ArrayRef, BooleanArray, FixedSizeListArray, Float32Array, Float64Array, Int32Array,
    Int64Array, ListArray, StringArray, StructArray, UInt64Array,
};
use arrow_schema::{DataType as ArrowDataType, Field};
use serde_json::{Value, json};
use std::sync::Arc;
use uni_common::DataType;
use uni_crdt::Crdt;

/// Convert an Arrow array element at a given row index to a JSON Value.
///
/// This function handles all common Arrow types and recursively processes
/// nested structures like Lists and Structs.
pub fn arrow_to_value(col: &dyn Array, row: usize) -> Value {
    if col.is_null(row) {
        return Value::Null;
    }

    // String types
    if let Some(s) = col.as_any().downcast_ref::<StringArray>() {
        return Value::String(s.value(row).to_string());
    }

    // Integer types
    if let Some(u) = col.as_any().downcast_ref::<UInt64Array>() {
        return json!(u.value(row));
    }
    if let Some(i) = col.as_any().downcast_ref::<Int64Array>() {
        return json!(i.value(row));
    }
    if let Some(i) = col.as_any().downcast_ref::<Int32Array>() {
        return json!(i.value(row));
    }

    // Float types
    if let Some(f) = col.as_any().downcast_ref::<Float64Array>() {
        return json!(f.value(row));
    }
    if let Some(f) = col.as_any().downcast_ref::<Float32Array>() {
        return json!(f.value(row));
    }

    // Boolean type
    if let Some(b) = col.as_any().downcast_ref::<BooleanArray>() {
        return Value::Bool(b.value(row));
    }

    // Fixed-size list (vectors)
    if let Some(list) = col.as_any().downcast_ref::<FixedSizeListArray>() {
        let arr = list.value(row);
        let mut vals = Vec::with_capacity(arr.len());
        for i in 0..arr.len() {
            vals.push(arrow_to_value(arr.as_ref(), i));
        }
        return Value::Array(vals);
    }

    // Variable-size list
    if let Some(list) = col.as_any().downcast_ref::<ListArray>() {
        let arr = list.value(row);
        let mut vals = Vec::with_capacity(arr.len());
        for i in 0..arr.len() {
            vals.push(arrow_to_value(arr.as_ref(), i));
        }
        return Value::Array(vals);
    }

    // Struct type
    if let Some(s) = col.as_any().downcast_ref::<StructArray>() {
        let mut map = serde_json::Map::new();
        for (field, child) in s.fields().iter().zip(s.columns()) {
            map.insert(field.name().clone(), arrow_to_value(child.as_ref(), row));
        }
        return Value::Object(map);
    }

    // Fallback
    Value::Null
}

// ============================================================================
// Helper functions for values_to_array to reduce CC
// ============================================================================

fn values_to_uint64_array(values: &[Value]) -> ArrayRef {
    let mut builder = UInt64Builder::with_capacity(values.len());
    for v in values {
        if let Some(n) = v.as_u64() {
            builder.append_value(n);
        } else {
            builder.append_null();
        }
    }
    Arc::new(builder.finish())
}

fn values_to_int64_array(values: &[Value]) -> ArrayRef {
    let mut builder = Int64Builder::with_capacity(values.len());
    for v in values {
        if let Some(n) = v.as_i64() {
            builder.append_value(n);
        } else {
            builder.append_null();
        }
    }
    Arc::new(builder.finish())
}

fn values_to_int32_array(values: &[Value]) -> ArrayRef {
    let mut builder = Int32Builder::with_capacity(values.len());
    for v in values {
        if let Some(n) = v.as_i64() {
            builder.append_value(n as i32);
        } else {
            builder.append_null();
        }
    }
    Arc::new(builder.finish())
}

fn values_to_string_array(values: &[Value]) -> ArrayRef {
    let mut builder = StringBuilder::with_capacity(values.len(), values.len() * 10);
    for v in values {
        if let Some(s) = v.as_str() {
            builder.append_value(s);
        } else if v.is_null() {
            builder.append_null();
        } else {
            builder.append_value(v.to_string());
        }
    }
    Arc::new(builder.finish())
}

fn values_to_bool_array(values: &[Value]) -> ArrayRef {
    let mut builder = BooleanBuilder::with_capacity(values.len());
    for v in values {
        if let Some(b) = v.as_bool() {
            builder.append_value(b);
        } else {
            builder.append_null();
        }
    }
    Arc::new(builder.finish())
}

fn values_to_float32_array(values: &[Value]) -> ArrayRef {
    let mut builder = Float32Builder::with_capacity(values.len());
    for v in values {
        if let Some(n) = v.as_f64() {
            builder.append_value(n as f32);
        } else {
            builder.append_null();
        }
    }
    Arc::new(builder.finish())
}

fn values_to_float64_array(values: &[Value]) -> ArrayRef {
    let mut builder = Float64Builder::with_capacity(values.len());
    for v in values {
        if let Some(n) = v.as_f64() {
            builder.append_value(n);
        } else {
            builder.append_null();
        }
    }
    Arc::new(builder.finish())
}

fn values_to_fixed_size_binary_array(values: &[Value], size: i32) -> Result<ArrayRef> {
    let mut builder = FixedSizeBinaryBuilder::with_capacity(values.len(), size);
    for v in values {
        if let Value::Array(bytes) = v {
            let b: Vec<u8> = bytes
                .iter()
                .map(|bv| bv.as_u64().unwrap_or(0) as u8)
                .collect();
            if b.len() as i32 == size {
                builder.append_value(&b)?;
            } else {
                builder.append_null();
            }
        } else {
            builder.append_null();
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn values_to_fixed_size_list_f32_array(values: &[Value], size: i32) -> ArrayRef {
    let mut builder = FixedSizeListBuilder::new(Float32Builder::new(), size);
    for v in values {
        if let Value::Array(arr) = v {
            if arr.len() as i32 == size {
                for item in arr {
                    builder
                        .values()
                        .append_value(item.as_f64().unwrap_or(0.0) as f32);
                }
                builder.append(true);
            } else {
                builder.append(false);
            }
        } else {
            builder.append(false);
        }
    }
    Arc::new(builder.finish())
}

/// Convert a slice of JSON Values to an Arrow array based on the target Arrow DataType.
pub fn values_to_array(values: &[Value], dt: &ArrowDataType) -> Result<ArrayRef> {
    match dt {
        ArrowDataType::UInt64 => Ok(values_to_uint64_array(values)),
        ArrowDataType::Int64 => Ok(values_to_int64_array(values)),
        ArrowDataType::Int32 => Ok(values_to_int32_array(values)),
        ArrowDataType::Utf8 => Ok(values_to_string_array(values)),
        ArrowDataType::Boolean => Ok(values_to_bool_array(values)),
        ArrowDataType::Float32 => Ok(values_to_float32_array(values)),
        ArrowDataType::Float64 => Ok(values_to_float64_array(values)),
        ArrowDataType::FixedSizeBinary(size) => values_to_fixed_size_binary_array(values, *size),
        ArrowDataType::FixedSizeList(inner, size) => {
            if inner.data_type() == &ArrowDataType::Float32 {
                Ok(values_to_fixed_size_list_f32_array(values, *size))
            } else {
                Err(anyhow!("Unsupported FixedSizeList inner type"))
            }
        }
        _ => Err(anyhow!("Unsupported type for conversion: {:?}", dt)),
    }
}

/// Property value extractor for building Arrow columns from entity properties.
pub struct PropertyExtractor<'a> {
    #[allow(dead_code)] // Kept for debugging/error messages
    name: &'a str,
    data_type: &'a DataType,
}

impl<'a> PropertyExtractor<'a> {
    pub fn new(name: &'a str, data_type: &'a DataType) -> Self {
        Self { name, data_type }
    }

    /// Build an Arrow column from a slice of property maps.
    /// The `deleted` slice indicates which entries are deleted (use default values).
    pub fn build_column<F>(&self, len: usize, deleted: &[bool], get_props: F) -> Result<ArrayRef>
    where
        F: Fn(usize) -> Option<&'a Value>,
    {
        match self.data_type {
            DataType::String => self.build_string_column(len, deleted, get_props),
            DataType::Int32 => self.build_int32_column(len, deleted, get_props),
            DataType::Int64 => self.build_int64_column(len, deleted, get_props),
            DataType::Float32 => self.build_float32_column(len, deleted, get_props),
            DataType::Float64 => self.build_float64_column(len, deleted, get_props),
            DataType::Bool => self.build_bool_column(len, deleted, get_props),
            DataType::Vector { dimensions } => {
                self.build_vector_column(len, deleted, get_props, *dimensions)
            }
            DataType::Json => self.build_json_column(len, deleted, get_props),
            DataType::List(inner) => self.build_list_column(len, deleted, get_props, inner),
            DataType::Map(key, value) => self.build_map_column(len, deleted, get_props, key, value),
            DataType::Crdt(_) => self.build_crdt_column(len, deleted, get_props),
            DataType::Date | DataType::Time | DataType::DateTime | DataType::Duration => {
                self.build_int64_column(len, deleted, get_props)
            }
            _ => Err(anyhow!(
                "Unsupported data type for arrow conversion: {:?}",
                self.data_type
            )),
        }
    }

    fn build_string_column<F>(&self, len: usize, deleted: &[bool], get_props: F) -> Result<ArrayRef>
    where
        F: Fn(usize) -> Option<&'a Value>,
    {
        let mut values = Vec::with_capacity(len);
        for (i, &is_deleted) in deleted.iter().enumerate().take(len) {
            let val = get_props(i).and_then(|v| v.as_str());
            if val.is_none() && is_deleted {
                values.push(Some(""));
            } else {
                values.push(val);
            }
        }
        Ok(Arc::new(StringArray::from(values)))
    }

    fn build_int32_column<F>(&self, len: usize, deleted: &[bool], get_props: F) -> Result<ArrayRef>
    where
        F: Fn(usize) -> Option<&'a Value>,
    {
        let mut values = Vec::with_capacity(len);
        for (i, &is_deleted) in deleted.iter().enumerate().take(len) {
            let val = get_props(i).and_then(|v| v.as_i64()).map(|v| v as i32);
            if val.is_none() && is_deleted {
                values.push(Some(0));
            } else {
                values.push(val);
            }
        }
        Ok(Arc::new(Int32Array::from(values)))
    }

    fn build_int64_column<F>(&self, len: usize, deleted: &[bool], get_props: F) -> Result<ArrayRef>
    where
        F: Fn(usize) -> Option<&'a Value>,
    {
        let mut values = Vec::with_capacity(len);
        for (i, &is_deleted) in deleted.iter().enumerate().take(len) {
            let val = get_props(i).and_then(|v| v.as_i64());
            if val.is_none() && is_deleted {
                values.push(Some(0));
            } else {
                values.push(val);
            }
        }
        Ok(Arc::new(Int64Array::from(values)))
    }

    fn build_float32_column<F>(
        &self,
        len: usize,
        deleted: &[bool],
        get_props: F,
    ) -> Result<ArrayRef>
    where
        F: Fn(usize) -> Option<&'a Value>,
    {
        let mut values = Vec::with_capacity(len);
        for (i, &is_deleted) in deleted.iter().enumerate().take(len) {
            let val = get_props(i).and_then(|v| v.as_f64()).map(|v| v as f32);
            if val.is_none() && is_deleted {
                values.push(Some(0.0));
            } else {
                values.push(val);
            }
        }
        Ok(Arc::new(Float32Array::from(values)))
    }

    fn build_float64_column<F>(
        &self,
        len: usize,
        deleted: &[bool],
        get_props: F,
    ) -> Result<ArrayRef>
    where
        F: Fn(usize) -> Option<&'a Value>,
    {
        let mut values = Vec::with_capacity(len);
        for (i, &is_deleted) in deleted.iter().enumerate().take(len) {
            let val = get_props(i).and_then(|v| v.as_f64());
            if val.is_none() && is_deleted {
                values.push(Some(0.0));
            } else {
                values.push(val);
            }
        }
        Ok(Arc::new(Float64Array::from(values)))
    }

    fn build_bool_column<F>(&self, len: usize, deleted: &[bool], get_props: F) -> Result<ArrayRef>
    where
        F: Fn(usize) -> Option<&'a Value>,
    {
        let mut values = Vec::with_capacity(len);
        for (i, &is_deleted) in deleted.iter().enumerate().take(len) {
            let val = get_props(i).and_then(|v| v.as_bool());
            if val.is_none() && is_deleted {
                values.push(Some(false));
            } else {
                values.push(val);
            }
        }
        Ok(Arc::new(BooleanArray::from(values)))
    }

    fn build_vector_column<F>(
        &self,
        len: usize,
        deleted: &[bool],
        get_props: F,
        dimensions: usize,
    ) -> Result<ArrayRef>
    where
        F: Fn(usize) -> Option<&'a Value>,
    {
        let mut builder = FixedSizeListBuilder::new(Float32Builder::new(), dimensions as i32);

        for (i, &is_deleted) in deleted.iter().enumerate().take(len) {
            let val_array = get_props(i).and_then(|v| v.as_array());
            let (values, valid) = self.extract_vector_values(val_array, is_deleted, dimensions);
            for v in values {
                builder.values().append_value(v);
            }
            builder.append(valid);
        }
        Ok(Arc::new(builder.finish()))
    }

    /// Extract vector values from a JSON array, handling defaults and validation.
    fn extract_vector_values(
        &self,
        val_array: Option<&Vec<Value>>,
        is_deleted: bool,
        dimensions: usize,
    ) -> (Vec<f32>, bool) {
        let zeros = || vec![0.0_f32; dimensions];

        match val_array {
            Some(arr) if arr.len() == dimensions => {
                let values: Vec<f32> = arr
                    .iter()
                    .map(|v| v.as_f64().unwrap_or(0.0) as f32)
                    .collect();
                (values, true)
            }
            Some(_) => (zeros(), false),           // Wrong dimensions
            None if is_deleted => (zeros(), true), // Deleted entry gets default
            None => (zeros(), false),              // Missing value
        }
    }

    fn build_json_column<F>(&self, len: usize, deleted: &[bool], get_props: F) -> Result<ArrayRef>
    where
        F: Fn(usize) -> Option<&'a Value>,
    {
        let mut values = Vec::with_capacity(len);
        for (i, &is_deleted) in deleted.iter().enumerate().take(len) {
            let val = get_props(i).map(|v| v.to_string());
            if val.is_none() && is_deleted {
                values.push(Some("null".to_string()));
            } else {
                values.push(val.or(Some("null".to_string())));
            }
        }
        Ok(Arc::new(StringArray::from(values)))
    }

    fn build_list_column<F>(
        &self,
        len: usize,
        deleted: &[bool],
        get_props: F,
        inner: &DataType,
    ) -> Result<ArrayRef>
    where
        F: Fn(usize) -> Option<&'a Value>,
    {
        match inner {
            DataType::String => {
                self.build_typed_list(len, deleted, &get_props, StringBuilder::new(), |v, b| {
                    if let Some(s) = v.as_str() {
                        b.append_value(s);
                    } else {
                        b.append_null();
                    }
                })
            }
            DataType::Int64 => {
                self.build_typed_list(len, deleted, &get_props, Int64Builder::new(), |v, b| {
                    if let Some(n) = v.as_i64() {
                        b.append_value(n);
                    } else {
                        b.append_null();
                    }
                })
            }
            DataType::Float64 => {
                self.build_typed_list(len, deleted, &get_props, Float64Builder::new(), |v, b| {
                    if let Some(f) = v.as_f64() {
                        b.append_value(f);
                    } else {
                        b.append_null();
                    }
                })
            }
            _ => Err(anyhow!("Unsupported inner type for List: {:?}", inner)),
        }
    }

    /// Generic helper to build a list column with any inner builder type.
    fn build_typed_list<F, B, A>(
        &self,
        len: usize,
        deleted: &[bool],
        get_props: &F,
        inner_builder: B,
        mut append_value: A,
    ) -> Result<ArrayRef>
    where
        F: Fn(usize) -> Option<&'a Value>,
        B: arrow_array::builder::ArrayBuilder,
        A: FnMut(&Value, &mut B),
    {
        let mut builder = ListBuilder::new(inner_builder);
        for (i, &is_deleted) in deleted.iter().enumerate().take(len) {
            let val_array = get_props(i).and_then(|v| v.as_array());
            if val_array.is_none() && is_deleted {
                builder.append_null();
            } else if let Some(arr) = val_array {
                for v in arr {
                    append_value(v, builder.values());
                }
                builder.append(true);
            } else {
                builder.append_null();
            }
        }
        Ok(Arc::new(builder.finish()))
    }

    fn build_map_column<F>(
        &self,
        len: usize,
        deleted: &[bool],
        get_props: F,
        key: &DataType,
        value: &DataType,
    ) -> Result<ArrayRef>
    where
        F: Fn(usize) -> Option<&'a Value>,
    {
        if !matches!(key, DataType::String) {
            return Err(anyhow!("Map keys must be String (JSON limitation)"));
        }

        match value {
            DataType::String => self.build_typed_map(
                len,
                deleted,
                &get_props,
                StringBuilder::new(),
                arrow_schema::DataType::Utf8,
                |v, b: &mut StringBuilder| {
                    if let Some(s) = v.as_str() {
                        b.append_value(s);
                    } else {
                        b.append_null();
                    }
                },
            ),
            DataType::Int64 => self.build_typed_map(
                len,
                deleted,
                &get_props,
                Int64Builder::new(),
                arrow_schema::DataType::Int64,
                |v, b: &mut Int64Builder| {
                    if let Some(n) = v.as_i64() {
                        b.append_value(n);
                    } else {
                        b.append_null();
                    }
                },
            ),
            _ => Err(anyhow!("Unsupported value type for Map: {:?}", value)),
        }
    }

    /// Generic helper to build a map column with any value builder type.
    fn build_typed_map<F, B, A>(
        &self,
        len: usize,
        deleted: &[bool],
        get_props: &F,
        value_builder: B,
        value_arrow_type: arrow_schema::DataType,
        mut append_value: A,
    ) -> Result<ArrayRef>
    where
        F: Fn(usize) -> Option<&'a Value>,
        B: arrow_array::builder::ArrayBuilder,
        A: FnMut(&Value, &mut B),
    {
        let key_builder = Box::new(StringBuilder::new());
        let value_builder = Box::new(value_builder);
        let struct_builder = StructBuilder::new(
            vec![
                Field::new("key", arrow_schema::DataType::Utf8, false),
                Field::new("value", value_arrow_type, true),
            ],
            vec![key_builder, value_builder],
        );
        let mut builder = ListBuilder::new(struct_builder);

        for (i, &is_deleted) in deleted.iter().enumerate().take(len) {
            self.append_map_entry(&mut builder, get_props(i), is_deleted, &mut append_value);
        }
        Ok(Arc::new(builder.finish()))
    }

    /// Append a single map entry to the list builder.
    fn append_map_entry<B, A>(
        &self,
        builder: &mut ListBuilder<StructBuilder>,
        val: Option<&'a Value>,
        is_deleted: bool,
        append_value: &mut A,
    ) where
        B: arrow_array::builder::ArrayBuilder,
        A: FnMut(&Value, &mut B),
    {
        let val_obj = val.and_then(|v| v.as_object());
        if val_obj.is_none() && is_deleted {
            builder.append(false);
        } else if let Some(obj) = val_obj {
            let struct_b = builder.values();
            for (k, v) in obj {
                struct_b
                    .field_builder::<StringBuilder>(0)
                    .unwrap()
                    .append_value(k);
                // Safety: We know the value builder type matches B
                let value_b = struct_b.field_builder::<B>(1).unwrap();
                append_value(v, value_b);
                struct_b.append(true);
            }
            builder.append(true);
        } else {
            builder.append(false);
        }
    }

    fn build_crdt_column<F>(&self, len: usize, deleted: &[bool], get_props: F) -> Result<ArrayRef>
    where
        F: Fn(usize) -> Option<&'a Value>,
    {
        let mut builder = BinaryBuilder::new();
        for (i, &is_deleted) in deleted.iter().enumerate().take(len) {
            if is_deleted {
                builder.append_null();
                continue;
            }
            if let Some(val) = get_props(i) {
                if let Ok(crdt) = serde_json::from_value::<Crdt>(val.clone()) {
                    if let Ok(bytes) = crdt.to_msgpack() {
                        builder.append_value(&bytes);
                    } else {
                        builder.append_null();
                    }
                } else {
                    builder.append_null();
                }
            } else {
                builder.append_null();
            }
        }
        Ok(Arc::new(builder.finish()))
    }
}

/// Build a column for edge entries (no deleted flag handling needed).
pub fn build_edge_column<'a>(
    name: &'a str,
    data_type: &'a DataType,
    len: usize,
    get_props: impl Fn(usize) -> Option<&'a Value>,
) -> Result<ArrayRef> {
    // For edges, use empty deleted array
    let deleted = vec![false; len];
    let extractor = PropertyExtractor::new(name, data_type);
    extractor.build_column(len, &deleted, get_props)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_arrow_to_value_string() {
        let arr = StringArray::from(vec![Some("hello"), None, Some("world")]);
        assert_eq!(arrow_to_value(&arr, 0), Value::String("hello".to_string()));
        assert_eq!(arrow_to_value(&arr, 1), Value::Null);
        assert_eq!(arrow_to_value(&arr, 2), Value::String("world".to_string()));
    }

    #[test]
    fn test_arrow_to_value_int64() {
        let arr = Int64Array::from(vec![Some(42), None, Some(-10)]);
        assert_eq!(arrow_to_value(&arr, 0), json!(42));
        assert_eq!(arrow_to_value(&arr, 1), Value::Null);
        assert_eq!(arrow_to_value(&arr, 2), json!(-10));
    }

    #[test]
    #[allow(clippy::approx_constant)]
    fn test_arrow_to_value_float64() {
        let arr = Float64Array::from(vec![Some(3.14), None]);
        assert_eq!(arrow_to_value(&arr, 0), json!(3.14));
        assert_eq!(arrow_to_value(&arr, 1), Value::Null);
    }

    #[test]
    fn test_arrow_to_value_bool() {
        let arr = BooleanArray::from(vec![Some(true), Some(false), None]);
        assert_eq!(arrow_to_value(&arr, 0), Value::Bool(true));
        assert_eq!(arrow_to_value(&arr, 1), Value::Bool(false));
        assert_eq!(arrow_to_value(&arr, 2), Value::Null);
    }

    #[test]
    fn test_values_to_array_int64() {
        let values = vec![json!(1), json!(2), Value::Null, json!(4)];
        let arr = values_to_array(&values, &ArrowDataType::Int64).unwrap();
        assert_eq!(arr.len(), 4);

        let int_arr = arr.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(int_arr.value(0), 1);
        assert_eq!(int_arr.value(1), 2);
        assert!(int_arr.is_null(2));
        assert_eq!(int_arr.value(3), 4);
    }

    #[test]
    fn test_values_to_array_string() {
        let values = vec![
            Value::String("a".to_string()),
            Value::String("b".to_string()),
            Value::Null,
        ];
        let arr = values_to_array(&values, &ArrowDataType::Utf8).unwrap();
        assert_eq!(arr.len(), 3);

        let str_arr = arr.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(str_arr.value(0), "a");
        assert_eq!(str_arr.value(1), "b");
        assert!(str_arr.is_null(2));
    }

    #[test]
    fn test_property_extractor_string() {
        let props: Vec<HashMap<String, Value>> = vec![
            [("name".to_string(), json!("Alice"))].into_iter().collect(),
            [("name".to_string(), json!("Bob"))].into_iter().collect(),
            HashMap::new(),
        ];
        let deleted = vec![false, false, true];

        let extractor = PropertyExtractor::new("name", &DataType::String);
        let arr = extractor
            .build_column(3, &deleted, |i| props[i].get("name"))
            .unwrap();

        let str_arr = arr.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(str_arr.value(0), "Alice");
        assert_eq!(str_arr.value(1), "Bob");
        assert_eq!(str_arr.value(2), ""); // Deleted entries get default
    }

    #[test]
    fn test_property_extractor_int64() {
        let props: Vec<HashMap<String, Value>> = vec![
            [("age".to_string(), json!(25))].into_iter().collect(),
            [("age".to_string(), json!(30))].into_iter().collect(),
            HashMap::new(),
        ];
        let deleted = vec![false, false, true];

        let extractor = PropertyExtractor::new("age", &DataType::Int64);
        let arr = extractor
            .build_column(3, &deleted, |i| props[i].get("age"))
            .unwrap();

        let int_arr = arr.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(int_arr.value(0), 25);
        assert_eq!(int_arr.value(1), 30);
        assert_eq!(int_arr.value(2), 0); // Deleted entries get default
    }
}
