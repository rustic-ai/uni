# Reference

Complete reference documentation for Uni.

<div class="feature-grid">

<div class="feature-card">

### [Rust API](rust-api.md)
Programmatic access to Uni from Rust applications.

</div>

<div class="feature-card">

### [Python API](python-api.md)
Python bindings for direct database access.

</div>

<div class="feature-card">

### [Pydantic OGM](pydantic-ogm.md)
Type-safe Python models with Pydantic validation.

</div>

<div class="feature-card">

### [REST API](rest-api.md)
HTTP API reference for remote query execution and monitoring.

</div>

<div class="feature-card">

### [Configuration](configuration.md)
All configuration options for storage, runtime, and queries.

</div>

<div class="feature-card">

### [Troubleshooting](troubleshooting.md)
Common issues, error messages, and solutions.

</div>

<div class="feature-card">

### [Glossary](glossary.md)
Terminology and abbreviations used in Uni documentation.

</div>

</div>

## Quick Reference

### Common Configuration

```rust
let config = StorageConfig {
    path: "./storage".into(),
    adjacency_cache_size: 1_000_000,
    property_cache_size: 100_000,
    max_l0_size: 64 * 1024 * 1024,  // 64 MB
    ..Default::default()
};
```

### Supported Data Types

| Type | Description | Example |
|------|-------------|---------|
| `String` | UTF-8 text | `"hello"` |
| `Int32` | 32-bit integer | `42` |
| `Int64` | 64-bit integer | `9223372036854775807` |
| `Float32` | 32-bit float | `3.14` |
| `Float64` | 64-bit float | `3.141592653589793` |
| `Boolean` | True/false | `true` |
| `Vector[N]` | N-dimensional float vector | `[0.1, 0.2, 0.3]` |
| `Json` | Nested JSON document | `{"key": "value"}` |

### Environment Variables

| Variable | Description |
|----------|-------------|
| `RUST_LOG` | Log level (`uni=debug`, `uni_db::storage=trace`) |
| `UNI_STORAGE_PATH` | Default storage path |
| `UNI_CACHE_DIR` | Local cache directory |

## Next Steps

- For API details, see [Rust API](rust-api.md)
- For tuning options, see [Configuration](configuration.md)
- Having issues? Check [Troubleshooting](troubleshooting.md)
