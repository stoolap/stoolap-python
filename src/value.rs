// Copyright 2025 Stoolap Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyDict, PyFloat, PyInt, PyList, PyString, PyTuple};

use stoolap::api::ParamVec;
use stoolap::core::Value;

/// Parsed bind parameters from Python.
pub enum BindParams {
    Positional(ParamVec),
    Named(Vec<(String, Value)>),
}

/// Convert a single Python object to a Stoolap Value.
pub fn py_to_value(obj: &Bound<'_, PyAny>) -> PyResult<Value> {
    // Check None first
    if obj.is_none() {
        return Ok(Value::null_unknown());
    }

    // Bool must be checked before int (bool is subclass of int in Python)
    if let Ok(b) = obj.downcast::<PyBool>() {
        return Ok(Value::Boolean(b.is_true()));
    }

    if let Ok(i) = obj.downcast::<PyInt>() {
        let val: i64 = i.extract()?;
        return Ok(Value::Integer(val));
    }

    if let Ok(f) = obj.downcast::<PyFloat>() {
        let val: f64 = f.extract()?;
        return Ok(Value::Float(val));
    }

    if let Ok(s) = obj.downcast::<PyString>() {
        let val: String = s.extract()?;
        return Ok(Value::text(&val));
    }

    // Check for datetime.datetime
    let py = obj.py();
    let datetime_mod = py.import("datetime")?;
    let datetime_type = datetime_mod.getattr("datetime")?;
    if obj.is_instance(&datetime_type)? {
        return py_datetime_to_value(obj);
    }

    // dict/list -> JSON string
    if obj.downcast::<PyDict>().is_ok() || obj.downcast::<PyList>().is_ok() {
        let json_mod = py.import("json")?;
        let json_str: String = json_mod.call_method1("dumps", (obj,))?.extract()?;
        return Ok(Value::json(&json_str));
    }

    Err(PyTypeError::new_err(format!(
        "Unsupported parameter type: {}",
        obj.get_type().name()?
    )))
}

/// Convert a Python datetime to a Stoolap Timestamp value.
fn py_datetime_to_value(obj: &Bound<'_, PyAny>) -> PyResult<Value> {
    let py = obj.py();

    // Try to get timezone-aware timestamp via .timestamp() method
    let ts: f64 = obj.call_method0("timestamp")?.extract()?;
    let secs = ts.floor() as i64;
    let remaining = ts - ts.floor();
    let nsecs = (remaining * 1_000_000_000.0).round() as u32;

    // Check if timezone-aware
    let tzinfo = obj.getattr("tzinfo")?;
    if tzinfo.is_none() {
        // Naive datetime -> treat as UTC
        if let Some(dt) = chrono::DateTime::from_timestamp(secs, nsecs) {
            return Ok(Value::Timestamp(dt));
        }
    } else {
        // Timezone-aware -> convert to UTC via astimezone
        let utc = py.import("datetime")?.getattr("timezone")?.getattr("utc")?;
        let utc_dt = obj.call_method1("astimezone", (utc,))?;
        let utc_ts: f64 = utc_dt.call_method0("timestamp")?.extract()?;
        let utc_secs = utc_ts.floor() as i64;
        let utc_remaining = utc_ts - utc_ts.floor();
        let utc_nsecs = (utc_remaining * 1_000_000_000.0).round() as u32;
        if let Some(dt) = chrono::DateTime::from_timestamp(utc_secs, utc_nsecs) {
            return Ok(Value::Timestamp(dt));
        }
    }

    // Fallback: store as ISO string
    let iso: String = obj.call_method0("isoformat")?.extract()?;
    Ok(Value::text(&iso))
}

/// Convert a Stoolap Value to a Python object.
pub fn value_to_py(py: Python<'_>, val: &Value) -> PyObject {
    match val {
        Value::Null(_) => py.None(),
        Value::Boolean(b) => b.into_pyobject(py).unwrap().to_owned().into_any().unbind(),
        Value::Integer(i) => i.into_pyobject(py).unwrap().to_owned().into_any().unbind(),
        Value::Float(f) => f.into_pyobject(py).unwrap().to_owned().into_any().unbind(),
        Value::Text(s) => s.as_str().into_pyobject(py).unwrap().to_owned().into_any().unbind(),
        Value::Timestamp(ts) => {
            let iso = ts.format("%Y-%m-%dT%H:%M:%S%.fZ").to_string();
            match py
                .import("datetime")
                .and_then(|m| m.getattr("datetime"))
                .and_then(|cls| cls.call_method1("fromisoformat", (iso.replace('Z', "+00:00"),)))
            {
                Ok(dt) => dt.unbind(),
                Err(_) => iso.into_pyobject(py).unwrap().to_owned().into_any().unbind(),
            }
        }
        Value::Json(s) => s.as_ref().into_pyobject(py).unwrap().to_owned().into_any().unbind(),
    }
}

/// Parse Python params into BindParams.
/// Accepts: list (positional), tuple (positional), dict (named), or None.
pub fn parse_params(params: Option<&Bound<'_, PyAny>>) -> PyResult<BindParams> {
    let params = match params {
        None => return Ok(BindParams::Positional(ParamVec::new())),
        Some(p) => p,
    };

    if params.is_none() {
        return Ok(BindParams::Positional(ParamVec::new()));
    }

    // Try list first (most common)
    if let Ok(list) = params.downcast::<PyList>() {
        let mut values = ParamVec::new();
        for item in list.iter() {
            values.push(py_to_value(&item)?);
        }
        return Ok(BindParams::Positional(values));
    }

    // Tuple
    if let Ok(tuple) = params.downcast::<PyTuple>() {
        let mut values = ParamVec::new();
        for item in tuple.iter() {
            values.push(py_to_value(&item)?);
        }
        return Ok(BindParams::Positional(values));
    }

    // Dict -> named params
    if let Ok(dict) = params.downcast::<PyDict>() {
        let mut named = Vec::with_capacity(dict.len());
        for (key, val) in dict.iter() {
            let key_str: String = key.extract()?;
            let clean = key_str
                .trim_start_matches(':')
                .trim_start_matches('@')
                .trim_start_matches('$')
                .to_string();
            named.push((clean, py_to_value(&val)?));
        }
        return Ok(BindParams::Named(named));
    }

    Err(PyTypeError::new_err(
        "Parameters must be a list, tuple, or dict",
    ))
}
