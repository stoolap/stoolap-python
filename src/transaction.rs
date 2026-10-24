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

use pyo3::prelude::*;
use pyo3::types::PyList;
use std::sync::{Arc, Mutex};

use stoolap::api::{Database as ApiDatabase, Transaction as ApiTransaction};

use crate::database::{first_row_to_dict, rows_to_dicts, rows_to_raw, to_named_params};
use crate::error::to_py;
use crate::statement::PreparedStatement;
use crate::value::{parse_params, BindParams};

/// A Stoolap transaction.
///
/// Can be used as a context manager:
/// ```python
/// with db.begin() as tx:
///     tx.execute("INSERT INTO users VALUES ($1, $2)", [1, "Alice"])
///     # auto-commits on clean exit, auto-rollbacks on exception
/// ```
#[pyclass]
pub struct Transaction {
    tx: Mutex<Option<ApiTransaction>>,
    db: Arc<ApiDatabase>,
}

impl Transaction {
    pub fn from_tx(tx: ApiTransaction, db: Arc<ApiDatabase>) -> Self {
        Self {
            tx: Mutex::new(Some(tx)),
            db,
        }
    }

    fn with_tx<F, R>(&self, f: F) -> PyResult<R>
    where
        F: FnOnce(&mut ApiTransaction) -> PyResult<R>,
    {
        let mut guard = self
            .tx
            .lock()
            .map_err(|_| crate::error::StoolapError::new_err("Transaction lock poisoned"))?;
        let tx = guard
            .as_mut()
            .ok_or_else(|| crate::error::StoolapError::new_err("Transaction is no longer active"))?;
        f(tx)
    }
}

#[pymethods]
impl Transaction {
    /// Execute a DDL/DML statement within the transaction.
    ///
    /// Returns the number of rows affected.
    #[pyo3(signature = (sql, params=None))]
    fn execute(&self, py: Python<'_>, sql: &str, params: Option<&Bound<'_, PyAny>>) -> PyResult<i64> {
        let bind = parse_params(params)?;
        let sql = sql.to_string();
        py.allow_threads(|| {
            self.with_tx(|tx| match bind {
                BindParams::Positional(p) => tx.execute(&sql, p).map_err(to_py),
                BindParams::Named(named) => {
                    tx.execute_named(&sql, to_named_params(&named)).map_err(to_py)
                }
            })
        })
    }

    /// Query rows within the transaction. Returns a list of dicts.
    #[pyo3(signature = (sql, params=None))]
    fn query(&self, py: Python<'_>, sql: &str, params: Option<&Bound<'_, PyAny>>) -> PyResult<PyObject> {
        let bind = parse_params(params)?;
        let sql = sql.to_string();
        let rows = py.allow_threads(|| {
            self.with_tx(|tx| match bind {
                BindParams::Positional(p) => tx.query(&sql, p).map_err(to_py),
                BindParams::Named(named) => {
                    tx.query_named(&sql, to_named_params(&named)).map_err(to_py)
                }
            })
        })?;
        rows_to_dicts(py, rows)
    }

    /// Query a single row. Returns a dict or None.
    #[pyo3(signature = (sql, params=None))]
    fn query_one(
        &self,
        py: Python<'_>,
        sql: &str,
        params: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<PyObject> {
        let bind = parse_params(params)?;
        let sql = sql.to_string();
        let rows = py.allow_threads(|| {
            self.with_tx(|tx| match bind {
                BindParams::Positional(p) => tx.query(&sql, p).map_err(to_py),
                BindParams::Named(named) => {
                    tx.query_named(&sql, to_named_params(&named)).map_err(to_py)
                }
            })
        })?;
        first_row_to_dict(py, rows)
    }

    /// Query rows in raw format. Returns { columns: [...], rows: [[...], ...] }.
    #[pyo3(signature = (sql, params=None))]
    fn query_raw(
        &self,
        py: Python<'_>,
        sql: &str,
        params: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<PyObject> {
        let bind = parse_params(params)?;
        let sql = sql.to_string();
        let rows = py.allow_threads(|| {
            self.with_tx(|tx| match bind {
                BindParams::Positional(p) => tx.query(&sql, p).map_err(to_py),
                BindParams::Named(named) => {
                    tx.query_named(&sql, to_named_params(&named)).map_err(to_py)
                }
            })
        })?;
        rows_to_raw(py, rows)
    }

    /// Execute the same SQL with multiple parameter sets.
    ///
    /// Returns total rows affected.
    #[pyo3(signature = (sql, params_list))]
    fn execute_batch(
        &self,
        py: Python<'_>,
        sql: &str,
        params_list: &Bound<'_, PyList>,
    ) -> PyResult<i64> {
        let sql = sql.to_string();

        // Parse all param sets while holding GIL
        let mut all_params = Vec::with_capacity(params_list.len());
        for item in params_list.iter() {
            let bind = parse_params(Some(&item))?;
            match bind {
                BindParams::Positional(p) => all_params.push(p),
                BindParams::Named(_) => {
                    return Err(pyo3::exceptions::PyTypeError::new_err(
                        "execute_batch only supports positional parameters (list/tuple)",
                    ));
                }
            }
        }

        // Execute without GIL
        py.allow_threads(|| {
            let stmt = self.db.prepare(&sql).map_err(to_py)?;
            self.with_tx(|tx| {
                let mut total = 0i64;
                for params in all_params {
                    total += stmt.execute_in(tx, params).map_err(to_py)?;
                }
                Ok(total)
            })
        })
    }

    /// Execute a prepared statement within the transaction. Returns rows affected.
    #[pyo3(signature = (stmt, params=None))]
    fn execute_prepared(
        &self,
        py: Python<'_>,
        stmt: &PreparedStatement,
        params: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<i64> {
        let bind = parse_params(params)?;
        let stmt = stmt.inner();
        py.allow_threads(|| {
            self.with_tx(|tx| match bind {
                BindParams::Positional(p) => stmt.execute_in(tx, p).map_err(to_py),
                BindParams::Named(named) => {
                    stmt.execute_named_in(tx, to_named_params(&named)).map_err(to_py)
                }
            })
        })
    }

    /// Query rows using a prepared statement within the transaction. Returns list of dicts.
    #[pyo3(signature = (stmt, params=None))]
    fn query_prepared(
        &self,
        py: Python<'_>,
        stmt: &PreparedStatement,
        params: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<PyObject> {
        let bind = parse_params(params)?;
        let stmt = stmt.inner();
        let rows = py.allow_threads(|| {
            self.with_tx(|tx| match bind {
                BindParams::Positional(p) => stmt.query_in(tx, p).map_err(to_py),
                BindParams::Named(named) => {
                    stmt.query_named_in(tx, to_named_params(&named)).map_err(to_py)
                }
            })
        })?;
        rows_to_dicts(py, rows)
    }

    /// Query a single row using a prepared statement. Returns dict or None.
    #[pyo3(signature = (stmt, params=None))]
    fn query_one_prepared(
        &self,
        py: Python<'_>,
        stmt: &PreparedStatement,
        params: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<PyObject> {
        let bind = parse_params(params)?;
        let stmt = stmt.inner();
        let rows = py.allow_threads(|| {
            self.with_tx(|tx| match bind {
                BindParams::Positional(p) => stmt.query_in(tx, p).map_err(to_py),
                BindParams::Named(named) => {
                    stmt.query_named_in(tx, to_named_params(&named)).map_err(to_py)
                }
            })
        })?;
        first_row_to_dict(py, rows)
    }

    /// Query rows using a prepared statement in raw format. Returns { columns: [...], rows: [[...], ...] }.
    #[pyo3(signature = (stmt, params=None))]
    fn query_raw_prepared(
        &self,
        py: Python<'_>,
        stmt: &PreparedStatement,
        params: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<PyObject> {
        let bind = parse_params(params)?;
        let stmt = stmt.inner();
        let rows = py.allow_threads(|| {
            self.with_tx(|tx| match bind {
                BindParams::Positional(p) => stmt.query_in(tx, p).map_err(to_py),
                BindParams::Named(named) => {
                    stmt.query_named_in(tx, to_named_params(&named)).map_err(to_py)
                }
            })
        })?;
        rows_to_raw(py, rows)
    }

    /// Commit the transaction.
    fn commit(&self, py: Python<'_>) -> PyResult<()> {
        py.allow_threads(|| {
            let mut guard = self
                .tx
                .lock()
                .map_err(|_| crate::error::StoolapError::new_err("Transaction lock poisoned"))?;
            let mut tx = guard.take().ok_or_else(|| {
                crate::error::StoolapError::new_err("Transaction is no longer active")
            })?;
            tx.commit().map_err(to_py)
        })
    }

    /// Rollback the transaction.
    fn rollback(&self, py: Python<'_>) -> PyResult<()> {
        py.allow_threads(|| {
            let mut guard = self
                .tx
                .lock()
                .map_err(|_| crate::error::StoolapError::new_err("Transaction lock poisoned"))?;
            let mut tx = guard.take().ok_or_else(|| {
                crate::error::StoolapError::new_err("Transaction is no longer active")
            })?;
            tx.rollback().map_err(to_py)
        })
    }

    /// Context manager: enter.
    fn __enter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    /// Context manager: exit. Auto-commits on clean exit, auto-rollbacks on exception.
    #[pyo3(signature = (exc_type=None, _exc_val=None, _exc_tb=None))]
    fn __exit__(
        &self,
        py: Python<'_>,
        exc_type: Option<&Bound<'_, PyAny>>,
        _exc_val: Option<&Bound<'_, PyAny>>,
        _exc_tb: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<bool> {
        let mut guard = self
            .tx
            .lock()
            .map_err(|_| crate::error::StoolapError::new_err("Transaction lock poisoned"))?;

        if let Some(mut tx) = guard.take() {
            if exc_type.is_some() {
                // Exception occurred -> rollback
                py.allow_threads(|| {
                    let _ = tx.rollback();
                });
            } else {
                // Clean exit -> commit
                py.allow_threads(|| tx.commit().map_err(to_py))?;
            }
        }

        // Don't suppress exceptions
        Ok(false)
    }

    fn __repr__(&self) -> String {
        let active = self.tx.lock().map(|g| g.is_some()).unwrap_or(false);
        if active {
            "Transaction(active)".to_string()
        } else {
            "Transaction(closed)".to_string()
        }
    }
}
