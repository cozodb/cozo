/*
 * Copyright 2022, The Cozo Project Authors.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file,
 * You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use std::collections::BTreeMap;

use cozo::*;

fn py_to_value(ob: &PyAny) -> PyResult<DataValue> {
    Ok(if ob.is_none() {
        DataValue::Null
    } else if let Ok(i) = ob.extract::<i64>() {
        DataValue::from(i)
    } else if let Ok(f) = ob.extract::<f64>() {
        DataValue::from(f)
    } else if let Ok(s) = ob.extract::<String>() {
        DataValue::from(s)
    } else if let Ok(b) = ob.extract::<Vec<u8>>() {
        DataValue::Bytes(b)
    } else if let Ok(l) = ob.downcast::<PyList>() {
        let mut coll = Vec::with_capacity(l.len());
        for el in l {
            let el = py_to_value(el)?;
            coll.push(el)
        }
        DataValue::List(coll)
    } else if let Ok(d) = ob.downcast::<PyDict>() {
        let mut coll = Vec::with_capacity(d.len());
        for (k, v) in d {
            let k = py_to_value(k)?;
            let v = py_to_value(v)?;
            coll.push(DataValue::List(vec![k, v]))
        }
        DataValue::List(coll)
    } else {
        return Err(PyException::new_err(format!("Cannot convert {ob} into Cozo value")));
    })
}

fn convert_params(ob: &PyDict) -> PyResult<BTreeMap<String, DataValue>> {
    let mut ret = BTreeMap::new();
    for (k, v) in ob {
        let k: String = k.extract()?;
        let v = py_to_value(v)?;
        ret.insert(k, v);
    }
    Ok(ret)
}

fn value_to_py(val: DataValue, py: Python<'_>) -> PyObject {
    match val {
        DataValue::Null => py.None(),
        DataValue::Bool(b) => b.into_py(py),
        DataValue::Num(num) => match num {
            Num::Int(i) => i.into_py(py),
            Num::Float(f) => f.into_py(py),
        },
        DataValue::Str(s) => s.as_str().into_py(py),
        DataValue::Bytes(b) => b.into_py(py),
        DataValue::Uuid(uuid) => uuid.0.to_string().into_py(py),
        DataValue::Regex(rx) => rx.0.as_str().into_py(py),
        DataValue::List(l) => {
            let vs: Vec<_> = l.into_iter().map(|v| value_to_py(v, py)).collect();
            vs.into_py(py)
        }
        DataValue::Set(l) => {
            let vs: Vec<_> = l.into_iter().map(|v| value_to_py(v, py)).collect();
            vs.into_py(py)
        }
        DataValue::Validity(vld) => {
            [vld.timestamp.0 .0.into_py(py), vld.is_assert.0.into_py(py)].into_py(py)
        }
        DataValue::Bot => py.None(),
    }
}

fn named_rows_to_py(named_rows: NamedRows, py: Python<'_>) -> PyObject {
    let rows = named_rows
        .rows
        .into_iter()
        .map(|row| {
            row.into_iter()
                .map(|val| value_to_py(val, py))
                .collect::<Vec<_>>()
                .into_py(py)
        })
        .collect::<Vec<_>>()
        .into_py(py);
    let headers = named_rows.headers.into_py(py);
    BTreeMap::from([("rows", rows), ("headers", headers)]).into_py(py)
}

#[pyclass]
struct CozoDbPy {
    db: Option<DbInstance>,
}

const DB_CLOSED_MSG: &str = r##"{"ok":false,"message":"database closed"}"##;

#[pymethods]
impl CozoDbPy {
    #[new]
    fn new(engine: &str, path: &str, options: &str) -> PyResult<Self> {
        match DbInstance::new(engine, path, options) {
            Ok(db) => Ok(Self { db: Some(db) }),
            Err(err) => Err(PyException::new_err(format!("{err:?}"))),
        }
    }
    pub fn run_script(&self, py: Python<'_>, query: &str, params: &PyDict) -> PyResult<PyObject> {
        if let Some(db) = &self.db {
            let params = convert_params(params)?;
            match py.allow_threads(|| db.run_script(query, params)) {
                Ok(rows) => Ok(named_rows_to_py(rows, py)),
                Err(err) => {
                    let reports = format_error_as_json(err, Some(query)).to_string();
                    Err(PyException::new_err(reports))
                }
            }
        } else {
            Err(PyException::new_err(DB_CLOSED_MSG))
        }
    }
    pub fn register_callback(&self, rel: &str, callback: &PyAny) -> PyResult<u32> {
        if let Some(db) = &self.db {
            let cb: Py<PyAny> = callback.into();
            match db.register_callback(rel, move |op, new, old| {
                Python::with_gil(|py| {
                    let callable = cb.as_ref(py);
                    let _ = callable.call0();
                })
            }) {
                Ok(id) => Ok(id),
                Err(err) => {
                    let reports = format_error_as_json(err, None).to_string();
                    Err(PyException::new_err(reports))
                }
            }
        } else {
            Err(PyException::new_err(DB_CLOSED_MSG))
        }
    }
    pub fn run_query(&self, py: Python<'_>, query: &str, params: &str) -> String {
        if let Some(db) = &self.db {
            py.allow_threads(|| db.run_script_str(query, params))
        } else {
            DB_CLOSED_MSG.to_string()
        }
    }
    pub fn export_relations(&self, py: Python<'_>, data: &str) -> String {
        if let Some(db) = &self.db {
            py.allow_threads(|| db.export_relations_str(data))
        } else {
            DB_CLOSED_MSG.to_string()
        }
    }
    pub fn import_relations(&self, py: Python<'_>, data: &str) -> String {
        if let Some(db) = &self.db {
            py.allow_threads(|| db.import_relations_str(data))
        } else {
            DB_CLOSED_MSG.to_string()
        }
    }
    pub fn backup(&self, py: Python<'_>, path: &str) -> String {
        if let Some(db) = &self.db {
            py.allow_threads(|| db.backup_db_str(path))
        } else {
            DB_CLOSED_MSG.to_string()
        }
    }
    pub fn restore(&self, py: Python<'_>, path: &str) -> String {
        if let Some(db) = &self.db {
            py.allow_threads(|| db.restore_backup_str(path))
        } else {
            DB_CLOSED_MSG.to_string()
        }
    }
    pub fn import_from_backup(&self, py: Python<'_>, data: &str) -> String {
        if let Some(db) = &self.db {
            py.allow_threads(|| db.import_from_backup_str(data))
        } else {
            DB_CLOSED_MSG.to_string()
        }
    }
    pub fn close(&mut self) -> bool {
        self.db.take().is_some()
    }
}

#[pymodule]
fn cozo_embedded(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_class::<CozoDbPy>()?;
    Ok(())
}
