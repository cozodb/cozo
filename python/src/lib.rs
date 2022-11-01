/*
 * Copyright 2022, The Cozo Project Authors. Licensed under MIT/Apache-2.0/BSD-3-Clause.
 */

use std::collections::BTreeMap;

use miette::miette;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;

use cozo::Db;

#[pyclass(extends=PyException)]
struct ErrorBridge(cozo::Error);
trait PyResultExt<T> {
    fn into_py_res(self) -> PyResult<T>;
}
impl<T> PyResultExt<T> for miette::Result<T> {
    fn into_py_res(self) -> PyResult<T> {
        match self {
            Ok(t) => Ok(t),
            Err(e) => Err(PyException::new_err(format!("{:?}", e))),
        }
    }
}
#[pyclass]
struct CozoDbPy {
    db: Db,
}
#[pymethods]
impl CozoDbPy {
    #[new]
    #[args(create_if_missing = true, destroy_on_exit = false)]
    fn new(path: &str) -> PyResult<Self> {
        let db = Db::new(path).into_py_res()?;
        Ok(Self { db })
    }
    pub fn run_query(&self, py: Python<'_>, query: &str, params: &str) -> PyResult<String> {
        let params_map: serde_json::Value = serde_json::from_str(params)
            .map_err(|_| miette!("the given params argument is not valid JSON"))
            .into_py_res()?;
        let params_arg: BTreeMap<_, _> = match params_map {
            serde_json::Value::Object(m) => m.into_iter().collect(),
            _ => Err(miette!("the given params argument is not a JSON map")).into_py_res()?,
        };
        let ret = py.allow_threads(|| self.db.run_script(query, &params_arg).into_py_res())?;
        Ok(ret.to_string())
    }
}
#[pymodule]
fn cozo_py_module(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_class::<CozoDbPy>()?;
    m.add_class::<ErrorBridge>()?;
    Ok(())
}
