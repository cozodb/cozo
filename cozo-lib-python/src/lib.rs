/*
 * Copyright 2022, The Cozo Project Authors. Licensed under MIT/Apache-2.0/BSD-3-Clause.
 */

use pyo3::exceptions::PyException;
use pyo3::prelude::*;

use cozo::*;

#[pyclass]
struct CozoDbPy {
    db: Option<DbInstance>,
}

const DB_CLOSED_MSG: &str = r##"{"ok":false,"message":"database closed"}"##;

#[pymethods]
impl CozoDbPy {
    #[new]
    fn new(kind: &str, path: &str) -> PyResult<Self> {
        match DbInstance::new(kind, path, Default::default()) {
            Ok(db) => Ok(Self { db: Some(db) }),
            Err(err) => Err(PyException::new_err(format!("{:?}", err))),
        }
    }
    pub fn run_query(&self, py: Python<'_>, query: &str, params: &str) -> String {
        if let Some(db) = &self.db {
            py.allow_threads(|| db.run_script_str(query, params))
        } else {
            DB_CLOSED_MSG.to_string()
        }
    }
    pub fn export_relations(&self, py: Python<'_>, rels: &str) -> String {
        if let Some(db) = &self.db {
            py.allow_threads(|| db.export_relations_str(rels))
        } else {
            DB_CLOSED_MSG.to_string()
        }
    }
    pub fn import_relation(&self, py: Python<'_>, data: &str) -> String {
        if let Some(db) = &self.db {
            py.allow_threads(|| db.import_relation_str(data))
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
    pub fn close(&mut self) -> bool {
        self.db.take().is_some()
    }
}

#[pymodule]
fn cozo_embedded(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_class::<CozoDbPy>()?;
    Ok(())
}
