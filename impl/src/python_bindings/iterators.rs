// ============================================================================
// Iterator Types
// ============================================================================

/// Iterator for PyTable - enables `for row in table:` syntax
#[pyclass(name = "TableIterator", unsendable)]
pub struct PyTableIterator {
    table: PyTable,
    index: usize,
    length: usize,
    /// Table version at iterator creation; used to detect mutation during iteration.
    start_version: u64,
}

#[pymethods]
impl PyTableIterator {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&mut self, py: Python) -> PyResult<Option<PyObject>> {
        if self.index >= self.length {
            return Ok(None);
        }
        if self.table.inner.borrow().version() != self.start_version {
            return Err(pyo3::exceptions::PyRuntimeError::new_err(
                "Table mutated during iteration",
            ));
        }
        let row = self.table.get_row(py, self.index)?;
        self.index += 1;
        Ok(Some(row))
    }
}

/// Iterator for PyFilterView
#[pyclass(name = "FilterViewIterator", unsendable)]
pub struct PyFilterViewIterator {
    view: Py<PyFilterView>,
    index: usize,
    length: usize,
    start_version: u64,
}

#[pymethods]
impl PyFilterViewIterator {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&mut self, py: Python) -> PyResult<Option<PyObject>> {
        if self.index >= self.length {
            return Ok(None);
        }
        let view = self.view.borrow(py);
        if view.table.inner.borrow().version() != self.start_version {
            return Err(pyo3::exceptions::PyRuntimeError::new_err(
                "Parent table mutated during iteration",
            ));
        }
        let row = view.get_row(py, self.index)?;
        self.index += 1;
        Ok(Some(row))
    }
}

/// Iterator for PyProjectionView
#[pyclass(name = "ProjectionViewIterator", unsendable)]
pub struct PyProjectionViewIterator {
    view: Py<PyProjectionView>,
    index: usize,
    length: usize,
    start_version: u64,
}

#[pymethods]
impl PyProjectionViewIterator {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&mut self, py: Python) -> PyResult<Option<PyObject>> {
        if self.index >= self.length {
            return Ok(None);
        }
        let view = self.view.borrow(py);
        if view.table.inner.borrow().version() != self.start_version {
            return Err(pyo3::exceptions::PyRuntimeError::new_err(
                "Parent table mutated during iteration",
            ));
        }
        let row = view.get_row(py, self.index)?;
        self.index += 1;
        Ok(Some(row))
    }
}

/// Iterator for PyComputedView
#[pyclass(name = "ComputedViewIterator", unsendable)]
pub struct PyComputedViewIterator {
    view: Py<PyComputedView>,
    index: usize,
    length: usize,
    start_version: u64,
}

#[pymethods]
impl PyComputedViewIterator {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&mut self, py: Python) -> PyResult<Option<PyObject>> {
        if self.index >= self.length {
            return Ok(None);
        }
        let view = self.view.borrow(py);
        if view.table.inner.borrow().version() != self.start_version {
            return Err(pyo3::exceptions::PyRuntimeError::new_err(
                "Parent table mutated during iteration",
            ));
        }
        let row = view.get_row(py, self.index)?;
        self.index += 1;
        Ok(Some(row))
    }
}

/// Iterator for PyJoinView
#[pyclass(name = "JoinViewIterator", unsendable)]
pub struct PyJoinViewIterator {
    view: Py<PyJoinView>,
    index: usize,
    length: usize,
}

#[pymethods]
impl PyJoinViewIterator {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&mut self, py: Python) -> PyResult<Option<PyObject>> {
        if self.index >= self.length {
            return Ok(None);
        }
        let view = self.view.borrow(py);
        let row = view.get_row(py, self.index)?;
        self.index += 1;
        Ok(Some(row))
    }
}

/// Iterator for PySortedView
#[pyclass(name = "SortedViewIterator", unsendable)]
pub struct PySortedViewIterator {
    view: Py<PySortedView>,
    index: usize,
    length: usize,
}

#[pymethods]
impl PySortedViewIterator {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&mut self, py: Python) -> PyResult<Option<PyObject>> {
        if self.index >= self.length {
            return Ok(None);
        }
        let view = self.view.borrow(py);
        let row = view.get_row(py, self.index)?;
        self.index += 1;
        Ok(Some(row))
    }
}

/// Iterator for PyAggregateView
#[pyclass(name = "AggregateViewIterator", unsendable)]
pub struct PyAggregateViewIterator {
    view: Py<PyAggregateView>,
    index: usize,
    length: usize,
}

#[pymethods]
impl PyAggregateViewIterator {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&mut self, py: Python) -> PyResult<Option<PyObject>> {
        if self.index >= self.length {
            return Ok(None);
        }
        let view = self.view.borrow(py);
        let row = view.get_row(py, self.index)?;
        self.index += 1;
        Ok(Some(row))
    }
}
