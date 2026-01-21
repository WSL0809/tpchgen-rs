use pyo3::exceptions::{PyOSError, PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use std::path::PathBuf;
use std::str::FromStr;
use tpchgen_cli::{parse_csv_delimiter, OutputFormat, Table, TpchGenerator};

#[pyfunction]
#[pyo3(
    signature = (
        scale_factor,
        output_dir,
        tables=None,
        format="tbl",
        delimiter=",",
        parts=None,
        part=None,
        num_threads=None,
    )
)]
fn generate(
    py: Python<'_>,
    scale_factor: f64,
    output_dir: PathBuf,
    tables: Option<Vec<String>>,
    format: &str,
    delimiter: &str,
    parts: Option<i32>,
    part: Option<i32>,
    num_threads: Option<usize>,
) -> PyResult<()> {
    let format = OutputFormat::from_str(format)
        .map_err(|e| PyValueError::new_err(format!("Invalid format: {e}")))?;

    if let Some(part) = part {
        let Some(parts) = parts else {
            return Err(PyValueError::new_err(
                "part requires parts to be set (1-based)",
            ));
        };
        if parts <= 0 {
            return Err(PyValueError::new_err("parts must be > 0"));
        }
        if part <= 0 || part > parts {
            return Err(PyValueError::new_err(
                "part must be in the range [1, parts]",
            ));
        }
    } else if let Some(parts) = parts {
        if parts <= 0 {
            return Err(PyValueError::new_err("parts must be > 0"));
        }
    }

    let csv_delimiter = if format == OutputFormat::Csv {
        parse_csv_delimiter(delimiter).map_err(PyValueError::new_err)?
    } else {
        b','
    };

    let tables = tables
        .map(|tables| {
            tables
                .into_iter()
                .map(|t| Table::from_str(&t).map_err(|_| t))
                .collect::<Result<Vec<_>, _>>()
        })
        .transpose()
        .map_err(|table| PyValueError::new_err(format!("Invalid table name: {table}")))?;

    let mut builder = TpchGenerator::builder()
        .with_scale_factor(scale_factor)
        .with_output_dir(output_dir)
        .with_format(format)
        .with_csv_delimiter(csv_delimiter);

    if let Some(tables) = tables {
        builder = builder.with_tables(tables);
    }

    if let Some(num_threads) = num_threads {
        builder = builder.with_num_threads(num_threads);
    }

    if let Some(parts) = parts {
        builder = builder.with_parts(parts);
    }

    if let Some(part) = part {
        builder = builder.with_part(part);
    }

    let generator = builder.build();

    enum GenerateError {
        Runtime(String),
        Io(std::io::Error),
    }

    let result: Result<(), GenerateError> = py.allow_threads(|| {
        let mut runtime = tokio::runtime::Builder::new_multi_thread();
        runtime.enable_all();
        if let Some(num_threads) = num_threads {
            runtime.worker_threads(num_threads);
        }
        let runtime = runtime
            .build()
            .map_err(|e| GenerateError::Runtime(e.to_string()))?;
        runtime
            .block_on(generator.generate())
            .map_err(GenerateError::Io)
    });

    match result {
        Ok(()) => Ok(()),
        Err(GenerateError::Io(e)) => Err(PyOSError::new_err(e.to_string())),
        Err(GenerateError::Runtime(e)) => Err(PyRuntimeError::new_err(e)),
    }
}

#[pymodule]
fn tpchgen(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(generate, m)?)?;
    Ok(())
}
