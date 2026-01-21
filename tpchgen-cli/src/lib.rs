//! TPC-H Data Generator Library
//!
//! This crate provides both a command-line tool and a library for generating
//! TPC-H benchmark data in various formats (TBL, CSV).
//!
//! # Examples
//!
//! ```no_run
//! use tpchgen_cli::{TpchGenerator, Table, OutputFormat};
//! use std::path::PathBuf;
//!
//! # async fn example() -> std::io::Result<()> {
//! let generator = TpchGenerator::builder()
//!     .with_scale_factor(10.0)
//!     .with_output_dir(PathBuf::from("./data"))
//!     .with_tables(vec![Table::Customer, Table::Orders])
//!     .with_format(OutputFormat::Csv)
//!     .with_num_threads(8)
//!     .build();
//!
//! generator.generate().await?;
//! # Ok(())
//! # }
//! ```

pub use crate::plan::GenerationPlan;

pub mod csv;
pub mod generate;
pub mod output_plan;
pub mod plan;
pub mod runner;
pub mod statistics;
pub mod tbl;

use crate::generate::Sink;
use crate::statistics::WriteStatistics;
use std::fmt::Display;
use std::io::{self, Write};
use std::str::FromStr;

/// Wrapper around a buffer writer that counts the number of buffers and bytes written
pub struct WriterSink<W: Write> {
    statistics: WriteStatistics,
    inner: W,
}

impl<W: Write> WriterSink<W> {
    pub fn new(inner: W) -> Self {
        Self {
            inner,
            statistics: WriteStatistics::new("buffers"),
        }
    }
}

impl<W: Write + Send> Sink for WriterSink<W> {
    fn sink(&mut self, buffer: &[u8]) -> Result<(), io::Error> {
        self.statistics.increment_chunks(1);
        self.statistics.increment_bytes(buffer.len());
        self.inner.write_all(buffer)
    }

    fn flush(mut self) -> Result<(), io::Error> {
        self.inner.flush()
    }
}

/// TPC-H table types
///
/// Represents the 8 tables in the TPC-H benchmark schema.
/// Tables are ordered by size (smallest to largest at SF=1).
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum Table {
    /// Nation table (25 rows)
    Nation,
    /// Region table (5 rows)
    Region,
    /// Part table (200,000 rows at SF=1)
    Part,
    /// Supplier table (10,000 rows at SF=1)
    Supplier,
    /// Part-Supplier relationship table (800,000 rows at SF=1)
    Partsupp,
    /// Customer table (150,000 rows at SF=1)
    Customer,
    /// Orders table (1,500,000 rows at SF=1)
    Orders,
    /// Line item table (6,000,000 rows at SF=1)
    Lineitem,
}

impl Display for Table {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}

impl FromStr for Table {
    type Err = &'static str;

    /// Returns the table enum value from the given string full name or abbreviation
    ///
    /// The original dbgen tool allows some abbreviations to mean two different tables
    /// like 'p' which aliases to both 'part' and 'partsupp'. This implementation does
    /// not support this since it just adds unnecessary complexity and confusion so we
    /// only support the exclusive abbreviations.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "n" | "nation" => Ok(Table::Nation),
            "r" | "region" => Ok(Table::Region),
            "s" | "supplier" => Ok(Table::Supplier),
            "P" | "part" => Ok(Table::Part),
            "S" | "partsupp" => Ok(Table::Partsupp),
            "c" | "customer" => Ok(Table::Customer),
            "O" | "orders" => Ok(Table::Orders),
            "L" | "lineitem" => Ok(Table::Lineitem),
            _ => Err("Invalid table name {s}"),
        }
    }
}

impl Table {
    fn name(&self) -> &'static str {
        match self {
            Table::Nation => "nation",
            Table::Region => "region",
            Table::Part => "part",
            Table::Supplier => "supplier",
            Table::Partsupp => "partsupp",
            Table::Customer => "customer",
            Table::Orders => "orders",
            Table::Lineitem => "lineitem",
        }
    }
}

/// Output format for generated data
///
/// # Format Details
///
/// - **TBL**: Pipe-delimited format compatible with original dbgen tool
/// - **CSV**: Comma-separated values with proper escaping
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum OutputFormat {
    /// TBL format (pipe-delimited, dbgen-compatible)
    Tbl,
    /// CSV format (comma-separated values)
    Csv,
}

impl FromStr for OutputFormat {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "tbl" => Ok(OutputFormat::Tbl),
            "csv" => Ok(OutputFormat::Csv),
            _ => Err(format!(
                "Invalid output format: {s}. Valid formats are: tbl, csv"
            )),
        }
    }
}

impl Display for OutputFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OutputFormat::Tbl => write!(f, "tbl"),
            OutputFormat::Csv => write!(f, "csv"),
        }
    }
}

/// Parses a delimiter value for CSV output.
///
/// Accepts a single ASCII character (including a literal tab) or common escape sequences:
/// `\\t`, `\\n`, `\\r`, `\\\\`, and `\\xNN` (hex).
pub fn parse_csv_delimiter(value: &str) -> Result<u8, String> {
    if value.is_empty() {
        return Err("CSV delimiter must not be empty".to_string());
    }

    let delimiter = if value.len() == 1 {
        value.as_bytes()[0]
    } else if value.starts_with('\\') {
        match value.as_bytes() {
            [b'\\', b't'] => b'\t',
            [b'\\', b'n'] => b'\n',
            [b'\\', b'r'] => b'\r',
            [b'\\', b'\\'] => b'\\',
            [b'\\', b'x', hi, lo] => {
                fn hex(b: u8) -> Option<u8> {
                    match b {
                        b'0'..=b'9' => Some(b - b'0'),
                        b'a'..=b'f' => Some(b - b'a' + 10),
                        b'A'..=b'F' => Some(b - b'A' + 10),
                        _ => None,
                    }
                }
                let Some(hi) = hex(*hi) else {
                    return Err(format!("Invalid CSV delimiter escape: {value}"));
                };
                let Some(lo) = hex(*lo) else {
                    return Err(format!("Invalid CSV delimiter escape: {value}"));
                };
                hi << 4 | lo
            }
            _ => return Err(format!("Invalid CSV delimiter escape: {value}")),
        }
    } else {
        let mut chars = value.chars();
        let Some(c) = chars.next() else {
            return Err("CSV delimiter must not be empty".to_string());
        };
        if chars.next().is_some() {
            return Err("CSV delimiter must be a single character".to_string());
        }
        if !c.is_ascii() {
            return Err("CSV delimiter must be an ASCII character".to_string());
        }
        c as u8
    };

    match delimiter {
        0 => Err("CSV delimiter cannot be NUL".to_string()),
        b'\n' | b'\r' => Err("CSV delimiter cannot be a newline character".to_string()),
        b'"' => Err("CSV delimiter cannot be '\"'".to_string()),
        _ => Ok(delimiter),
    }
}

/// Configuration for TPC-H data generation
///
/// This struct holds all the parameters needed to generate TPC-H benchmark data.
/// It's typically not constructed directly - use [`TpchGeneratorBuilder`] instead.
///
/// # Examples
///
/// ```no_run
/// use tpchgen_cli::{GeneratorConfig, OutputFormat};
///
/// // Usually you would use TpchGenerator::builder() instead
/// let config = GeneratorConfig {
///     scale_factor: 10.0,
///     ..Default::default()
/// };
/// ```
#[derive(Debug, Clone)]
pub struct GeneratorConfig {
    /// Scale factor (e.g., 1.0 for 1GB, 10.0 for 10GB)
    pub scale_factor: f64,
    /// Output directory for generated files
    pub output_dir: std::path::PathBuf,
    /// Tables to generate (if None, generates all tables)
    pub tables: Option<Vec<Table>>,
    /// Output format (TBL, or CSV)
    pub format: OutputFormat,
    /// Delimiter byte for CSV output (default: `,`)
    pub csv_delimiter: u8,
    /// Number of threads for parallel generation
    pub num_threads: usize,
    /// Number of partitions to generate (if None, generates a single file per table)
    pub parts: Option<i32>,
    /// Specific partition to generate (1-based, requires parts to be set)
    pub part: Option<i32>,
    /// Write output to stdout instead of files
    pub stdout: bool,
}

impl Default for GeneratorConfig {
    fn default() -> Self {
        Self {
            scale_factor: 1.0,
            output_dir: std::path::PathBuf::from("."),
            tables: None,
            format: OutputFormat::Tbl,
            csv_delimiter: b',',
            num_threads: num_cpus::get(),
            parts: None,
            part: None,
            stdout: false,
        }
    }
}

/// TPC-H data generator
///
/// The main entry point for generating TPC-H benchmark data.
/// Use the builder pattern via [`TpchGenerator::builder()`] to configure and create instances.
///
/// # Examples
///
/// ```no_run
/// use tpchgen_cli::{TpchGenerator, Table, OutputFormat};
/// use std::path::PathBuf;
/// # async fn example() -> std::io::Result<()> {
/// // Generate all tables at scale factor 1 in TBL format
/// TpchGenerator::builder()
///     .with_scale_factor(1.0)
///     .with_output_dir(PathBuf::from("./data"))
///     .build()
///     .generate()
///     .await?;
///
/// # Ok(())
/// # }
/// ```
pub struct TpchGenerator {
    config: GeneratorConfig,
}

impl TpchGenerator {
    /// Create a new builder for configuring the generator
    ///
    /// This is the recommended way to construct a [`TpchGenerator`].
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use tpchgen_cli::TpchGenerator;
    ///
    /// let generator = TpchGenerator::builder()
    ///     .with_scale_factor(1.0)
    ///     .build();
    /// ```
    pub fn builder() -> TpchGeneratorBuilder {
        TpchGeneratorBuilder::new()
    }

    /// Generate TPC-H data with the configured settings
    ///
    /// This async method performs the actual data generation, creating files
    /// in the configured output directory (or writing to stdout if configured).
    ///
    /// # Returns
    ///
    /// - `Ok(())` on successful generation
    /// - `Err(io::Error)` if file I/O or generation fails
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use tpchgen_cli::TpchGenerator;
    ///
    /// # async fn example() -> std::io::Result<()> {
    /// TpchGenerator::builder()
    ///     .with_scale_factor(1.0)
    ///     .build()
    ///     .generate()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn generate(self) -> io::Result<()> {
        use crate::output_plan::OutputPlanGenerator;
        use crate::runner::PlanRunner;
        use log::info;
        use std::time::Instant;
        use tpchgen::distribution::Distributions;
        use tpchgen::text::TextPool;

        let config = self.config;

        // Create output directory if it doesn't exist and we are not writing to stdout
        if !config.stdout {
            std::fs::create_dir_all(&config.output_dir)?;
        }

        // Determine which tables to generate
        let tables: Vec<Table> = if let Some(tables) = config.tables {
            tables
        } else {
            vec![
                Table::Nation,
                Table::Region,
                Table::Part,
                Table::Supplier,
                Table::Partsupp,
                Table::Customer,
                Table::Orders,
                Table::Lineitem,
            ]
        };

        // Determine what files to generate
        let mut output_plan_generator = OutputPlanGenerator::new(
            config.format,
            config.scale_factor,
            config.stdout,
            config.output_dir,
        );

        for table in tables {
            output_plan_generator.generate_plans(table, config.part, config.parts)?;
        }
        let output_plans = output_plan_generator.build();

        // Force the creation of the distributions and text pool so it doesn't
        // get charged to the first table
        let start = Instant::now();
        Distributions::static_default();
        TextPool::get_or_init_default();
        let elapsed = start.elapsed();
        info!("Created static distributions and text pools in {elapsed:?}");

        // Run
        let runner = PlanRunner::new(output_plans, config.num_threads, config.csv_delimiter);
        runner.run().await?;
        info!("Generation complete!");
        Ok(())
    }
}

/// Builder for constructing a [`TpchGenerator`]
///
/// Provides a fluent interface for configuring TPC-H data generation parameters.
/// All builder methods can be chained, and calling [`build()`](TpchGeneratorBuilder::build)
/// produces a [`TpchGenerator`] ready to generate data.
///
/// # Defaults
///
/// - Scale factor: 1.0
/// - Output directory: current directory (".")
/// - Tables: all 8 tables
/// - Format: TBL
/// - Threads: number of CPUs
///
/// # Examples
///
/// ```no_run
/// use tpchgen_cli::{TpchGenerator, Table, OutputFormat};
/// use std::path::PathBuf;
///
/// # async fn example() -> std::io::Result<()> {
/// let generator = TpchGenerator::builder()
///     .with_scale_factor(100.0)
///     .with_output_dir(PathBuf::from("/data/tpch"))
///     .with_tables(vec![Table::Lineitem, Table::Orders])
///     .with_format(OutputFormat::Csv)
///     .with_num_threads(32)
///     .build();
///
/// generator.generate().await?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct TpchGeneratorBuilder {
    config: GeneratorConfig,
}

impl TpchGeneratorBuilder {
    /// Create a new builder with default configuration
    ///
    /// # Examples
    ///
    /// ```
    /// use tpchgen_cli::TpchGeneratorBuilder;
    ///
    /// let builder = TpchGeneratorBuilder::new();
    /// ```
    pub fn new() -> Self {
        Self {
            config: GeneratorConfig::default(),
        }
    }

    /// Returns the scale factor.
    pub fn scale_factor(&self) -> f64 {
        self.config.scale_factor
    }

    /// Set the scale factor (e.g., 1.0 for 1GB, 10.0 for 10GB)
    pub fn with_scale_factor(mut self, scale_factor: f64) -> Self {
        self.config.scale_factor = scale_factor;
        self
    }

    /// Set the output directory
    pub fn with_output_dir(mut self, output_dir: impl Into<std::path::PathBuf>) -> Self {
        self.config.output_dir = output_dir.into();
        self
    }

    /// Set which tables to generate (default: all tables)
    pub fn with_tables(mut self, tables: Vec<Table>) -> Self {
        self.config.tables = Some(tables);
        self
    }

    /// Set the output format (default: TBL)
    pub fn with_format(mut self, format: OutputFormat) -> Self {
        self.config.format = format;
        self
    }

    /// Set delimiter byte for CSV output (default: `,`)
    pub fn with_csv_delimiter(mut self, csv_delimiter: u8) -> Self {
        self.config.csv_delimiter = csv_delimiter;
        self
    }

    /// Set the number of threads for parallel generation (default: number of CPUs)
    pub fn with_num_threads(mut self, num_threads: usize) -> Self {
        self.config.num_threads = num_threads;
        self
    }

    /// Set the number of partitions to generate
    pub fn with_parts(mut self, parts: i32) -> Self {
        self.config.parts = Some(parts);
        self
    }

    /// Set the specific partition to generate (1-based, requires parts to be set)
    pub fn with_part(mut self, part: i32) -> Self {
        self.config.part = Some(part);
        self
    }

    /// Write output to stdout instead of files
    pub fn with_stdout(mut self, stdout: bool) -> Self {
        self.config.stdout = stdout;
        self
    }

    /// Build the [`TpchGenerator`] with the configured settings
    pub fn build(self) -> TpchGenerator {
        TpchGenerator {
            config: self.config,
        }
    }
}

impl Default for TpchGeneratorBuilder {
    fn default() -> Self {
        Self::new()
    }
}
