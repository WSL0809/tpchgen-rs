use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand};
use mysql::prelude::Queryable;
use mysql::{Conn, LocalInfileHandler, OptsBuilder};
use serde::Serialize;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;
use std::time::Instant;
use tpchgen_cli::{parse_csv_delimiter, OutputFormat, Table, TpchGenerator};

mod queries;
mod schema;

const TABLES: &[Table] = &[
    Table::Nation,
    Table::Region,
    Table::Part,
    Table::Supplier,
    Table::Partsupp,
    Table::Customer,
    Table::Orders,
    Table::Lineitem,
];

#[derive(Parser)]
#[command(name = "tpch-mysql")]
#[command(version)]
#[command(about = "Generate TPC-H data and load/run on MySQL")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Create TPC-H tables in MySQL
    Schema(MySqlArgs),

    /// Generate TPC-H data files (tab-delimited CSV with header by default)
    Gen(GenArgs),

    /// Load generated files into MySQL using LOAD DATA LOCAL INFILE (client-side)
    Load(LoadArgs),

    /// Run schema + gen + load (+ truncate) + run
    Bench(BenchArgs),

    /// Run TPC-H queries and record timings
    Run(RunArgs),

    /// Run schema + gen + load
    All(AllArgs),
}

#[derive(Parser, Clone)]
struct MySqlArgs {
    #[arg(long)]
    host: String,

    #[arg(long)]
    port: u16,

    #[arg(long)]
    user: String,

    #[arg(long)]
    password: String,

    #[arg(long)]
    database: String,
}

#[derive(Parser, Clone)]
struct GenArgs {
    /// Directory to write generated files into
    #[arg(long)]
    data_dir: PathBuf,

    /// TPC-H scale factor
    #[arg(long)]
    scale_factor: f64,

    /// Number of threads for generation (default: num CPUs)
    #[arg(long)]
    threads: Option<usize>,

    /// Output delimiter for CSV (default: \\t)
    #[arg(long, default_value = "\\t")]
    delimiter: String,
}

#[derive(Parser, Clone)]
struct LoadArgs {
    #[command(flatten)]
    mysql: MySqlArgs,

    /// Directory containing generated files
    #[arg(long)]
    data_dir: PathBuf,

    /// Comma-separated table list (default: all)
    #[arg(long, value_delimiter = ',')]
    tables: Option<Vec<Table>>,

    /// Ignore this many header lines (default: 1)
    #[arg(long, default_value_t = 1)]
    ignore_header_lines: u64,

    /// Truncate target tables before loading
    #[arg(long, default_value_t = false)]
    truncate: bool,

    /// Field terminator used in the generated files (default: \\t)
    #[arg(long, default_value = "\\t")]
    field_terminated_by: String,

    /// Line terminator used in the generated files (default: \\n)
    #[arg(long, default_value = "\\n")]
    line_terminated_by: String,
}

#[derive(Parser, Clone)]
struct BenchArgs {
    #[command(flatten)]
    mysql: MySqlArgs,

    #[arg(long)]
    data_dir: PathBuf,

    #[arg(long)]
    scale_factor: f64,

    #[arg(long)]
    threads: Option<usize>,

    /// Output delimiter for generated CSV (default: \\t)
    #[arg(long, default_value = "\\t")]
    delimiter: String,

    /// Ignore this many header lines when loading (default: 1)
    #[arg(long, default_value_t = 1)]
    ignore_header_lines: u64,

    /// Truncate target tables before loading (default: true)
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    truncate: bool,

    /// Per-query server-side timeout via MAX_EXECUTION_TIME (0 disables)
    #[arg(long, default_value_t = 0)]
    timeout_seconds: u64,

    /// Precheck queries on empty tables first (fails fast on errors)
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    precheck: bool,

    /// Write JSON results to this path (default: ./tpch_timings.json)
    #[arg(long, default_value = "tpch_timings.json")]
    output: PathBuf,

    /// Monitor this local PID's memory usage via /proc during each query (Linux only)
    #[arg(long)]
    monitor_pid: Option<u32>,
}

#[derive(Parser, Clone)]
struct AllArgs {
    #[command(flatten)]
    mysql: MySqlArgs,

    #[arg(long)]
    data_dir: PathBuf,

    #[arg(long)]
    scale_factor: f64,

    #[arg(long)]
    threads: Option<usize>,

    #[arg(long, value_delimiter = ',')]
    tables: Option<Vec<Table>>,

    #[arg(long, default_value_t = 1)]
    ignore_header_lines: u64,

    #[arg(long, default_value_t = false)]
    truncate: bool,

    #[arg(long, default_value = "\\t")]
    delimiter: String,
}

#[derive(Parser, Clone)]
struct RunArgs {
    #[command(flatten)]
    mysql: MySqlArgs,

    /// Precheck queries on empty tables first (fails fast on errors)
    ///
    /// This is recommended when validating query compatibility before benchmarking.
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    precheck: bool,

    /// Run Q1..Q22
    #[arg(long, default_value_t = false, conflicts_with = "query")]
    all: bool,

    /// Query ids (repeatable or comma-separated), e.g. --query 1 --query 6,19
    #[arg(long, action = clap::ArgAction::Append, value_delimiter = ',')]
    query: Vec<u32>,

    /// Per-query server-side timeout via MAX_EXECUTION_TIME (0 disables)
    #[arg(long, default_value_t = 0)]
    timeout_seconds: u64,

    /// Write JSON results to this path (default: stdout)
    #[arg(long)]
    output: Option<PathBuf>,

    /// Monitor this local PID's memory usage via /proc during each query (Linux only)
    #[arg(long)]
    monitor_pid: Option<u32>,
}

#[derive(Serialize)]
struct QueryResultRecord {
    query_id: u32,
    title: &'static str,
    ok: bool,
    seconds: f64,
    rows: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    monitor_pid: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    monitor_rss_start_kb: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    monitor_rss_end_kb: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    monitor_rss_peak_kb: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    monitor_samples: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    monitor_error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

#[derive(Debug, Default, Clone)]
struct ProcMemStats {
    rss_start_kb: u64,
    rss_end_kb: u64,
    rss_peak_kb: u64,
    samples: u64,
    error: Option<String>,
}

struct ProcRssSampler {
    pid: u32,
    stop: Arc<AtomicBool>,
    stats: Arc<Mutex<ProcMemStats>>,
    handle: Option<JoinHandle<()>>,
}

impl ProcRssSampler {
    fn start(pid: u32, interval: Duration) -> Result<Self> {
        let rss_start_kb =
            read_proc_rss_kb(pid).with_context(|| format!("read /proc/{pid}/status"))?;
        let stop = Arc::new(AtomicBool::new(false));
        let stats = Arc::new(Mutex::new(ProcMemStats {
            rss_start_kb,
            rss_end_kb: rss_start_kb,
            rss_peak_kb: rss_start_kb,
            samples: 1,
            error: None,
        }));

        let stop_thread = Arc::clone(&stop);
        let stats_thread = Arc::clone(&stats);
        let handle = std::thread::spawn(move || {
            while !stop_thread.load(Ordering::Relaxed) {
                std::thread::sleep(interval);
                match read_proc_rss_kb(pid) {
                    Ok(rss_kb) => {
                        if let Ok(mut s) = stats_thread.lock() {
                            s.rss_end_kb = rss_kb;
                            s.rss_peak_kb = s.rss_peak_kb.max(rss_kb);
                            s.samples += 1;
                        }
                    }
                    Err(e) => {
                        if let Ok(mut s) = stats_thread.lock() {
                            s.error = Some(format!("read /proc/{pid}/status: {e}"));
                        }
                        break;
                    }
                }
            }
        });

        Ok(Self {
            pid,
            stop,
            stats,
            handle: Some(handle),
        })
    }

    fn stop(mut self) -> ProcMemStats {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
        self.stats
            .lock()
            .map(|s| s.clone())
            .unwrap_or_else(|_| ProcMemStats {
                error: Some(format!("failed to lock sampler stats for pid {}", self.pid)),
                ..Default::default()
            })
    }
}

#[cfg(target_os = "linux")]
fn read_proc_rss_kb(pid: u32) -> std::io::Result<u64> {
    let status_path = format!("/proc/{pid}/status");
    let status = std::fs::read_to_string(status_path)?;
    for line in status.lines() {
        let line = line.trim_start();
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            let mut parts = rest.split_whitespace();
            let kb = parts.next().ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, "missing VmRSS value")
            })?;
            let kb: u64 = kb.parse().map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, "invalid VmRSS value")
            })?;
            return Ok(kb);
        }
    }
    Err(std::io::Error::new(
        std::io::ErrorKind::NotFound,
        "VmRSS not found in /proc/<pid>/status",
    ))
}

#[cfg(not(target_os = "linux"))]
fn read_proc_rss_kb(_pid: u32) -> std::io::Result<u64> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "--monitor-pid requires Linux /proc",
    ))
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Schema(args) => cmd_schema(args),
        Command::Gen(args) => cmd_gen(args).await,
        Command::Load(args) => cmd_load(args),
        Command::Bench(args) => cmd_bench(args).await,
        Command::Run(args) => cmd_run(args),
        Command::All(args) => cmd_all(args).await,
    }
}

fn connect_mysql(args: &MySqlArgs) -> Result<Conn> {
    let opts = OptsBuilder::new()
        .ip_or_hostname(Some(args.host.as_str()))
        .tcp_port(args.port)
        .user(Some(args.user.as_str()))
        .pass(Some(args.password.as_str()))
        .db_name(Some(args.database.as_str()));
    Conn::new(opts).context("connect mysql")
}

fn run_sql_script(conn: &mut Conn, sql: &str) -> Result<()> {
    for stmt in sql.split(';').map(str::trim).filter(|s| !s.is_empty()) {
        conn.query_drop(stmt)
            .with_context(|| format!("execute sql: {stmt}"))?;
    }
    Ok(())
}

fn mysql_string_literal(value: &str) -> String {
    // Conservative escaping for single-quoted string literals.
    value.replace('\\', "\\\\").replace('\'', "''")
}

fn mysql_delimiter_literal(value: &str) -> String {
    // Keep backslashes so users can pass `\t`, `\n`, `\x09` style values that MySQL parses.
    value.replace('\'', "''")
}

fn install_local_infile_handler(conn: &mut Conn, infile_map: HashMap<String, PathBuf>) {
    conn.set_local_infile_handler(Some(LocalInfileHandler::new(move |file_name, writer| {
        let file_name = std::str::from_utf8(file_name).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "non-utf8 local infile name",
            )
        })?;
        let file_path = infile_map.get(file_name).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                format!("unexpected local infile name: {file_name}"),
            )
        })?;

        let mut file = std::fs::File::open(file_path)?;
        std::io::copy(&mut file, writer)?;
        Ok(())
    })));
}

fn load_data_local_infile(
    conn: &mut Conn,
    table: &str,
    infile: &str,
    field_terminated_by: &str,
    line_terminated_by: &str,
    ignore_header_lines: u64,
) -> Result<()> {
    let infile = mysql_string_literal(infile);
    let fields = mysql_delimiter_literal(field_terminated_by);
    let lines = mysql_delimiter_literal(line_terminated_by);
    let sql = format!(
        "LOAD DATA LOCAL INFILE '{infile}' INTO TABLE `{table}` \
         FIELDS TERMINATED BY '{fields}' \
         LINES TERMINATED BY '{lines}' \
         IGNORE {ignore_header_lines} LINES"
    );
    conn.query_drop(sql).context("load data local infile")?;
    Ok(())
}

fn try_set_session_max_execution_time(conn: &mut Conn, milliseconds: Option<u64>) -> Result<()> {
    let Some(ms) = milliseconds else {
        return Ok(());
    };
    let sql = format!("SET SESSION max_execution_time={ms}");
    let _ = conn.query_drop(sql);
    Ok(())
}

fn is_select_statement(sql: &str) -> bool {
    let s = sql.trim_start().to_ascii_lowercase();
    s.starts_with("select") || s.starts_with("with")
}

fn apply_max_execution_time_hint(sql: &str, timeout_ms: Option<u64>) -> String {
    let Some(timeout_ms) = timeout_ms else {
        return sql.to_string();
    };
    let stripped = sql.trim_start();
    if stripped.len() < 6 || !stripped[..6].eq_ignore_ascii_case("select") {
        return sql.to_string();
    }
    let indent_len = sql.len() - stripped.len();
    let indent = &sql[..indent_len];
    format!(
        "{indent}select /*+ MAX_EXECUTION_TIME({timeout_ms}) */{}",
        &stripped[6..]
    )
}

fn table_file_name(table: Table) -> &'static str {
    match table {
        Table::Nation => "nation.csv",
        Table::Region => "region.csv",
        Table::Part => "part.csv",
        Table::Supplier => "supplier.csv",
        Table::Partsupp => "partsupp.csv",
        Table::Customer => "customer.csv",
        Table::Orders => "orders.csv",
        Table::Lineitem => "lineitem.csv",
    }
}

fn table_name(table: Table) -> &'static str {
    match table {
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

fn cmd_schema(args: MySqlArgs) -> Result<()> {
    let mut conn = connect_mysql(&args)?;
    run_sql_script(&mut conn, schema::SCHEMA_SQL)?;
    Ok(())
}

async fn cmd_gen(args: GenArgs) -> Result<()> {
    let delimiter = parse_csv_delimiter(&args.delimiter)
        .map_err(|e| anyhow::anyhow!(e))
        .context("parse --delimiter")?;
    let mut builder = TpchGenerator::builder()
        .with_scale_factor(args.scale_factor)
        .with_output_dir(args.data_dir)
        .with_format(OutputFormat::Csv)
        .with_csv_delimiter(delimiter);
    if let Some(threads) = args.threads {
        builder = builder.with_num_threads(threads);
    }
    builder.build().generate().await.context("generate tpch")
}

fn cmd_load(args: LoadArgs) -> Result<()> {
    let tables = args.tables.unwrap_or_else(|| TABLES.to_vec());
    let mut conn = connect_mysql(&args.mysql)?;
    let infile_map: HashMap<String, PathBuf> = tables
        .iter()
        .map(|table| {
            let file_name = table_file_name(*table).to_string();
            let file_path = args.data_dir.join(&file_name);
            (file_name, file_path)
        })
        .collect();
    install_local_infile_handler(&mut conn, infile_map);

    if args.truncate {
        for table in &tables {
            let sql = format!("TRUNCATE TABLE `{}`", table_name(*table));
            conn.query_drop(sql)
                .with_context(|| format!("truncate table {}", table_name(*table)))?;
        }
    }

    for table in &tables {
        let file_name = table_file_name(*table);
        let file_path = args.data_dir.join(file_name);
        load_data_local_infile(
            &mut conn,
            table_name(*table),
            file_name,
            &args.field_terminated_by,
            &args.line_terminated_by,
            args.ignore_header_lines,
        )
        .with_context(|| {
            format!(
                "load table {} from {}",
                table_name(*table),
                file_path.display()
            )
        })?;
    }
    Ok(())
}

fn cmd_run(args: RunArgs) -> Result<()> {
    if !args.all && args.query.is_empty() {
        bail!("either --all or --query must be set");
    }
    let query_ids: Vec<u32> = if args.all {
        (1..=22).collect()
    } else {
        args.query.clone()
    };
    for &qid in &query_ids {
        if queries::get_query(qid).is_none() {
            bail!("unknown query id: {qid}");
        }
    }

    let timeout_ms = if args.timeout_seconds == 0 {
        None
    } else {
        Some(args.timeout_seconds * 1000)
    };

    let mut conn = connect_mysql(&args.mysql)?;

    if args.precheck {
        match install_precheck_temp_schema(&mut conn) {
            Ok(()) => {
                precheck_queries(&mut conn, &query_ids, timeout_ms)?;
                drop_precheck_temp_schema(&mut conn).ok();
            }
            Err(e) if should_skip_precheck_due_to_temporary_table_syntax(&e) => {
                eprintln!(
                    "warning: precheck disabled because server does not support TEMPORARY TABLE; \
                     re-run with --no-precheck to silence this warning"
                );
            }
            Err(e) => return Err(e),
        }
    }

    let mut results: Vec<QueryResultRecord> = Vec::with_capacity(query_ids.len());
    for qid in query_ids {
        let q = queries::get_query(qid).expect("validated above");
        let start = Instant::now();
        let sampler = args
            .monitor_pid
            .map(|pid| ProcRssSampler::start(pid, Duration::from_millis(50)))
            .transpose()
            .with_context(|| {
                format!(
                    "start proc monitor (pid={})",
                    args.monitor_pid.expect("set above")
                )
            })?;
        let mut rows: u64 = 0;

        let mut record = QueryResultRecord {
            query_id: q.query_id,
            title: q.title,
            ok: false,
            seconds: 0.0,
            rows: 0,
            monitor_pid: args.monitor_pid,
            monitor_rss_start_kb: None,
            monitor_rss_end_kb: None,
            monitor_rss_peak_kb: None,
            monitor_samples: None,
            monitor_error: None,
            error: None,
        };

        let exec_res: Result<()> = (|| {
            try_set_session_max_execution_time(&mut conn, timeout_ms)?;
            for &stmt in q.statements {
                let stmt = apply_max_execution_time_hint(stmt, timeout_ms);
                if is_select_statement(&stmt) {
                    let result = conn.query_iter(stmt)?;
                    for row in result {
                        row?;
                        rows += 1;
                    }
                } else {
                    conn.query_drop(stmt)?;
                }
            }
            Ok(())
        })();

        record.seconds = start.elapsed().as_secs_f64();
        if let Some(sampler) = sampler {
            let stats = sampler.stop();
            record.monitor_rss_start_kb = Some(stats.rss_start_kb);
            record.monitor_rss_end_kb = Some(stats.rss_end_kb);
            record.monitor_rss_peak_kb = Some(stats.rss_peak_kb);
            record.monitor_samples = Some(stats.samples);
            record.monitor_error = stats.error;
        }
        match exec_res {
            Ok(()) => {
                record.ok = true;
                record.rows = rows;
            }
            Err(e) => {
                record.error = Some(e.to_string());
                record.rows = rows;
            }
        }

        results.push(record);
    }

    let json = serde_json::to_string_pretty(&results).context("serialize results")? + "\n";
    if let Some(path) = args.output {
        std::fs::write(&path, json).with_context(|| format!("write {}", path.display()))?;
    } else {
        print!("{json}");
    }
    Ok(())
}

fn should_skip_precheck_due_to_temporary_table_syntax(err: &anyhow::Error) -> bool {
    let mut found_syntax_1064 = false;
    let mut found_temporary = false;

    for cause in err.chain() {
        if let Some(mysql::Error::MySqlError(e)) = cause.downcast_ref::<mysql::Error>() {
            if e.code == 1064 && e.message.to_ascii_uppercase().contains("TEMPORARY") {
                return true;
            }
        }

        let msg = cause.to_string();
        if msg.contains("ERROR 1064") || msg.contains("error 1064") {
            found_syntax_1064 = true;
        }
        if msg.to_ascii_uppercase().contains("TEMPORARY") {
            found_temporary = true;
        }
    }

    found_syntax_1064 && found_temporary
}

async fn cmd_all(args: AllArgs) -> Result<()> {
    cmd_schema(args.mysql.clone())?;
    cmd_gen(GenArgs {
        data_dir: args.data_dir.clone(),
        scale_factor: args.scale_factor,
        threads: args.threads,
        delimiter: args.delimiter.clone(),
    })
    .await?;
    cmd_load(LoadArgs {
        mysql: args.mysql,
        data_dir: args.data_dir,
        tables: args.tables,
        ignore_header_lines: args.ignore_header_lines,
        truncate: args.truncate,
        field_terminated_by: args.delimiter,
        line_terminated_by: "\\n".to_string(),
    })?;
    Ok(())
}

fn install_precheck_temp_schema(conn: &mut Conn) -> Result<()> {
    for &table in TABLES {
        let name = table_name(table);
        conn.query_drop(format!("DROP TEMPORARY TABLE IF EXISTS `{name}`"))
            .with_context(|| format!("precheck: drop temporary table `{name}`"))?;
    }
    conn.query_drop("DROP TEMPORARY TABLE IF EXISTS revenue0")
        .ok();

    for stmt in schema::SCHEMA_SQL
        .split(';')
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        let sql = to_temporary_table_ddl(stmt);
        conn.query_drop(sql)
            .with_context(|| format!("precheck: install temporary schema: {stmt}"))?;
    }
    Ok(())
}

fn drop_precheck_temp_schema(conn: &mut Conn) -> Result<()> {
    for &table in TABLES {
        let name = table_name(table);
        conn.query_drop(format!("DROP TEMPORARY TABLE IF EXISTS `{name}`"))
            .with_context(|| format!("precheck: drop temporary table `{name}`"))?;
    }
    conn.query_drop("DROP TEMPORARY TABLE IF EXISTS revenue0")
        .ok();
    Ok(())
}

fn to_temporary_table_ddl(stmt: &str) -> String {
    let trimmed = stmt.trim_start();
    if !trimmed.to_ascii_lowercase().starts_with("create table") {
        return stmt.to_string();
    }
    let indent_len = stmt.len() - trimmed.len();
    let indent = &stmt[..indent_len];
    format!("{indent}CREATE TEMPORARY TABLE{}", &trimmed[12..])
}

fn precheck_queries(conn: &mut Conn, query_ids: &[u32], timeout_ms: Option<u64>) -> Result<()> {
    for &qid in query_ids {
        let q = queries::get_query(qid).expect("validated above");
        for (stmt_idx, &stmt) in q.statements.iter().enumerate() {
            let stmt = apply_max_execution_time_hint(stmt, timeout_ms);
            let stmt = precheck_rewrite_statement(qid, &stmt);
            try_set_session_max_execution_time(conn, timeout_ms).ok();

            let res: Result<()> = if is_select_statement(&stmt) {
                let result = conn
                    .query_iter(stmt)
                    .with_context(|| format!("precheck: Q{qid} stmt#{stmt_idx}"))?;
                for row in result {
                    row?;
                }
                Ok(())
            } else {
                conn.query_drop(stmt)
                    .with_context(|| format!("precheck: Q{qid} stmt#{stmt_idx}"))?;
                Ok(())
            };

            if let Err(e) = res {
                bail!("precheck failed (Q{qid} - {}): {e}", q.title);
            }
        }
    }
    Ok(())
}

fn precheck_rewrite_statement(query_id: u32, stmt: &str) -> String {
    if query_id != 15 {
        return stmt.to_string();
    }
    let s = stmt.trim_start();
    let lower = s.to_ascii_lowercase();
    if lower.starts_with("drop view") {
        return "DROP TEMPORARY TABLE IF EXISTS revenue0".to_string();
    }
    if lower.starts_with("create view") {
        // Replace "create view" with "create temporary table" for precheck. Views cannot reliably
        // target temporary tables across MySQL versions, but temporary tables work here.
        let indent_len = stmt.len() - s.len();
        let indent = &stmt[..indent_len];
        return format!("{indent}CREATE TEMPORARY TABLE{}", &s[11..]);
    }
    stmt.to_string()
}

async fn cmd_bench(args: BenchArgs) -> Result<()> {
    cmd_schema(args.mysql.clone())?;
    cmd_gen(GenArgs {
        data_dir: args.data_dir.clone(),
        scale_factor: args.scale_factor,
        threads: args.threads,
        delimiter: args.delimiter.clone(),
    })
    .await?;
    cmd_load(LoadArgs {
        mysql: args.mysql.clone(),
        data_dir: args.data_dir,
        tables: None,
        ignore_header_lines: args.ignore_header_lines,
        truncate: args.truncate,
        field_terminated_by: args.delimiter,
        line_terminated_by: "\\n".to_string(),
    })?;
    cmd_run(RunArgs {
        mysql: args.mysql,
        precheck: args.precheck,
        all: true,
        query: vec![],
        timeout_seconds: args.timeout_seconds,
        output: Some(args.output),
        monitor_pid: args.monitor_pid,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_apply_max_execution_time_hint() {
        let sql = "  select 1";
        let out = apply_max_execution_time_hint(sql, Some(123));
        assert!(out.contains("MAX_EXECUTION_TIME(123)"));
        assert!(out.starts_with("  select "));
    }

    #[test]
    fn test_is_select_statement() {
        assert!(is_select_statement("select 1"));
        assert!(is_select_statement(" with t as (select 1) select * from t"));
        assert!(!is_select_statement("create view v as select 1"));
    }
}
