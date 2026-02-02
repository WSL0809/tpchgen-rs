use assert_cmd::Command;
use std::fs;
use std::fs::File;
use std::io::Read;
use std::path::Path;
use tempfile::tempdir;

/// Test TBL output for scale factor 0.001 using tpchgen-cli
#[test]
fn test_tpchgen_cli_tbl_scale_factor_0_001() {
    let temp_dir = tempdir().expect("Failed to create temporary directory");

    // Run the tpchgen-cli command
    Command::cargo_bin("tpchgen-cli")
        .expect("Binary not found")
        .arg("--scale-factor")
        .arg("0.001")
        .arg("--output-dir")
        .arg(temp_dir.path())
        .assert()
        .success();

    // List of expected files
    let expected_files = vec![
        "customer.tbl",
        "lineitem.tbl",
        "nation.tbl",
        "orders.tbl",
        "part.tbl",
        "partsupp.tbl",
        "region.tbl",
        "supplier.tbl",
    ];

    // Verify that all expected files are created
    for file in &expected_files {
        let generated_file = temp_dir.path().join(file);
        assert!(
            generated_file.exists(),
            "File {:?} does not exist",
            generated_file
        );
        let generated_contents = fs::read(generated_file).expect("Failed to read generated file");
        let generated_contents = String::from_utf8(generated_contents)
            .expect("Failed to convert generated contents to string");

        // load the reference file
        let reference_file = format!("../tpchgen/data/sf-0.001/{}.gz", file);
        let reference_contents = match read_gzipped_file_to_string(&reference_file) {
            Ok(contents) => contents,
            Err(e) => {
                panic!("Failed to read reference file {reference_file}: {e}");
            }
        };

        assert_eq!(
            generated_contents, reference_contents,
            "Contents of {:?} do not match reference",
            file
        );
    }
}

/// Test CSV output using a tab delimiter
#[test]
fn test_tpchgen_cli_csv_tab_delimiter() {
    let temp_dir = tempdir().expect("Failed to create temporary directory");

    Command::cargo_bin("tpchgen-cli")
        .expect("Binary not found")
        .arg("--scale-factor")
        .arg("0.001")
        .arg("--tables")
        .arg("nation")
        .arg("--format")
        .arg("csv")
        // Simulate shell usage: --delimiter '\t'
        .arg("--delimiter")
        .arg("\\t")
        .arg("--output-dir")
        .arg(temp_dir.path())
        .assert()
        .success();

    let path = temp_dir.path().join("nation.csv");
    let contents = fs::read_to_string(path).expect("Failed to read generated csv file");
    let mut lines = contents.lines();

    let header = lines.next().expect("Missing header line");
    assert!(header.contains('\t'), "Expected tab-separated header");
    assert!(
        !header.contains(','),
        "Expected header to not contain commas: {header}"
    );

    let first_row = lines.next().expect("Missing first data row");
    assert_eq!(
        first_row.matches('\t').count(),
        3,
        "Expected 4 columns in nation row: {first_row}"
    );
}

/// Test that when creating output, if the file already exists it is not overwritten
#[test]
fn test_tpchgen_cli_tbl_no_overwrite() {
    let temp_dir = tempdir().expect("Failed to create temporary directory");
    let expected_file = temp_dir.path().join("part.tbl");

    // First run - create the file
    Command::cargo_bin("tpchgen-cli")
        .expect("Binary not found")
        .arg("--scale-factor")
        .arg("0.001")
        .arg("--tables")
        .arg("part")
        .arg("--output-dir")
        .arg(temp_dir.path())
        .assert()
        .success();

    let original_metadata =
        fs::metadata(&expected_file).expect("Failed to get metadata of generated file");
    assert_eq!(original_metadata.len(), 23498);

    // Run the tpchgen-cli command again with the same parameters and expect the
    // file to not be overwritten and a warning to be logged
    let output = Command::cargo_bin("tpchgen-cli")
        .expect("Binary not found")
        .arg("--scale-factor")
        .arg("0.001")
        .arg("--tables")
        .arg("part")
        .arg("--output-dir")
        .arg(temp_dir.path())
        .arg("--overwrite=false")
        .assert()
        .success();

    let stderr = String::from_utf8_lossy(&output.get_output().stderr);
    assert!(
        stderr.contains("already exists, skipping generation"),
        "Expected warning message not found in stderr: {}",
        stderr
    );

    let new_metadata =
        fs::metadata(&expected_file).expect("Failed to get metadata of generated file");
    assert_eq!(original_metadata.len(), new_metadata.len());
    assert_eq!(
        original_metadata
            .modified()
            .expect("Failed to get modified time"),
        new_metadata
            .modified()
            .expect("Failed to get modified time")
    );
}

/// Test that when creating output, if the file already exists it can be overwritten
#[test]
fn test_tpchgen_cli_tbl_overwrite() {
    let temp_dir = tempdir().expect("Failed to create temporary directory");
    let expected_file = temp_dir.path().join("part.tbl");

    // First run - create the file
    Command::cargo_bin("tpchgen-cli")
        .expect("Binary not found")
        .arg("--scale-factor")
        .arg("0.001")
        .arg("--tables")
        .arg("part")
        .arg("--output-dir")
        .arg(temp_dir.path())
        .assert()
        .success();

    // Corrupt the file and ensure overwrite restores it.
    fs::write(&expected_file, "THIS_IS_NOT_TPCH").expect("Failed to corrupt generated file");

    let output = Command::cargo_bin("tpchgen-cli")
        .expect("Binary not found")
        .arg("--scale-factor")
        .arg("0.001")
        .arg("--tables")
        .arg("part")
        .arg("--output-dir")
        .arg(temp_dir.path())
        .arg("--overwrite=true")
        .assert()
        .success();

    let stderr = String::from_utf8_lossy(&output.get_output().stderr);
    assert!(
        !stderr.contains("already exists, skipping generation"),
        "Expected no skip message with --overwrite, but found: {}",
        stderr
    );

    let new_metadata =
        fs::metadata(&expected_file).expect("Failed to get metadata of generated file");
    assert_eq!(new_metadata.len(), 23498);

    let mut contents = String::new();
    File::open(&expected_file)
        .expect("Failed to open generated file")
        .read_to_string(&mut contents)
        .expect("Failed to read generated file");
    assert!(
        !contents.contains("THIS_IS_NOT_TPCH"),
        "Expected overwritten file to not contain sentinel"
    );
}

/// Test that --quiet flag suppresses stdout output
#[test]
fn test_tpchgen_cli_quiet_flag() {
    let temp_dir = tempdir().expect("Failed to create temporary directory");
    let expected_file = temp_dir.path().join("part.tbl");

    // First run - create the file
    Command::cargo_bin("tpchgen-cli")
        .expect("Binary not found")
        .arg("--scale-factor")
        .arg("0.001")
        .arg("--tables")
        .arg("part")
        .arg("--output-dir")
        .arg(temp_dir.path())
        .assert()
        .success();

    let original_metadata =
        fs::metadata(&expected_file).expect("Failed to get metadata of generated file");
    assert_eq!(original_metadata.len(), 23498);

    // Run the tpchgen-cli command again with --quiet flag
    // Expect the file to not be overwritten and NO warning even though warnings show by default
    let output = Command::cargo_bin("tpchgen-cli")
        .expect("Binary not found")
        .arg("--scale-factor")
        .arg("0.001")
        .arg("--tables")
        .arg("part")
        .arg("--output-dir")
        .arg(temp_dir.path())
        .arg("--overwrite=false")
        .arg("--quiet")
        .assert()
        .success();

    let stderr = String::from_utf8_lossy(&output.get_output().stderr);
    assert!(
        !stderr.contains("already exists"),
        "Expected no warning message in stderr with --quiet flag, but found: {}",
        stderr
    );

    // Verify file was not overwritten
    let new_metadata =
        fs::metadata(&expected_file).expect("Failed to get metadata of generated file");
    assert_eq!(original_metadata.len(), new_metadata.len());
    assert_eq!(
        original_metadata
            .modified()
            .expect("Failed to get modified time"),
        new_metadata
            .modified()
            .expect("Failed to get modified time")
    );
}

/// Test generating the order table using 4 parts implicitly
#[test]
fn test_tpchgen_cli_parts() {
    let temp_dir = tempdir().expect("Failed to create temporary directory");

    // generate 4 parts of the orders table with scale factor 0.001 and let
    // tpchgen-cli generate the multiple files

    let num_parts = 4;
    let output_dir = temp_dir.path().to_path_buf();
    Command::cargo_bin("tpchgen-cli")
        .expect("Binary not found")
        .arg("--scale-factor")
        .arg("0.001")
        .arg("--output-dir")
        .arg(&output_dir)
        .arg("--parts")
        .arg(num_parts.to_string())
        .arg("--tables")
        .arg("orders")
        .assert()
        .success();

    verify_table(temp_dir.path(), "orders", num_parts, "0.001");
}

/// Test generating the order table with multiple invocations using --parts and
/// --part options
#[test]
fn test_tpchgen_cli_parts_explicit() {
    let temp_dir = tempdir().expect("Failed to create temporary directory");

    // generate 4 parts of the orders table with scale factor 0.001
    // use threads to run the command concurrently to minimize the time taken
    let num_parts = 4;
    let mut threads = vec![];
    for part in 1..=num_parts {
        let output_dir = temp_dir.path().to_path_buf();
        threads.push(std::thread::spawn(move || {
            // Run the tpchgen-cli command for each part
            // output goes into `output_dir/orders/orders.{part}.tbl`
            Command::cargo_bin("tpchgen-cli")
                .expect("Binary not found")
                .arg("--scale-factor")
                .arg("0.001")
                .arg("--output-dir")
                .arg(&output_dir)
                .arg("--parts")
                .arg(num_parts.to_string())
                .arg("--part")
                .arg(part.to_string())
                .arg("--tables")
                .arg("orders")
                .assert()
                .success();
        }));
    }
    // Wait for all threads to finish
    for thread in threads {
        thread.join().expect("Thread panicked");
    }
    verify_table(temp_dir.path(), "orders", num_parts, "0.001");
}

/// Create all tables using --parts option and verify the output layouts
#[test]
fn test_tpchgen_cli_parts_all_tables() {
    let temp_dir = tempdir().expect("Failed to create temporary directory");

    let num_parts = 8;
    let output_dir = temp_dir.path().to_path_buf();
    Command::cargo_bin("tpchgen-cli")
        .expect("Binary not found")
        .arg("--scale-factor")
        .arg("0.001")
        .arg("--output-dir")
        .arg(&output_dir)
        .arg("--parts")
        .arg(num_parts.to_string())
        .assert()
        .success();

    verify_table(temp_dir.path(), "lineitem", num_parts, "0.001");
    verify_table(temp_dir.path(), "orders", num_parts, "0.001");
    verify_table(temp_dir.path(), "part", num_parts, "0.001");
    verify_table(temp_dir.path(), "partsupp", num_parts, "0.001");
    verify_table(temp_dir.path(), "customer", num_parts, "0.001");
    verify_table(temp_dir.path(), "supplier", num_parts, "0.001");
    // Note, nation and region have only a single part regardless of --parts
    verify_table(temp_dir.path(), "nation", 1, "0.001");
    verify_table(temp_dir.path(), "region", 1, "0.001");
}

/// Read the N files from `output_dir/table_name/table_name.part.tml` into a
/// single buffer and compare them to the contents of the reference file
fn verify_table(output_dir: &Path, table_name: &str, parts: usize, scale_factor: &str) {
    let mut output_contents = Vec::new();
    for part in 1..=parts {
        let generated_file = output_dir
            .join(table_name)
            .join(format!("{table_name}.{part}.tbl"));
        assert!(
            generated_file.exists(),
            "File {:?} does not exist",
            generated_file
        );
        let generated_contents =
            fs::read_to_string(generated_file).expect("Failed to read generated file");
        output_contents.append(&mut generated_contents.into_bytes());
    }
    let output_contents =
        String::from_utf8(output_contents).expect("Failed to convert output contents to string");

    // load the reference file
    let reference_file = read_reference_file(table_name, scale_factor);
    assert_eq!(output_contents, reference_file);
}

#[test]
fn test_tpchgen_cli_part_no_parts() {
    let temp_dir = tempdir().expect("Failed to create temporary directory");

    // CLI Error test --part and but not --parts
    Command::cargo_bin("tpchgen-cli")
        .expect("Binary not found")
        .arg("--output-dir")
        .arg(temp_dir.path())
        .arg("--part")
        .arg("42")
        .assert()
        .failure()
        .stderr(predicates::str::contains(
            "The --part option requires the --parts option to be set",
        ));
}

#[test]
fn test_tpchgen_cli_too_many_parts() {
    let temp_dir = tempdir().expect("Failed to create temporary directory");

    // This should fail because --part is 42 which is more than the --parts 10
    Command::cargo_bin("tpchgen-cli")
        .expect("Binary not found")
        .arg("--output-dir")
        .arg(temp_dir.path())
        .arg("--part")
        .arg("42")
        .arg("--parts")
        .arg("10")
        .assert()
        .failure()
        .stderr(predicates::str::contains(
            "Invalid --part. Expected at most the value of --parts (10), got 42",
        ));
}

#[test]
fn test_tpchgen_cli_zero_part() {
    let temp_dir = tempdir().expect("Failed to create temporary directory");

    Command::cargo_bin("tpchgen-cli")
        .expect("Binary not found")
        .arg("--output-dir")
        .arg(temp_dir.path())
        .arg("--part")
        .arg("0")
        .arg("--parts")
        .arg("10")
        .assert()
        .failure()
        .stderr(predicates::str::contains(
            "Invalid --part. Expected a number greater than zero, got 0",
        ));
}
#[test]
fn test_tpchgen_cli_zero_part_zero_parts() {
    let temp_dir = tempdir().expect("Failed to create temporary directory");

    Command::cargo_bin("tpchgen-cli")
        .expect("Binary not found")
        .arg("--output-dir")
        .arg(temp_dir.path())
        .arg("--part")
        .arg("0")
        .arg("--parts")
        .arg("0")
        .assert()
        .failure()
        .stderr(predicates::str::contains(
            "Invalid --part. Expected a number greater than zero, got 0",
        ));
}

/// Test specifying CSV options even when writing TBL output
#[tokio::test]
async fn test_incompatible_options_warnings() {
    let output_dir = tempdir().unwrap();
    Command::cargo_bin("tpchgen-cli")
        .expect("Binary not found")
        .arg("--format")
        .arg("tbl")
        .arg("--tables")
        .arg("orders")
        .arg("--scale-factor")
        .arg("0.0001")
        .arg("--output-dir")
        .arg(output_dir.path())
        // pass in CSV option that is incompatible with tbl
        .arg("--delimiter")
        .arg("\\t")
        .assert()
        // still success, but should see warnings in stderr
        .success()
        .stderr(predicates::str::contains(
            "CSV delimiter option set but not generating CSV files",
        ));
}

/// Test that --quiet flag suppresses warning messages
#[tokio::test]
async fn test_quiet_flag_suppresses_warnings() {
    let output_dir = tempdir().unwrap();
    let output = Command::cargo_bin("tpchgen-cli")
        .expect("Binary not found")
        .env("RUST_LOG", "warn")
        .arg("--format")
        .arg("tbl")
        .arg("--tables")
        .arg("orders")
        .arg("--scale-factor")
        .arg("0.0001")
        .arg("--output-dir")
        .arg(output_dir.path())
        .arg("--delimiter")
        .arg("\\t")
        .arg("--quiet")
        .assert()
        .success();

    let stderr = String::from_utf8_lossy(&output.get_output().stderr);
    assert!(
        !stderr.contains("CSV delimiter option set but not generating CSV files"),
        "Expected no warning messages in stderr with --quiet flag, but found: {}",
        stderr
    );
}

fn read_gzipped_file_to_string<P: AsRef<Path>>(path: P) -> Result<String, std::io::Error> {
    let file = File::open(path)?;
    let mut decoder = flate2::read::GzDecoder::new(file);
    let mut contents = Vec::new();
    decoder.read_to_end(&mut contents)?;
    let contents = String::from_utf8(contents)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    Ok(contents)
}

/// Reads the reference file for the specified table and scale factor.
///
/// example usage: `read_reference_file("orders", "0.001")`
fn read_reference_file(table_name: &str, scale_factor: &str) -> String {
    let reference_file = format!("../tpchgen/data/sf-{scale_factor}/{table_name}.tbl.gz");
    match read_gzipped_file_to_string(&reference_file) {
        Ok(contents) => contents,
        Err(e) => {
            panic!("Failed to read reference file {reference_file}: {e}");
        }
    }
}
