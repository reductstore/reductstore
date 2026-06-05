// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

//! Read-only storage doctor for offline ReductStore data-path inspection.
//!
//! Scans a local filesystem data path and reports storage consistency
//! problems without mutating any files. Strictly read-only: no server
//! startup, no log forwarding, no remote backend access.

use reductstore::storage::doctor::{
    scan_data_path, DoctorIssue, DoctorReport, DoctorSeverity, DoctorSummary,
};
use std::path::PathBuf;
use std::process::ExitCode;

const DEFAULT_DATA_PATH: &str = "/data";
const DEFAULT_FAIL_ON: &str = "error";

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().collect();

    if args.iter().any(|arg| arg == "--version" || arg == "-V") {
        println!("reduct-doctor {}", env!("CARGO_PKG_VERSION"));
        return ExitCode::SUCCESS;
    }

    if args.iter().any(|arg| arg == "--help" || arg == "-h") {
        print_help();
        return ExitCode::SUCCESS;
    }

    let opts = match parse_args(&args[1..]) {
        Ok(opts) => opts,
        Err(err) => {
            eprintln!("reduct-doctor: {err}");
            eprintln!("Try 'reduct-doctor --help' for usage.");
            return ExitCode::from(2);
        }
    };

    let data_path = match resolve_data_path(&opts) {
        Ok(path) => path,
        Err(err) => {
            eprintln!("reduct-doctor: {err}");
            return ExitCode::from(2);
        }
    };

    let report = match scan_data_path(&data_path) {
        Ok(report) => report,
        Err(err) => {
            eprintln!(
                "reduct-doctor: failed to scan data path '{}': {err}",
                data_path.display()
            );
            return ExitCode::from(2);
        }
    };

    match opts.format {
        OutputFormat::Human => print_human(&report),
        OutputFormat::Json => match serde_json::to_string_pretty(&report) {
            Ok(text) => println!("{text}"),
            Err(err) => {
                eprintln!("reduct-doctor: failed to render JSON: {err}");
                return ExitCode::from(2);
            }
        },
    }

    if should_fail(&report, opts.fail_on) {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OutputFormat {
    Human,
    Json,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FailOn {
    Error,
    Warning,
    Info,
}

struct Options {
    data_path: Option<PathBuf>,
    format: OutputFormat,
    fail_on: FailOn,
}

fn parse_args(args: &[String]) -> Result<Options, String> {
    let mut data_path: Option<PathBuf> = None;
    let mut format = OutputFormat::Human;
    let mut fail_on = parse_fail_on(DEFAULT_FAIL_ON)
        .map_err(|_| format!("invalid default fail-on '{}'", DEFAULT_FAIL_ON))?;

    let mut i = 0;
    while i < args.len() {
        let arg = &args[i];
        match arg.as_str() {
            "--data-path" => {
                let value = args
                    .get(i + 1)
                    .ok_or_else(|| "--data-path requires a value".to_string())?;
                data_path = Some(PathBuf::from(value));
                i += 2;
            }
            "--format" => {
                let value = args
                    .get(i + 1)
                    .ok_or_else(|| "--format requires a value".to_string())?;
                format = parse_format(value)?;
                i += 2;
            }
            "--fail-on" => {
                let value = args
                    .get(i + 1)
                    .ok_or_else(|| "--fail-on requires a value".to_string())?;
                fail_on = parse_fail_on(value)?;
                i += 2;
            }
            other if other.starts_with("--data-path=") => {
                data_path = Some(PathBuf::from(&other["--data-path=".len()..]));
                i += 1;
            }
            other if other.starts_with("--format=") => {
                format = parse_format(&other["--format=".len()..])?;
                i += 1;
            }
            other if other.starts_with("--fail-on=") => {
                fail_on = parse_fail_on(&other["--fail-on=".len()..])?;
                i += 1;
            }
            other => return Err(format!("unknown argument: {other}")),
        }
    }

    Ok(Options {
        data_path,
        format,
        fail_on,
    })
}

fn parse_format(value: &str) -> Result<OutputFormat, String> {
    match value {
        "human" => Ok(OutputFormat::Human),
        "json" => Ok(OutputFormat::Json),
        other => Err(format!(
            "invalid --format '{}' (expected 'human' or 'json')",
            other
        )),
    }
}

fn parse_fail_on(value: &str) -> Result<FailOn, String> {
    match value {
        "error" => Ok(FailOn::Error),
        "warning" => Ok(FailOn::Warning),
        "info" => Ok(FailOn::Info),
        other => Err(format!(
            "invalid --fail-on '{}' (expected 'error', 'warning', or 'info')",
            other
        )),
    }
}

fn resolve_data_path(opts: &Options) -> Result<PathBuf, String> {
    if let Some(path) = &opts.data_path {
        return Ok(path.clone());
    }
    if let Ok(value) = std::env::var("RS_DATA_PATH") {
        if !value.is_empty() {
            return Ok(PathBuf::from(value));
        }
    }
    Ok(PathBuf::from(DEFAULT_DATA_PATH))
}

fn should_fail(report: &DoctorReport, fail_on: FailOn) -> bool {
    let count = match fail_on {
        FailOn::Error => report.summary.error_count,
        FailOn::Warning => report.summary.error_count + report.summary.warning_count,
        FailOn::Info => {
            report.summary.error_count + report.summary.warning_count + report.summary.info_count
        }
    };
    count > 0
}

fn print_help() {
    println!(
        "reduct-doctor {version}

Read-only storage doctor for offline ReductStore data-path inspection.

USAGE:
    reduct-doctor [OPTIONS]

OPTIONS:
    --data-path <PATH>    Path to the ReductStore data directory.
                          Falls back to $RS_DATA_PATH, then /data.
    --format <FORMAT>     Output format: 'human' (default) or 'json'.
    --fail-on <LEVEL>     Exit non-zero when issues at or above LEVEL are
                          present: 'error' (default), 'warning', or 'info'.
    -V, --version         Print version and exit.
    -h, --help            Print this help and exit.

The doctor is strictly read-only. It does not start the server, modify
files, or contact the network. Use --data-path to point at an offline copy
of a data directory.

EXIT CODES:
    0   No issues at or above --fail-on.
    1   Issues at or above --fail-on were found.
    2   Usage error or unrecoverable I/O error.",
        version = env!("CARGO_PKG_VERSION"),
    );
}

fn print_human(report: &DoctorReport) {
    println!("ReductStore Doctor Report");
    println!("==========================");
    println!("Data path: {}", report.data_path.display());
    print_summary(&report.summary);
    println!();

    if report.issues.is_empty() {
        println!("No issues found.");
        return;
    }

    println!("Issues ({}):", report.issues.len());
    for issue in &report.issues {
        print_issue(issue);
    }
}

fn print_summary(summary: &DoctorSummary) {
    println!(
        "Buckets: {}  Entries: {}  Blocks: {}",
        summary.bucket_count, summary.entry_count, summary.block_count
    );
    println!(
        "Issues: {} error, {} warning, {} info",
        summary.error_count, summary.warning_count, summary.info_count
    );
}

fn print_issue(issue: &DoctorIssue) {
    let severity = match issue.severity {
        DoctorSeverity::Error => "ERROR  ",
        DoctorSeverity::Warning => "WARNING",
        DoctorSeverity::Info => "INFO   ",
    };
    let location = match (&issue.bucket, &issue.entry, issue.block_id) {
        (Some(b), Some(e), Some(block)) => format!("{b}/{e}#{block}"),
        (Some(b), Some(e), None) => format!("{b}/{e}"),
        (Some(b), None, _) => format!("{b}/"),
        (None, Some(e), _) => format!("./{e}"),
        (None, None, Some(block)) => format!("#{block}"),
        (None, None, None) => String::new(),
    };

    let kind = serde_json::to_value(&issue.kind)
        .ok()
        .and_then(|v| v.as_str().map(ToString::to_string))
        .unwrap_or_else(|| "<unknown>".to_string());

    println!(
        "  {severity} {kind:<32} {location:<40} {}",
        issue.path.display()
    );
    println!("           {}", issue.message);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_format_accepts_human_and_json() {
        assert_eq!(parse_format("human").unwrap(), OutputFormat::Human);
        assert_eq!(parse_format("json").unwrap(), OutputFormat::Json);
    }

    #[test]
    fn parse_format_rejects_unknown() {
        assert!(parse_format("yaml").is_err());
    }

    #[test]
    fn parse_fail_on_accepts_known_levels() {
        assert_eq!(parse_fail_on("error").unwrap(), FailOn::Error);
        assert_eq!(parse_fail_on("warning").unwrap(), FailOn::Warning);
        assert_eq!(parse_fail_on("info").unwrap(), FailOn::Info);
    }

    #[test]
    fn parse_fail_on_rejects_unknown() {
        assert!(parse_fail_on("debug").is_err());
    }

    #[test]
    fn parse_args_uses_defaults_when_empty() {
        let opts = parse_args(&[]).unwrap();
        assert!(opts.data_path.is_none());
        assert_eq!(opts.format, OutputFormat::Human);
        assert_eq!(opts.fail_on, FailOn::Error);
    }

    #[test]
    fn parse_args_supports_equals_form() {
        let opts = parse_args(&[
            "--data-path=/tmp/foo".to_string(),
            "--format=json".to_string(),
            "--fail-on=warning".to_string(),
        ])
        .unwrap();
        assert_eq!(opts.data_path, Some(PathBuf::from("/tmp/foo")));
        assert_eq!(opts.format, OutputFormat::Json);
        assert_eq!(opts.fail_on, FailOn::Warning);
    }

    #[test]
    fn parse_args_supports_space_form() {
        let opts = parse_args(&[
            "--data-path".to_string(),
            "/tmp/foo".to_string(),
            "--format".to_string(),
            "json".to_string(),
            "--fail-on".to_string(),
            "info".to_string(),
        ])
        .unwrap();
        assert_eq!(opts.data_path, Some(PathBuf::from("/tmp/foo")));
        assert_eq!(opts.format, OutputFormat::Json);
        assert_eq!(opts.fail_on, FailOn::Info);
    }

    #[test]
    fn parse_args_rejects_unknown_flag() {
        assert!(parse_args(&["--unknown".to_string()]).is_err());
    }

    #[test]
    fn parse_args_rejects_missing_value() {
        assert!(parse_args(&["--data-path".to_string()]).is_err());
        assert!(parse_args(&["--format".to_string()]).is_err());
        assert!(parse_args(&["--fail-on".to_string()]).is_err());
    }

    #[test]
    #[allow(unused_variables)]
    fn should_fail_threshold_levels() {
        use reductstore::storage::doctor::{DoctorIssue, DoctorSeverity};

        fn make_report(error: usize, warning: usize, info: usize) -> DoctorReport {
            let mut report = DoctorReport {
                data_path: PathBuf::from("/tmp"),
                summary: DoctorSummary::default(),
                buckets: Vec::new(),
                issues: Vec::new(),
            };
            for i in 0..error {
                report.issues.push(DoctorIssue {
                    severity: DoctorSeverity::Error,
                    kind: reductstore::storage::doctor::DoctorIssueKind::CorruptBlockIndex,
                    bucket: None,
                    entry: None,
                    block_id: Some(i as u64),
                    path: PathBuf::from("/tmp/x"),
                    message: "e".to_string(),
                });
            }
            for i in 0..warning {
                report.issues.push(DoctorIssue {
                    severity: DoctorSeverity::Warning,
                    kind: reductstore::storage::doctor::DoctorIssueKind::DescriptorNotIndexed,
                    bucket: None,
                    entry: None,
                    block_id: Some(i as u64),
                    path: PathBuf::from("/tmp/y"),
                    message: "w".to_string(),
                });
            }
            for _i in 0..info {
                report.issues.push(DoctorIssue {
                    severity: DoctorSeverity::Info,
                    kind: reductstore::storage::doctor::DoctorIssueKind::UnknownEntryInternalFile,
                    bucket: None,
                    entry: None,
                    block_id: None,
                    path: PathBuf::from("/tmp/z"),
                    message: "i".to_string(),
                });
            }
            report.summary = DoctorSummary {
                error_count: error,
                warning_count: warning,
                info_count: info,
                ..DoctorSummary::default()
            };
            report
        }

        let r = make_report(1, 0, 0);
        assert!(should_fail(&r, FailOn::Error));
        assert!(should_fail(&r, FailOn::Warning));
        assert!(should_fail(&r, FailOn::Info));

        let r = make_report(0, 1, 0);
        assert!(!should_fail(&r, FailOn::Error));
        assert!(should_fail(&r, FailOn::Warning));
        assert!(should_fail(&r, FailOn::Info));

        let r = make_report(0, 0, 1);
        assert!(!should_fail(&r, FailOn::Error));
        assert!(!should_fail(&r, FailOn::Warning));
        assert!(should_fail(&r, FailOn::Info));

        let r = make_report(0, 0, 0);
        assert!(!should_fail(&r, FailOn::Error));
        assert!(!should_fail(&r, FailOn::Warning));
        assert!(!should_fail(&r, FailOn::Info));
    }
}
