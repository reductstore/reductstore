# Repository Guidelines

## Project Structure & Modules

- Crates: `reductstore/` (server/CLI; proto in `reductstore/src/proto`), `reduct_base/` (models, logging, JSON
  marshalling), `reduct_macros/` (proc macros).
- Tests: inline `#[cfg(test)]` plus Python in `integration_tests/` (`api/`, `data_check/`, `benchmarks/`).
- Ops assets: `snap/`, `buildx.Dockerfile`, `.github/actions/`; sample certs/data in `misc/`.
- Key modules: `reductstore/src/{api,asset,auth,backend,cfg,core,ext,proto,replication,storage}`;
  `reduct_base/src/{logging,msg,io,ext}`.

## Build, Test, and Development

- Format/check: `cargo fmt --all` then `cargo check --workspace`.
- Run locally: `RS_DATA_PATH=./data cargo run -p reductstore --features "fs-backend web-console"`.
- Release: `cargo build -p reductstore --release --features "fs-backend web-console"`.
- Unit tests: `cargo test --workspace` (add `--features "s3-backend,fs-backend,ci"` as needed).
- Integration/API: `pip install -r integration_tests/api/requirements.txt` then
  `STORAGE_URL=http://127.0.0.1:8383 pytest integration_tests/api`.
- Coverage: `cargo llvm-cov --features "s3-backend,fs-backend,ci" --workspace --lcov --output-path lcov.info`.

## Coding Style & Naming

- Rust 2021, 4-space indent; modules/files snake_case, types/traits title case, functions snake_case.
- `rustfmt` (imports reordered via `.rustfmt.toml`) is required before commits.
- Prefer `?` over `unwrap` in non-test code; keep errors contextual.
- Python in `integration_tests` should pass `black .` and avoid leaking secrets in logs.

## Testing Guidelines

- Add unit tests near code; use `rstest`, `serial_test`, and `test-log`.
- Name tests after behavior (`writes_entry_when_token_valid`).
- Integration runs: set `STORAGE_URL` to a live instance; use `misc/certificate.crt` only for local TLS checks.
- When adding endpoints/storage behaviors, update Rust unit tests and pytest suites; avoid coverage regressions.
- Use GitHub CLI for runnig and checking tests: `gh run list`, `gh run view <run-id> --log`.
- Run coverage locally with ` cargo llvm-cov --workspace --lcov --output-path lcov.info`.

## Commit & Pull Request Guidelines

- Commit messages are short, imperative, lower-case; optionally tag issues/PRs (e.g., `fix artifact name`,
  `bump regex from 1.12.1 to 1.12.2 (#1037)`).
- Keep changes scoped; include tests or a rationale when skipping them.
- PRs should state motivation, config/env changes (e.g., new `RS_*` vars, license paths), and validation steps; attach
  console screenshots or API traces when UI/API is touched.
- Ensure `cargo fmt`, `cargo check`, and relevant tests pass locally before opening a PR.
- Use GitHub CLI to create a PR with the following tempate https://github.com/reductstore/.github/blob/main/.github/pull_request_template.md.
- Do not commit or push changes without explicit user approval.

## Security & Configuration

- Default data root is `RS_DATA_PATH` (`/data` in containers); avoid committing real keys or licenses—use sample PEMs in
  `misc/`.
- S3 backend needs `s3-backend` plus AWS credentials/env vars; prefer env vars over checked-in config.
