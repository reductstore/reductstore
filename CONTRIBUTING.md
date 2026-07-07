# Contributing to ReductStore

Thanks for contributing.

We want ReductStore to be easy to contribute to, especially for people joining the project for the first time.
This guide keeps the workflow clear so you can spend more time building and less time guessing what reviewers expect.

## Start with an Issue

Open a pull request only for an existing issue.

- If you want to work on an issue, say so in the comments first and wait to be assigned before you start coding.
- If you want to fix a reported bug, comment on the issue first so maintainers know you are working on it.
- If you have a new idea, do not open a surprise PR. Start with a GitHub issue or a thread on the [ReductStore community forum](https://community.reduct.store) to discuss scope first.
- If you want a small first task, start with [good first issue](https://github.com/reductstore/reductstore/issues?q=is%3Aissue%20is%3Aopen%20label%3A%22good%20first%20issue%22) or [help wanted](https://github.com/reductstore/reductstore/issues?q=is%3Aissue%20is%3Aopen%20label%3A%22help%20wanted%22).

## Prepare Your Branch

Before you start coding:

1. Sync your fork with the latest `upstream/main` (or update your local `main` from `origin/main` if you work directly in the repository).
2. Create a fresh branch from the latest `main`.
3. Keep the branch focused on one issue.

This repository expects every PR to be based on the newest default branch commit.
If possible, start the branch name with the issue ID, for example `1469-docs-contributing-guide`. It helps keep PRs easier to organize and review.

## Run ReductStore Locally

If your change affects runtime behavior, start a real ReductStore instance and test it yourself when possible.

Please validate the changed path against a live server, not only with unit tests.
Contributors should run the code from their branch, not only the published Docker image, so the instance actually includes their changes.

For most changes, build and run ReductStore locally:

```bash
mkdir -p ./data
RS_DATA_PATH=./data cargo run -p reductstore --features "fs-backend web-console"
```

If you prefer building a binary first:

```bash
cargo build -p reductstore --features "fs-backend web-console"
mkdir -p ./data
RS_DATA_PATH=./data ./target/debug/reductstore
```

Use the published Docker image only for dependency-free smoke checks or to compare released behavior:

```bash
mkdir -p ./data
sudo chown -R 10001:10001 ./data
docker run -p 8383:8383 -v ${PWD}/data:/data reduct/store:latest
```

Then exercise the API, replication flow, query behavior, lifecycle policy, or UI path that your change touches.

## Build and Test

Run the smallest relevant set locally before opening a PR:

```bash
cargo fmt --all
cargo check --workspace
cargo test --workspace
```

Useful follow-ups when they apply:

- `cargo test --workspace --features "s3-backend,fs-backend,ci"`
- `pip install -r integration_tests/api/requirements.txt`
- `STORAGE_URL=http://127.0.0.1:8383 pytest integration_tests/api`

If you skip a test, explain why in the PR.

## AI-Assisted Changes

Generated code is allowed.

But the contributor submitting the PR is responsible for it:

- Read and understand every generated change before you submit it.
- Remove dead code, vague comments, and accidental scope creep.
- Re-run tests and manual checks yourself.
- Be ready to explain the design and tradeoffs in review.

Human judgment is required. "The AI wrote it" is not enough.

## Open the Pull Request

When you open the PR:

- Link the issue it resolves.
- Use the repository pull request template.
- Explain the user or maintainer problem you fixed.
- List the validation you ran, including manual runtime checks when applicable.
- Mention config or environment changes such as new `RS_*` variables or feature flags.
- Update `CHANGELOG.md` when the change affects users, operators, or client-visible behavior.
- Keep screenshots, logs, or request examples when they help reviewers verify behavior quickly.

## Update the Changelog

If your PR changes behavior that users may notice, add an entry to `CHANGELOG.md`.

- Add the entry in the relevant unreleased section.
- Keep it short and factual.
- Focus on what changed for users or operators, not on internal refactoring details.
- Include the pull request number if the surrounding entries already follow that style.
- Skip changelog noise for purely internal changes with no user-facing effect.

## Review Expectations

Recent contributor PRs in this repository show a consistent pattern:

- Well-scoped PRs tied to an issue move faster.
- Changes that come with local validation are easier to review.
- Ambiguous feature additions usually need issue discussion before code review starts.
- Maintainers may ask for behavior changes, test updates, or smaller scope before merge.

Treat review as collaboration. The goal is to help each other land solid changes with as little friction as possible.

## Need Help?

- Ask on the [community forum](https://community.reduct.store)
- Comment on the issue you want to work on
- Open a draft PR early if you want feedback on direction
