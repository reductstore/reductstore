---
name: create-pr
description: Create GitHub pull requests with the gh CLI using the reductstore/.github PR template, then update CHANGELOG.md with the created PR ID and commit the changelog (do not push). Use when a user asks to open a PR and record its ID in the changelog.
---

# Create Pr

## Overview

Create a PR using the standardized template from reductstore/.github, then record the PR number in CHANGELOG.md and commit the changelog without pushing.

## Workflow

### 1) Preconditions
- Ensure `gh auth status` is logged in and the current branch is the intended PR branch.
- Ensure the working tree is clean (or only the intended changes are present).
- Use context from the current conversation to infer intent, scope, and any constraints.

### 2) Understand changes and rationale
- Compare the current branch against `main` to understand what changed and why (use this to derive the PR title, description, and rationale):
  - `git fetch origin main`
  - `git log --oneline origin/main..HEAD`
  - `git diff --stat origin/main...HEAD`
  - `git diff origin/main...HEAD`
- If the branch name starts with an issue number (e.g., `123-...`), use that issue in the rationale and fill the `Closes #` line in the PR template.
- If the branch name does not include an issue number, infer the likely issue from changes and conversation context, or leave `Closes #` empty if unsure.

### 3) Fetch the PR template
Use the helper script to download the latest PR template from reductstore/.github:

```bash
./.codex/skills/create-pr/scripts/fetch_pr_template.sh /tmp/pr_template.md
```

Fill in the template file with the relevant summary, testing, and rationale derived from the diff and conversation context.

### 4) Create the PR with gh
Use the filled template as the PR body:

```bash
gh pr create --title "<title>" --body-file /tmp/pr_template.md
```

Capture the PR number after creation:

```bash
gh pr view --json number -q .number
```

### 5) Update CHANGELOG.md
- Find the appropriate section (usually the most recent/unreleased section).
- Add a new entry following the existing style in the file.
- Include the PR ID as `#<number>` exactly as prior entries do.

### 6) Commit (no push)
Stage and commit the changelog update only:

```bash
git add CHANGELOG.md
git commit -m "Update changelog for PR #<number>"
```

Do not push.

## Resources

### scripts/
- `fetch_pr_template.sh`: Download the latest PR template from reductstore/.github.
