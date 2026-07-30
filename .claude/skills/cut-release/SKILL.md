---
name: cut-release
description: Cut a release for the Aerospike Go client (aerospike-client-go). Use when the user runs "/cut-release <version>" (e.g. "/cut-release v10.2.1"). Syncs stage, creates a release branch named for the version, updates CHANGELOG.md with the changes since the last release, and drafts GitHub release notes in the project's release format.
---

# cut-release

Cuts a release for **aerospike-client-go**. Invoked as `/cut-release <version>`, e.g.
`/cut-release v10.2.1`. The version is ALWAYS supplied by the user — never guess it.

Repo: `/Users/mkaracic/workfolder/source/aerospike-client-go`
GitHub: `https://github.com/aerospike/aerospike-client-go`
Default branch: `v8`. Release work branches off `stage`.

## Inputs

- `version` (required) — the release version, taken from the skill arguments (everything
  after `/cut-release`). Must match `v<major>.<minor>.<patch>` (e.g. `v10.2.1`). If it is
  missing or malformed, stop and ask the user for it. Do not proceed without it.

## Steps

Run these in order. Stop and report if any step fails.

### 1. Verify a clean working tree

```bash
cd /Users/mkaracic/workfolder/source/aerospike-client-go
git status --porcelain
```

If there are uncommitted changes, STOP and tell the user — do not stash, reset, or discard
their work. They must clean up first.

### 2. Sync `stage`

```bash
git fetch --all --tags --prune
git checkout stage
git pull --ff-only origin stage
```

If `--ff-only` fails (local `stage` diverged), STOP and report — do not force or reset
without asking.

### 3. Determine the previous released version

```bash
PREV=$(git tag --list 'v*' --sort=-v:refname | head -1)
```

`$PREV` is the base for both the commit range and the "Full Changelog" compare link
(e.g. `v8.7.0`). Sanity-check it looks like a real prior version relative to the one being cut.

### 4. Create the release branch off `stage`

Branch name convention (matches existing repo history): `update-changelog-<version>`.

```bash
git checkout -b update-changelog-<version> stage
```

### 5. Collect and categorize the changes

```bash
git log --no-merges --format='%h %s' <PREV>..HEAD
```

Then curate:

- **Exclude** dependency/CI/security-bot noise: any `ci(deps):` / `chore(deps):` bumps,
  `[StepSecurity]` commits, and non-ticket test/CI infra commits (e.g. "fix that causes
  dependabot PRs to fail…"). These never appear in CHANGELOG.md — confirm by scanning the
  existing file.
- **Keep** substantive commits — primarily those with a `[CLIENT-XXXX]` ticket. Note and
  fix obvious ticket-ID typos in subjects (e.g. `CLEINT-4753` → `CLIENT-4753`).
- **Categorize** each kept commit into one of three buckets, following the conventions in
  the existing CHANGELOG.md:
  - **New Features** — new APIs/capabilities (e.g. "Added …", "Implemented …" of a new feature).
  - **Fixes** — bug fixes, regressions, incorrect behavior (subjects with "Fix", "Fixed",
    "prevent", "regression", wrong-behavior descriptions).
  - **Improvements** — enhancements, guardrails, docs, server-alignment, CI, perf gates that
    aren't a new feature or a bug fix.
  Categorization is a judgment call — read the subject and, if unclear, the ticket/diff.
  **Omit any category that ends up empty.**

Rewrite each subject into a clean past-tense description (drop the PR `(#123)` suffix and the
short hash). Keep it faithful to what the commit did.

### 6. Update CHANGELOG.md

CHANGELOG.md format (ticket comes FIRST, date has NO comma). Insert a new section directly
below the `# Change History` heading, above the most recent entry. Match the style of the
latest existing entry (plain `- New Features` headers, two-space nested `  - [CLIENT-…]` items):

```
## <Month DD YYYY>: <version>

- New Features
  - [CLIENT-XXXX] <Description.>
- Fixes
  - [CLIENT-XXXX] <Description.>
- Improvements
  - [CLIENT-XXXX] <Description.>
```

- `<Month DD YYYY>` = today's date, full month name, **no comma** (e.g. `July 29 2026`).
- Omit empty categories. Order: New Features, Fixes, Improvements.
- Descriptions end with a period, ticket ID in `[…]` at the START of the line.

### 7. Write the draft release notes file

GitHub release format (ticket comes LAST, date HAS a comma, plus the compare link). Write to
`<repo>/RELEASE_NOTES_<version>.md`:

```
Release Date: <Month DD, YYYY>

## New Features
- <Description.> [CLIENT-XXXX]

## Fixes
- <Description.> [CLIENT-XXXX]

## Improvements
- <Description.> [CLIENT-XXXX]


**Full Changelog**: https://github.com/aerospike/aerospike-client-go/compare/<PREV>...<version>
```

- `<Month DD, YYYY>` = today's date, full month name, **with a comma** (e.g. `July 29, 2026`).
- Same categorized content as the CHANGELOG, but ticket ID in `[…]` at the END of each line.
- Omit empty categories.
- Compare link uses `<PREV>...<version>` (the `<version>` tag will resolve once the release
  is published).

### 8. Commit the CHANGELOG to the release branch (local only)

Committing the changelog to the release branch is part of cutting the release, so do it:

```bash
git add CHANGELOG.md
git commit -m "Update CHANGELOG for <version>"
```

Do NOT `git add` the `RELEASE_NOTES_<version>.md` file — it is scratch for the GitHub draft,
not a repo artifact. **NEVER push the branch** — pushing requires an explicit, separate ask.

### 9. Create the draft GitHub release (confirm first)

Creating a GitHub release is outward-facing. Show the user the exact command and the notes,
and get explicit confirmation before running it:

```bash
gh release create <version> --draft --target stage \
  --title "<version>" \
  --notes-file RELEASE_NOTES_<version>.md
```

- `--draft` keeps it unpublished. `--target stage` anchors it to a commitish that exists on
  the remote (the release branch is local-only). The user can retarget/tag when publishing.
- After it succeeds, print the draft release URL that `gh` returns.

## Format cheat-sheet (the two files differ — do not mix them up)

| | CHANGELOG.md | Draft release notes |
|---|---|---|
| Ticket position | START: `[CLIENT-XXXX] Desc.` | END: `Desc. [CLIENT-XXXX]` |
| Date | `Month DD YYYY` (no comma) | `Release Date: Month DD, YYYY` (comma) |
| Section headers | `- New Features` | `## New Features` |
| Version header | `## <date>: <version>` | — |
| Compare link | no | yes, `compare/<PREV>...<version>` |

## Guardrails

- Version is user-supplied; never invent one. Ask if missing/malformed.
- Never push, force-push, publish, or create a remote tag. Local commit + `--draft` only.
- Never discard uncommitted work — stop and ask if the tree is dirty.
- Do not touch `stage` history; all changes go on the `update-changelog-<version>` branch.
