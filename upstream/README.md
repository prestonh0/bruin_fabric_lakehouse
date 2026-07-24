# Upstream integration patch for bruin-data/bruin

This directory contains the **complete, verified integration** of the Fabric
Lakehouse Spark connector into the bruin codebase, as a single git patch
ready to apply to a fork and open as a PR against
[bruin-data/bruin](https://github.com/bruin-data/bruin).

- **Patch**: `0001-feat-add-Microsoft-Fabric-Lakehouse-Spark-platform.patch`
- **Base commit**: `a65d9d8a0a424a5659ef0f4f57b12eb9b574943c`
  (bruin `main`, 2026-07-24 — "Merge pull request #2444")
- **Scope**: 33 files, +4,908 / −38

## What the patch contains

- `pkg/fabricspark/` — the full connector (17 files, code + tests), imports
  rewritten to `github.com/bruin-data/bruin/pkg/fabricspark`, with the asset
  type constants and the `anti_join` strategy moved into `pkg/pipeline`
  where bruin keeps them.
- Registration in every touchpoint, mirroring the `databricks` platform:
  - `pkg/pipeline/pipeline.go` — 5 asset types, connection mapping,
    default connection name, `IsSQLAssetType`, majority-type map, and the
    `anti_join` strategy constant + `AllAvailableMaterializationStrategies`
  - `pkg/config` — `FabricSparkConnection` + `fabric_spark` in the
    add/delete/merge connection paths
  - `pkg/connection` — manager map, `AddFabricSparkConnectionFromConfig`,
    parallel connection processing
  - `pkg/executor/defaults.go` — default operators for all 5 asset types
  - `cmd/run.go` — operator wiring (SQL, PySpark, seed, both sensors)
  - `cmd/render.go` / `cmd/render_ddl.go` — materialization renderer
  - `pkg/lint/rules.go` — table-sensor allowlist, platform name, and a
    dedicated `anti_join` validation case
  - `pkg/jinja` — `PlatformFabricSpark` with Spark-dialect helper overrides
  - `pkg/sqlparser/parser.go` — sqlglot dialect mapping (`spark`)
  - `docs/platforms/fabric-spark.md` + docs sidebar entry

## Verification performed (on the patched tree)

- `go build ./...` — the full bruin CLI builds (including the Rust FFI
  sqlparser via `cargo build --release`)
- `go test ./pkg/fabricspark ./pkg/pipeline ./pkg/config ./pkg/lint
  ./pkg/jinja ./pkg/executor ./pkg/connection ./cmd` — all pass
- End-to-end with the built binary: `bruin validate` (no issues) and
  `bruin render` (correct anti-join SQL) against a `fabric.spark.sql`
  pipeline using a `fabric_spark` connection
- `gofmt` clean on all touched files

## How to apply

```bash
# 1. Fork bruin-data/bruin on GitHub, then:
git clone git@github.com:<your-user>/bruin.git
cd bruin
git checkout -b fabric-spark-lakehouse-connector a65d9d8a0a424a5659ef0f4f57b12eb9b574943c
git am path/to/0001-feat-add-Microsoft-Fabric-Lakehouse-Spark-platform.patch
git push -u origin fabric-spark-lakehouse-connector
# 2. Open the PR from that branch against bruin-data/bruin main.
```

If bruin's `main` has moved and `git am` conflicts, rebase the branch onto
the new `main` — the touched registration points are append-only lists, so
conflicts should be trivial.
