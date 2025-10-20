# Universal ERD & Data Model Generator (Databricks / Unity Catalog)

**Version:** 2.1.0  
**Author:** Joshua Williamson

This repository ships a single Databricks-ready notebook (`Notebook`) that reverse-engineers the schema of a catalog or legacy database and materialises a complete entity relationship model. The implementation favours Unity Catalog, performs bounded sampling for statistics, infers missing keys/relationships, and emits clean artefacts (JSON, Markdown + Mermaid ERD, GraphML, CSVs, optional Delta tables) suitable for documentation and governance.

---

## Contents

- [Overview](#overview)
- [Key Capabilities](#key-capabilities)
- [Platform Requirements](#platform-requirements)
- [Quick Start](#quick-start)
  - [Unity Catalog Workflow](#unity-catalog-workflow)
  - [Legacy Database Workflow](#legacy-database-workflow)
- [Configuration Reference](#configuration-reference)
- [Execution Flow](#execution-flow)
- [Output Artefacts](#output-artefacts)
- [Operational Modes](#operational-modes)
- [Running in Databricks Jobs](#running-in-databricks-jobs)
- [Operational Guardrails & Security](#operational-guardrails--security)
- [Troubleshooting](#troubleshooting)
- [Change Log](#change-log)

---

## Overview

The notebook orchestrates six disciplined phases:

1. Parameter bootstrap from Databricks widgets (with safe defaults)  
2. Table discovery in Unity Catalog (or legacy databases) with regex filters and safety caps  
3. Table metadata capture (`DESCRIBE DETAIL`, `DESCRIBE EXTENDED`, `information_schema`)  
4. Targeted sampling for column statistics, hints, and optional datetime heuristics  
5. Primary/foreign key candidate scoring with declared constraint lift, sampled uniqueness checks, referential coverage, and optional composite inference  
6. Report rendering + confidence labelling, followed by output persistence to UC Volumes or an explicitly supplied path

All work is confined to a single notebook file to ease review and deployment. The code is defensive, logs progress, honours runtime ceilings, and gracefully degrades when optional metadata (e.g., `information_schema`) is unavailable.

## Key Capabilities

- **Unity Catalog–first discovery** with legacy fallbacks, regex filters, and strict safety caps.
- **Advanced key analytics** that blend declared constraints, distinct ratios, nullity, naming hints, and sampled duplicate detection to emit PK confidence bands.
- **Relationship inference with confidence scoring** using name similarity, family compatibility, cardinality heuristics, and sampled referential coverage (high/medium/low labels).
- **Composite key support** for both primary and foreign keys, including hashed overlap checks when enabled.
- **Performance-aware sampling** with bounded row/column operations so the notebook scales across large domains without exhausting cluster resources.
- **Audit-friendly artefacts** (JSON, Markdown + Mermaid ERD, HTML ERD, GraphML, CSV, optional Delta tables) enriched with per-relationship evidence and per-column confidence metrics.

---

## Platform Requirements

- Databricks workspace with **Unity Catalog** enabled (legacy databases supported as a fallback)
- Runtime: Databricks Runtime 11.x (Spark 3.3) or newer
- Cluster with PySpark (no external libraries required)
- Read access to the target catalog/schema (and `information_schema` for richer metadata)
- Storage location for outputs: UC Volume or a pre-provisioned DBFS / external path
- Optional: GraphML viewer (yEd, Gephi) for visualising exported graphs

---

## Quick Start

### Unity Catalog Workflow

1. Import the `Notebook` file into Databricks (Repos, Workspace, or directly into a notebook).  
2. Attach the notebook to a cluster with appropriate UC privileges.  
3. Execute the first cell. Widgets are created automatically for catalog, schema, and tuning parameters.  
4. Provide values for `CATALOG`, `SCHEMA`, and output location (`UC_VOLUME_PATH` or `OUTPUT_BASE`).  
5. Click **Run All**.  
6. Upon completion, the final cell prints a summary and the output directory containing all artefacts.

### Legacy Database Workflow

1. Set widget `USE_LEGACY_DATABASE = true` and `DATABASE = <database_name>`.  
2. Leave `CATALOG` / `SCHEMA` as defaults (they are ignored in this mode).  
3. Provide an output path (e.g., `/dbfs/tmp/datamodeler`).  
4. Execute the notebook; legacy-compatible logic is invoked automatically.

---

## Configuration Reference

All parameters are available as Databricks widgets and can be overwritten in Jobs or via the REST API. Defaults favour quick surveys.

| Parameter | Type | Default | Description |
| --- | --- | --- | --- |
| `CATALOG` | string | `main` | Unity Catalog to inspect |
| `SCHEMA` | string | `default` | Unity Catalog schema |
| `USE_LEGACY_DATABASE` | bool | `false` | Use legacy schema discovery (`DATABASE`) instead of UC |
| `DATABASE` | string | `default` | Legacy database name when the above is `true` |
| `INCLUDE_TABLES_RE` | regex | `.*` | Tables to include (regex on table name) |
| `EXCLUDE_TABLES_RE` | regex | `` | Tables to exclude (regex on table name) |
| `MAX_TABLES` | int | `10000` | Hard cap to prevent runaway discovery |
| `FAST_MODE` | bool | `true` | Skip heavy computations (overlap, composite inference, deep stats) |
| `ENABLE_VALUE_OVERLAP` | bool | `false` | Sample FK⊆PK value overlap (requires `FAST_MODE=false`) |
| `ENABLE_COMPOSITE_FK_INFERENCE` | bool | `false` | Infer composite FKs via hashed sampling (requires `FAST_MODE=false`) |
| `ENABLE_ADVANCED_KEY_TESTS` | bool | `true` | Run sampled PK uniqueness and referential coverage scoring (disabled automatically in `FAST_MODE`) |
| `ROW_SAMPLE_FRACTION` | float | `0.05` | Fraction sampled for stats (bounded by `ROW_SAMPLE_MAX`) |
| `ROW_SAMPLE_MAX` | int | `100000` | Maximum sampled rows per table |
| `NULLABILITY_SAMPLE_ROWS` | int | `50000` | Rows inspected for null ratio |
| `VALUE_OVERLAP_SAMPLE_ROWS` | int | `50000` | Rows per table for overlap checks |
| `MIN_REL_SCORE` | float | `0.55` | Minimum score to emit inferred relationships |
| `TOP_PK_CANDIDATES` | int | `6` | Candidate columns considered for composite PK search |
| `MAX_COMPOSITE_K` | int | `3` | Maximum size of composite PKs (1–4 supported) |
| `INCLUDE_EXAMPLES` | bool | `false` | Capture up to three example rows per table |
| `ENABLE_DATETIME_DETECTION` | bool | `false` | Detect datetime patterns in string columns (slow) |
| `DATETIME_SAMPLE_SIZE` | int | `100` | String samples to test for datetime detection |
| `DATETIME_SUPPORT_MIN` | float | `0.50` | Support threshold to emit a detected datetime pattern |
| `PK_SAMPLE_ROWS` | int | `20000` | Rows sampled to validate PK uniqueness |
| `PK_ACCEPT_THRESHOLD` | float | `0.70` | Minimum PK score to treat a column as a key |
| `PK_HIGH_CONFIDENCE` | float | `0.90` | PK score required for a “high” confidence label |
| `RELATIONSHIP_HIGH_CONFIDENCE` | float | `0.75` | Relationship score required for a “high” confidence label |
| `WRITE_FILES` | bool | `true` | Persist artefacts to storage; disabled when no path supplied |
| `USE_UC_VOLUME_OUTPUT` | bool | `true` | Prefer UC Volume paths (recommended) |
| `UC_VOLUME_PATH` | string | `` | Destination, e.g. `/Volumes/<catalog>/<schema>/<volume>/erd` |
| `OUTPUT_BASE` | string | `` | Alternative base path (e.g. `/dbfs/...` or external mount) |
| `EMIT_GRAPHML` | bool | `true` | Export GraphML for external tooling |
| `EMIT_MERMAID` | bool | `true` | Embed Mermaid ERD in Markdown output |
| `EMIT_HTML` | bool | `true` | Emit standalone interactive HTML ERD |
| `MAX_MERMAID_TABLES` | int | `150` | Skip Mermaid when table count exceeds this cap |
| `MERMAID_MAX_COLUMNS` | int | `20` | Columns per entity in Mermaid (`-1` for unlimited) |
| `HTML_MAX_COLUMNS` | int | `30` | Columns per entity in the HTML ERD (`-1` for unlimited) |
| `MD_TRUNCATE_TYPE_CHARS` | int | `30` | Truncate long type strings in Markdown (`-1` for unlimited) |
| `WRITE_CSVS` | bool | `true` | Emit `entities.csv`, `columns.csv`, `relationships.csv` |
| `WRITE_DELTA_SUMMARY` | bool | `false` | Write Delta tables (`delta_columns`, `delta_relationships`) |
| `SKIP_INFORMATION_SCHEMA` | bool | `false` | Force discovery without `information_schema` |
| `MAX_RUNTIME_MINUTES` | int | `30` | Soft runtime guard; phases stop once exceeded |
| `SESSION_TIMEZONE` | string | `UTC` | Applied to `spark.sql.session.timeZone` |
| `VERBOSE_LOGS` | bool | `true` | Print banners, progress, and Markdown preview |

> **Sanity checks:** Booleans are normalised, numeric inputs are clamped to safe ranges, and incompatible toggles (e.g. overlap inference in fast mode) are automatically disabled with warnings.

---

## Execution Flow

1. **Widget Bootstrap & Configuration Sanitisation**  
   Widgets are created (if running interactively) and settings are validated. Missing output paths automatically disable `WRITE_FILES` to avoid unexpected DBFS usage.

2. **Namespace Discovery**  
   `SHOW TABLES` is executed with include/exclude filters and `MAX_TABLES` guardrails. Unity Catalog runs also collect `information_schema.tables` metadata when available.

3. **Constraint & Metadata Harvesting**  
   Declared PK/FK/check constraints are lifted via `information_schema.*`. Each table gets `DESCRIBE DETAIL` and `DESCRIBE EXTENDED` metadata, with nested type introspection for STRUCT/ARRAY/MAP columns.

4. **Statistics & Heuristics**  
   Bounded sampling produces null ratio estimates, optional approx distinct counts, sample rows (if enabled), and optional datetime format detection for string columns.

5. **Key Discovery**  
   Column-level scores consider distinctness, nullability, naming hints, and declared constraints. Optional composite search hashes candidate column sets to detect strong multi-column keys.

6. **Relationship Inference**  
   Declared FKs are emitted first. Additional candidates score name similarity, type compatibility, distinct ratios, null ratios, cardinality hints, and optional value overlap checks.

7. **Reporting & Persistence**  
   A canonical JSON model feeds Markdown, Mermaid ERD, GraphML, CSVs, and optional Delta tables. A manifest captures run parameters, counts, Spark environment, and warnings/errors for audit trails.

---

## Output Artefacts

When `WRITE_FILES=true` and a valid path is supplied, the notebook creates a run-specific folder containing:

- `data_model.json` – canonical entity/column/relationship model with metadata, evidence, and per-item confidence labels  
- `data_model.md` – human-readable report with summaries, table profiles (including PK/FK confidence), relationships, and optional Mermaid diagram  
- `data_model.graphml` – GraphML for graph visualisation tools, annotated with relationship confidence (optional toggle)  
- `entities.csv` / `columns.csv` / `relationships.csv` – curated extracts; column output includes PK/FK scores, confidence, and sampled uniqueness, relationships include confidence levels  
- `run_manifest.json` – execution manifest (parameters, counts, Spark environment, warnings, errors, output path)  
- `delta_columns` / `delta_relationships` – optional Delta tables for downstream processing

---

## Operational Modes

| Mode | When to Use | Key Settings |
| --- | --- | --- |
| **Fast Survey** (default) | Daily/adhoc scans, catalog health checks | `FAST_MODE=true`, leave overlap/composite/datetime detection disabled |
| **Deep Inference** | Design reviews, onboarding unfamiliar schemas, data lineage exercises | `FAST_MODE=false`, enable value overlap/composite FK inference, optionally enable datetime detection |
| **Targeted Audit** | Focused domain review | Provide `INCLUDE_TABLES_RE`, tighten `MAX_TABLES`, optionally increase `ROW_SAMPLE_FRACTION` on small domains |

Additional tips:
- Use UC Volumes for outputs when possible to stay within Unity Catalog governance.  
- Keep `INCLUDE_EXAMPLES=false` for PII-sensitive domains.  
- Lower `ROW_SAMPLE_FRACTION` for very large tables to reduce shuffle pressure.  
- Increase `MIN_REL_SCORE` to surface only the highest-confidence inferred relationships.
- Leave `ENABLE_ADVANCED_KEY_TESTS=true` for production-grade confidence scoring; it is automatically disabled when `FAST_MODE=true`.

---

## Running in Databricks Jobs

1. Create a Databricks Job → **Notebook task** → point to this notebook.  
2. Set job parameters to override widgets (JSON map of key/value pairs).  
3. Configure the cluster policy to grant access to the target catalog/schema and the output volume.  
4. (Optional) Chain a follow-up task that consumes the generated JSON/Delta outputs for documentation or governance pipelines.  
5. Monitor job runs; the notebook emits a concise summary plus the number of warnings/errors encountered.

---

## Operational Guardrails & Security

- **Access control:** the notebook never elevates privileges; it respects the active cluster identity.  
- **Runtime limits:** `MAX_RUNTIME_MINUTES` provides a soft stop—subsequent phases respect the deadline and truncate work safely.  
- **Error handling:** failures (e.g. missing tables, `information_schema` restrictions) are converted into warnings where possible to keep the run progressing.  
- **Output paths:** no default DBFS usage—users must opt in via `UC_VOLUME_PATH` or `OUTPUT_BASE`.  
- **Deterministic run IDs:** every execution receives a time-stamped, UUID-suffixed run identifier used across filenames and manifest records.

---

## Troubleshooting

| Symptom | Likely Cause | Resolution |
| --- | --- | --- |
| No tables discovered | Incorrect catalog/schema or restrictive regex | Confirm widget values, clear `EXCLUDE_TABLES_RE`, or reduce `MAX_TABLES` |
| `information_schema` warnings | Missing privileges on UC metadata | Request `USE CATALOG` / `USE SCHEMA` alongside `SELECT` on `information_schema.*`, or set `SKIP_INFORMATION_SCHEMA=true` |
| Slow execution | Large tables with deep inference enabled | Keep `FAST_MODE=true`, lower `ROW_SAMPLE_FRACTION`, or tighten the include regex |
| Missing inferred relationships | Strict score threshold or limited stats | Lower `MIN_REL_SCORE`, disable `FAST_MODE`, enable value overlap checks |
| Low-confidence relationships | Limited referential evidence due to sampling caps | Increase `VALUE_OVERLAP_SAMPLE_ROWS`, disable `FAST_MODE`, or ensure `ENABLE_ADVANCED_KEY_TESTS=true` |
| GraphML viewer issues | Tool-specific rendering quirks | Use the embedded Mermaid ERD or inspect the Markdown report |

---

## Change Log

- **2.1.0**  
  - Added advanced PK uniqueness sampling, FK referential coverage analysis, and confidence labelling (high/medium/low) across columns and relationships  
  - Extended configuration with thresholds for acceptance/high-confidence, plus manifest and artifact updates surfacing the new metrics  
  - Enhanced Markdown, GraphML, and CSV outputs to include scores, confidence indicators, and sampled evidence for audit trails
- **2.0.0**  
  - Rebuilt as a single self-contained notebook with class-based orchestration and explicit runtime guards  
  - Unity Catalog–first execution path with safe fallbacks for legacy databases  
  - Expanded documentation, manifest, and logging to meet enterprise audit expectations  
  - Outputs default to UC Volumes, avoiding silent DBFS writes  
  - Improved configuration sanitisation and reporting, ready for Databricks Jobs integration

For previous behaviour or simulator harnesses, see the project history.

---

**Contact / Extensions:**  
The notebook is production-ready but intentionally self-contained. Extend by forking the notebook, adjusting configuration defaults, or layering downstream transformations on the JSON/Delta outputs.
