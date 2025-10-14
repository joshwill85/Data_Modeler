# Universal ERD & Data Model Generator (Databricks / PySpark)

**Version:** 1.9.2 (Best-of-Both)  
**Author:** Joshua Williamson

> Generate a complete, portable data model from your Databricks environment (Unity Catalog–first, legacy DB optional) with safe defaults, bounded sampling, and clean artifacts (JSON, Markdown + Mermaid ERD, GraphML, CSVs, optional Delta tables).

---

## Table of Contents

- [What This Is (Plain Language)](#what-this-is-plain-language)
- [Key Features](#key-features)
- [How It Works — Phase by Phase](#how-it-works--phase-by-phase)
- [Requirements & Dependencies](#requirements--dependencies)
- [Quick Start](#quick-start)
  - [Unity Catalog](#unity-catalog)
  - [Legacy Database](#legacy-database)
- [Configuration](#configuration)
- [Output Artifacts](#output-artifacts)
- [Typical Modes](#typical-modes)
  - [Fast Survey](#fast-survey)
  - [Deep Inference](#deep-inference)
- [Using in a Databricks Job](#using-in-a-databricks-job)
- [Best Practices](#best-practices)
- [Troubleshooting](#troubleshooting)
- [Examples](#examples)
  - [Minimal UC Run](#minimal-uc-run)
  - [Deep Inference Profile](#deep-inference-profile)
- [Versioning & Changelog (Highlights)](#versioning--changelog-highlights)
- [Contributing](#contributing)
- [FAQ](#faq)

---

## What This Is (Plain Language)

This script scans your Databricks namespace, discovers tables and columns, identifies (or infers) keys and relationships, and produces a **complete data model**:

- **JSON** (machine-readable canonical model)
- **Markdown** report with optional **Mermaid ERD**
- **GraphML** for graph tools (yEd, Gephi)
- **CSVs** (entities, columns, relationships)
- Optional **Delta tables** for governance/BI

It avoids full table scans by default, works even with partial permissions, and includes guidance for parsing nested types (STRUCT/ARRAY/MAP).

---

## Key Features

- **Unity Catalog first**: Leverages `information_schema` where available; safe fallbacks elsewhere.
- **Fail-open ERD**: Emits entities/columns even with limited access; infers relationships best-effort.
- **Bounded performance**: Sampling is capped and configurable; `DESCRIBE DETAIL` used for fast row counts.
- **Relationship inference**: Respects declared PK/FK; infers single-column FKs and optional composite FKs.
- **Nested type support**: Walks STRUCT/ARRAY/MAP; provides SQL/PySpark parsing hints.
- **Hardened outputs**: XML-escaped GraphML, validated params/regex, divide-by-zero guards, explicit "no tables" checks.
- **Config toggles**: Diagram readability caps, type truncation, datetime detection threshold, verbose logging.

---

## How It Works — Phase by Phase

1. **Configuration Load**  
   Reads widgets/placeholders, validates ranges (fractions, thresholds) and regex patterns.

2. **Namespace Discovery**  
   UC: `SHOW TABLES` + `information_schema.tables`; Legacy: `SHOW TABLES`. Applies include/exclude regex and `MAX_TABLES`.

3. **Constraint Lift (UC)**  
   Attempts to load PRIMARY KEY, FOREIGN KEY, and CHECK constraints from `information_schema.*`.

4. **Entity Metadata & Fast Counts**  
   Reads schemas and `DESCRIBE DETAIL` for fast `numRows`. Captures `DESCRIBE EXTENDED` props (best-effort).

5. **Sampling & Statistics**  
   Null ratios (sampled), optional approx distinct (off in FAST mode), optional example rows, datetime format detection with configurable support threshold.

6. **Key Candidates & Composites**  
   Scores PK/FK candidates; honors declared PKs; optionally searches composite PKs via NULL-safe hashed combos.

7. **Relationships**  
   Adds declared FKs first (single & multi-column) with join examples; infers single-column FKs; optional composite FK inference.

8. **Model & Rendering**  
   Assembles a single JSON model; renders Markdown (+Mermaid), GraphML, CSVs, and optional Delta tables.

9. **Output & Manifest**  
   Writes to DBFS or UC Volume; emits a run manifest (parameters, counts, env, warnings, errors).

---

## Requirements & Dependencies

- **Platform:** Databricks (Unity Catalog recommended; legacy DB supported)  
- **Spark:** Apache Spark 3.x (Databricks Runtime 11+ recommended)  
- **Language:** Python (Databricks notebook/Job with PySpark)  
- **Permissions:** Read access to schemas/tables; UC `information_schema` access recommended  
- **Storage:** DBFS or UC Volume for outputs  
- **Optional:** GraphML viewer (yEd/Gephi), Markdown viewer

> No additional PyPI dependencies beyond Databricks/PySpark.

---

## Quick Start

### Unity Catalog

1. Add the script to a Databricks notebook cell.  
2. Set widgets (auto-created by the script):
   - `CATALOG = your_catalog`
   - `SCHEMA  = your_schema`
3. Run the notebook.  
4. Open printed output path (e.g., `/dbfs/tmp/datamodeler/run_...`) → `data_model.md` and `data_model.json`.

### Legacy Database

- Set `USE_LEGACY_DATABASE=true` and `DATABASE=<db_name>`.  
- `CATALOG/SCHEMA` are ignored in this mode.

---

## Configuration

Set via widgets or by replacing placeholders for non-interactive runs.

| Param | Type | Default | Purpose |
|---|---|---:|---|
| `CATALOG` | string | `main` | UC catalog to scan |
| `SCHEMA` | string | `default` | UC schema to scan |
| `USE_LEGACY_DATABASE` | bool | `false` | Use legacy database instead of UC |
| `DATABASE` | string | `default` | Legacy DB name (if used) |
| `INCLUDE_TABLES_RE` | regex | `.*` | Include tables matching regex |
| `EXCLUDE_TABLES_RE` | regex | `` | Exclude tables matching regex |
| `FAST_MODE` | bool | `true` | Skip heavy stats/overlap (faster) |
| `ENABLE_VALUE_OVERLAP` | bool | `false` | Sampled FK⊆PK overlap (requires `FAST_MODE=false`) |
| `ENABLE_COMPOSITE_FK_INFERENCE` | bool | `false` | Composite FK inference (requires `FAST_MODE=false`) |
| `INCLUDE_EXAMPLES` | bool | `false` | Include up to 3 example rows |
| `ROW_SAMPLE_FRACTION` | float | `0.10` | Sample fraction (0–1) |
| `ROW_SAMPLE_MAX` | int | `500000` | Row cap per table sample |
| `NULLABILITY_SAMPLE_ROWS` | int | `300000` | Rows for null% calc |
| `VALUE_OVERLAP_SAMPLE_ROWS` | int | `150000` | Rows per side for overlap |
| `MIN_REL_SCORE` | float | `0.55` | Threshold for inferred FKs |
| `TOP_PK_CANDIDATES` | int | `6` | Top-N columns for composite PK |
| `MAX_COMPOSITE_K` | int | `3` | Max columns in composite PK (1–4) |
| `MAX_TABLES` | int | `10000` | Safety cap for discovery |
| `WRITE_FILES` | bool | `true` | Write artifacts to disk |
| `OUTPUT_BASE` | path | `/dbfs/tmp/datamodeler/<run_id>` | Base output folder |
| `USE_UC_VOLUME_OUTPUT` | bool | `false` | Write into a UC Volume |
| `UC_VOLUME_PATH` | path | `` | `/Volumes/<cat>/<schema>/<vol>/erd` |
| `EMIT_GRAPHML` | bool | `true` | Write GraphML |
| `EMIT_MERMAID` | bool | `true` | Add Mermaid ERD to Markdown |
| `MAX_MERMAID_TABLES` | int | `150` | Omit Mermaid above this table count |
| `MERMAID_MAX_COLUMNS` | int | `20` | Max columns/entity in Mermaid (`-1` unlimited) |
| `MD_TRUNCATE_TYPE_CHARS` | int | `30` | Truncate type strings in MD (`-1` unlimited) |
| `DATETIME_SUPPORT_MIN` | float | `0.50` | Min support to emit detected datetime (0–1) |
| `VERBOSE_LOGS` | bool | `true` | Print run summary and preview |
| `WRITE_CSVS` | bool | `true` | Emit entities/columns/relationships CSVs |
| `WRITE_DELTA_SUMMARY` | bool | `false` | Write Delta summary tables |

> **Tips:**  
> • `ENABLE_VALUE_OVERLAP` & `ENABLE_COMPOSITE_FK_INFERENCE` require `FAST_MODE=false`.  
> • For maximum inference quality, set `FAST_MODE=false` (heavier, smarter).

---

## Output Artifacts

- `data_model.json` — canonical model (entities, columns, stats, relationships, warnings, errors)
- `data_model.md` — human-readable report (summary, entities, relationships, parsing guidance, optional Mermaid)
- `data_model.graphml` — nodes (tables) and edges (FKs), XML-escaped
- `entities.csv`, `columns.csv`, `relationships.csv` — spreadsheet-friendly summaries
- `delta_columns`, `delta_relationships` — optional Delta datasets for BI/governance
- `run_manifest.json` — parameters, counts, env, warnings/errors, output dir

---

## Typical Modes

### Fast Survey
- `FAST_MODE=true` (default)  
- `ENABLE_VALUE_OVERLAP=false`, `ENABLE_COMPOSITE_FK_INFERENCE=false`  
- Quick mapping of entities/columns and high-confidence FKs.

### Deep Inference
- `FAST_MODE=false`  
- `ENABLE_VALUE_OVERLAP=true`, `ENABLE_COMPOSITE_FK_INFERENCE=true`  
- Lower `ROW_SAMPLE_FRACTION` if very large; expect longer runs for better FK accuracy.

---

## Using in a Databricks Job

1. Create a **Job** → “Notebook” task → reference the notebook with this code.  
2. Set **Parameters** to override widgets (e.g., `CATALOG`, `SCHEMA`, `OUTPUT_BASE`).  
3. Optionally schedule or chain in a documentation/governance pipeline.  
4. Outputs are written to `OUTPUT_BASE` or your **UC Volume** path.

---

## Best Practices

- Narrow scope with `INCLUDE_TABLES_RE` / `EXCLUDE_TABLES_RE` to focus runs.  
- Prefer **Unity Catalog** for declared PK/FK via `information_schema`.  
- Daily runs: keep `FAST_MODE=true`; switch to **Deep Inference** as needed.  
- **PII/Sensitivity**: keep `INCLUDE_EXAMPLES=false` (default).  
- Version the **JSON**/Delta outputs to track schema drift.  
- Tune diagram readability with `MERMAID_MAX_COLUMNS` & `MAX_MERMAID_TABLES`.

---

## Troubleshooting

- **No tables found**  
  Check `CATALOG/SCHEMA` or `DATABASE`. Relax regex (e.g., `INCLUDE_TABLES_RE=.*`), clear excludes, verify permissions.

- **Slow runs**  
  Use `FAST_MODE=true`. Reduce `ROW_SAMPLE_FRACTION`/`ROW_SAMPLE_MAX`. Limit scope. Disable GraphML.

- **Missing/inaccurate relationships**  
  Lower `MIN_REL_SCORE` slightly (e.g., `0.5 → 0.45`). Use deep mode (`FAST_MODE=false`, `ENABLE_VALUE_OVERLAP=true`). Declared PKs help.

- **GraphML oddities**  
  XML is escaped; some viewers render differently. Check Mermaid ERD in the Markdown report.

- **Datetime detection weirdness**  
  Mixed formats reduce confidence. Adjust `DATETIME_SUPPORT_MIN` (e.g., `0.35`) to surface minority patterns.

---

## Examples

### Minimal UC Run

```python
# Set parameters via widgets (optional if already set)
dbutils.widgets.text("CATALOG", "prod", "UC Catalog")
dbutils.widgets.text("SCHEMA", "finance", "UC Schema")
dbutils.widgets.text("FAST_MODE", "true", "Fast mode")
dbutils.widgets.text("OUTPUT_BASE", "/dbfs/tmp/datamodeler/manual_run")

# Optional narrowing
dbutils.widgets.text("INCLUDE_TABLES_RE", "^(txn_|dim_|fact_).*", "Regex include")
dbutils.widgets.text("EXCLUDE_TABLES_RE", "", "Regex exclude")
