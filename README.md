# Data_Modeler

Universal ERD & Data Model Generator (Databricks / PySpark)
Version: 1.9.2
Author: Joshua Williamson
What this is (plain language)
This script scans a Databricks database (Unity Catalog–first, legacy DB optional), figures out what tables and columns exist, guesses keys and relationships, then produces a complete, portable data model:
A JSON file with the full machine-readable model.
A Markdown report with readable tables and an optional Mermaid ERD diagram.
Optional GraphML file for graph tools (Gephi, yEd, etc.).
Optional CSVs (entities, columns, relationships).
Optional Delta tables (columns, relationships) for governance/reporting.
It’s designed to avoid full table scans, work even with partial permissions, and handle nested types (STRUCT/ARRAY/MAP) with parsing guidance.
Key features
Unity Catalog first: pulls tables and constraints from information_schema when available; safe fallbacks elsewhere.
Fail-open ERD: emits entities/columns even with access gaps; relationships are best-effort.
Bounded performance: sampling is capped and configurable; DESCRIBE DETAIL used for fast row counts when possible.
Relationship inference: respects declared PK/FK, infers single-column and optional composite FKs.
Nested types: recursively walks STRUCT/ARRAY/MAP; includes “how to parse” hints.
Outputs for everyone: JSON, Markdown (+Mermaid), GraphML, CSVs; optional Delta summary.
Hardening: XML-safe GraphML, validated parameters, resilient regex, divide-by-zero guards, explicit “no tables found” checks.
Config toggles: caps for diagram readability, truncation controls, threshold for datetime detection, verbose logging switch.
How it works — phase by phase
Configuration load
Reads parameters from Databricks widgets (if present) or from placeholders/defaults.
Validates ranges and patterns (e.g., regex, fractions, score thresholds).
Namespace discovery
Unity Catalog: SHOW TABLES + information_schema.tables.
Legacy DB (optional): SHOW TABLES.
Applies include/exclude regex and a MAX_TABLES safety cap.
Constraint lift (Unity Catalog)
Attempts to pull PRIMARY KEY, FOREIGN KEY, and CHECK constraints from information_schema.*.
Builds declared PK/FK registry for later use.
Entity metadata & fast counts
Loads schemas for each table/view and tries DESCRIBE DETAIL for fast numRows.
Captures props from DESCRIBE EXTENDED (best-effort).
Sampling & statistics (bounded)
Calculates null ratios from a sample.
Optionally computes approx distinct (not in FAST_MODE).
Collects example rows (opt-in).
Detects datetime formats for STRING columns with a configurable support threshold.
Key candidates & composites
Scores columns as PK/FK candidates (name hints + stats).
Honors declared PKs. Optionally searches for composite PKs using NULL-safe hashed combos on a sample.
Relationship assembly
Loads declared FKs first (single or multi-column), generates join examples.
Infers high-confidence single-column FKs across entities (configurable threshold).
Optional composite FK inference by name + optional overlap check.
Model & rendering
Assembles a single model JSON with entities, columns, stats, relationships, warnings, errors.
Renders Markdown (readable report) and optional Mermaid ERD.
Emits GraphML with XML-escaped label data; CSVs; optional Delta tables.
Output & manifest
Writes to DBFS or a UC Volume path (if configured).
Produces a run manifest with parameters, counts, environment, warnings, errors.
Requirements & dependencies
Platform: Databricks (Unity Catalog recommended; legacy DB supported).
Spark: Apache Spark 3.x (Databricks Runtime 11+ recommended).
Language: Python (Databricks notebook/Job, PySpark available by default).
Permissions: Read access to schemas/tables; optional access to UC information_schema for richer results.
Storage: DBFS or a UC Volume path for outputs (JSON/MD/GraphML/CSV/Delta).
Optional tools:
GraphML viewers (yEd, Gephi) if you plan to open *.graphml.
Markdown viewer (Databricks notebook or any MD-capable viewer) for the report.
No external PyPI packages are required beyond the Databricks/PySpark stack.
Quick start (default, Unity Catalog)
Add the script to a Databricks notebook cell.
Set parameters via widgets (top of the script auto-creates them):
CATALOG = your_catalog
SCHEMA = your_schema
Leave defaults unless you know you need changes.
Run the notebook.
Open outputs:
Paths will print in the console (e.g., /dbfs/tmp/datamodeler/run_.../data_model.md).
Download data_model.md for a human-readable report and data_model.json for machine use.
Quick start (legacy database only)
Set USE_LEGACY_DATABASE=true and DATABASE=<db_name>.
CATALOG/SCHEMA are ignored in this mode.
Configuration (most used parameters)
You can set these via Databricks widgets (preferred) or replace placeholders in the code if running non-interactively.
Param	Type	Default	What it does
CATALOG	string	main	UC catalog to scan.
SCHEMA	string	default	UC schema to scan.
USE_LEGACY_DATABASE	bool	false	Use legacy database instead of UC.
DATABASE	string	default	Legacy DB name (if used).
INCLUDE_TABLES_RE	regex	.*	Include tables matching regex.
EXCLUDE_TABLES_RE	regex	``	Exclude tables matching regex.
FAST_MODE	bool	true	Skips heavier stats (approx distinct, overlap). Faster.
ENABLE_VALUE_OVERLAP	bool	false	Sampled FK⊆PK overlap check. Requires FAST_MODE=false.
ENABLE_COMPOSITE_FK_INFERENCE	bool	false	Attempt composite FK inference. Requires FAST_MODE=false.
INCLUDE_EXAMPLES	bool	false	Include up to 3 example rows per entity.
ROW_SAMPLE_FRACTION	float	0.10	Sample fraction for stats (0–1).
ROW_SAMPLE_MAX	int	500000	Row cap per table sample.
NULLABILITY_SAMPLE_ROWS	int	300000	Rows for null% calc.
VALUE_OVERLAP_SAMPLE_ROWS	int	150000	Rows per side for overlap.
MIN_REL_SCORE	float	0.55	Threshold for inferred FKs.
TOP_PK_CANDIDATES	int	6	Top-N columns considered for composite PK search.
MAX_COMPOSITE_K	int	3	Max columns in composite PK (1–4).
MAX_TABLES	int	10000	Safety cap for discovery.
WRITE_FILES	bool	true	Write all output artifacts to disk.
OUTPUT_BASE	path	/dbfs/tmp/datamodeler/<run_id>	Base output folder if not using Volumes.
USE_UC_VOLUME_OUTPUT	bool	false	Write into a UC Volume.
UC_VOLUME_PATH	path	``	Volume path (e.g., /Volumes/<cat>/<schema>/<vol>/erd).
EMIT_GRAPHML	bool	true	Write GraphML file.
EMIT_MERMAID	bool	true	Include Mermaid ERD in Markdown.
MAX_MERMAID_TABLES	int	150	Omit Mermaid if too many tables.
MERMAID_MAX_COLUMNS	int	20	Max columns per entity in Mermaid (-1 for unlimited).
MD_TRUNCATE_TYPE_CHARS	int	30	Truncate long type strings in MD (-1 for unlimited).
DATETIME_SUPPORT_MIN	float	0.50	Minimum support to emit a detected datetime format (0–1).
VERBOSE_LOGS	bool	true	Print run summary and preview.
WRITE_CSVS	bool	true	Emit entities.csv, columns.csv, relationships.csv.
WRITE_DELTA_SUMMARY	bool	false	Write Delta tables for columns/relationships.
Notes
ENABLE_VALUE_OVERLAP and ENABLE_COMPOSITE_FK_INFERENCE are ignored when FAST_MODE=true.
If you want maximum inference quality, set FAST_MODE=false (heavier but smarter).
Output artifacts
data_model.json — Complete canonical model:
Spark env, namespace metadata
Entities → columns (stats, hints)
Relationships (declared + inferred) with evidence
Warnings and errors
data_model.md — Human-readable report with:
Summary, entities/columns tables, relationships catalog
Complex type parsing tips, datetime parsing notes
Optional Mermaid ERD diagram
data_model.graphml — GraphML with nodes (tables) and edges (FKs).
entities.csv, columns.csv, relationships.csv — spreadsheets for quick triage.
delta_columns, delta_relationships (optional) — Delta datasets for BI/governance.
run_manifest.json — run parameters, counts, partial warnings list, environment, output dir.
Typical modes
“Fast survey”
Set: FAST_MODE=true (default).
Leave ENABLE_VALUE_OVERLAP=false, ENABLE_COMPOSITE_FK_INFERENCE=false.
Good for large catalogs to get a quick map of entities/columns and high-confidence FKs.
“Deep inference”
Set: FAST_MODE=false, ENABLE_VALUE_OVERLAP=true, ENABLE_COMPOSITE_FK_INFERENCE=true.
Consider lowering ROW_SAMPLE_FRACTION if data is massive.
Expect longer runtimes; better FK accuracy, especially on denormalized or sparse keys.
Using in a Databricks Job
Create a Job → “Notebook” task → point to the notebook with this code.
Under Parameters, define any widget values you want to override (e.g., CATALOG, SCHEMA, OUTPUT_BASE, etc.).
Optional: schedule it or chain as part of documentation/governance pipelines.
Outputs are written to OUTPUT_BASE or a UC Volume if configured.
Best practices
Scope discovery with INCLUDE_TABLES_RE / EXCLUDE_TABLES_RE to keep runs focused.
Use UC where possible: declared PK/FK via information_schema greatly improves accuracy.
Keep FAST_MODE=true for daily runs; use “Deep inference” only when you need to validate complex relationships.
Protect PII: set INCLUDE_EXAMPLES=false (default) for sensitive datasets; examples are never required.
Capture state: archive data_model.json in version control (or Delta) to track schema drift over time.
Diagram size: adjust MERMAID_MAX_COLUMNS and MAX_MERMAID_TABLES to keep diagrams readable.
Troubleshooting
“No tables found”
Check CATALOG/SCHEMA or DATABASE.
Confirm your regex include/exclude patterns. Try INCLUDE_TABLES_RE=.* and clear EXCLUDE_TABLES_RE.
Validate permissions on the namespace, and information_schema if using UC.
Slow runs
Use FAST_MODE=true.
Reduce ROW_SAMPLE_FRACTION and ROW_SAMPLE_MAX.
Limit scope via INCLUDE_TABLES_RE.
Skip GraphML: EMIT_GRAPHML=false.
Missing/inaccurate relationships
Lower MIN_REL_SCORE slightly (e.g., 0.5 → 0.45) to see more candidates.
Switch to deep mode: FAST_MODE=false and ENABLE_VALUE_OVERLAP=true.
Ensure PK candidates exist; declared PKs help a lot.
GraphML fails or looks odd
Names with special characters are escaped, but some tools render differently.
Open the MD report’s Mermaid diagram for a second view.
Weird datetime detection
Mixed formats in a single column can reduce confidence.
Adjust DATETIME_SUPPORT_MIN down (e.g., 0.35) to surface minority formats.
Security & privacy
The script does not copy raw data unless you enable INCLUDE_EXAMPLES; even then it limits to 3 small rows.
Stats and overlaps are sampled and bounded; no full scans by default.
Outputs may contain table/column names and basic stats; classify and store them appropriately.
Example: minimal UC run (copy/paste into a cell above the code)
# Set parameters directly (optional if you use widgets)
dbutils.widgets.text("CATALOG", "prod", "UC Catalog")
dbutils.widgets.text("SCHEMA", "finance", "UC Schema")
dbutils.widgets.text("FAST_MODE", "true", "Fast mode")
dbutils.widgets.text("OUTPUT_BASE", f"/dbfs/tmp/datamodeler/manual_run")

# Optional narrowing
dbutils.widgets.text("INCLUDE_TABLES_RE", "^(txn_|dim_|fact_).*", "Regex include")
dbutils.widgets.text("EXCLUDE_TABLES_RE", "", "Regex exclude")
Run the notebook. Open the printed output path to get data_model.md and data_model.json.
Example: deep inference profile
dbutils.widgets.text("CATALOG", "prod", "")
dbutils.widgets.text("SCHEMA", "crm", "")
dbutils.widgets.text("FAST_MODE", "false", "")
dbutils.widgets.text("ENABLE_VALUE_OVERLAP", "true", "")
dbutils.widgets.text("ENABLE_COMPOSITE_FK_INFERENCE", "true", "")
dbutils.widgets.text("ROW_SAMPLE_FRACTION", "0.05", "")  # tighten a bit if very large
dbutils.widgets.text("MIN_REL_SCORE", "0.5", "")
dbutils.widgets.text("DATETIME_SUPPORT_MIN", "0.40", "")
Versioning & changelog (highlights)
1.9.2 (Best-of-Both)
Stable overlap cache key (_jdf.toString() with fallback).
Configurable Mermaid column cap & MD type truncation.
Configurable datetime support threshold.
Per-get widget fallback; verbose logging toggle.
Retains all 1.9.1 hardening (XML escaping, validations, robust inference).
1.9.1 (Reviewed & Enhanced)
GraphML XML escaping, better error handling, improved composite PK logic, safer scoring.
1.9.0
UC-first discovery, nested guidance, Mermaid/GraphML, CSVs, optional Delta.
