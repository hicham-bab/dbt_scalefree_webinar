# Cloud platform recovery

## Provenance

The platform definitions were reconstructed from production run `70403231720017`,
step 4, completed on November 2, 2026. Its source repository is
`dbt-Hands-on-Labs/lab-dbt-scalefreewebinar-6zl1moge`, branch `main`, commit
`7de584d4dc872da37e1734bb7b970fb74bc3468a`.

The recovery uses `manifest.json`, `semantic_manifest.json`, and the
`raw_customer_complaints` Parquet artifact. It does not require Git access to
that repository. Downloaded artifacts remain in ignored `target/cloud_recovery/`.

## Fidelity

- All 55 model SQL bodies and seven singular test SQL bodies are copied verbatim
  from `raw_code`, without rewriting business rules.
- YAML is reconstructed from resolved artifact metadata: eight sources, model
  descriptions, columns, contracts, groups, privacy metadata, 137 generic tests,
  five semantic models, 28 metrics, and the calendar spine.
- The three project macro definitions are recovered from the manifest.
- The complaint CSV contains the 320 rows from the deployed seed artifact.
  CSV formatting and inferred representations may differ from the original file.
- The datavault4dbt dependency is pinned to commit
  `6d832cee2d30f12c93d08533a1dad12368066dc4`: all 712 deployed package macro
  definitions match this revision, rather than today's moving `main`.
- Local e-commerce models and seeds absent from the deployed platform are removed.

This is an artifact-based reconstruction of the deployed **platform**, not a
byte-for-byte copy of the inaccessible repository. Artifacts do not retain original
YAML formatting, every project-level setting or variable, disabled resources absent
from the manifest, arbitrary scripts, documentation, or Cloud IDE uncommitted edits.
The existing project identity, Cloud project ID, source database and schema settings
remain in place; effective model configurations are recovered into model YAML.

## Compatibility

The Cloud project models financial services, not Leather Luxe e-commerce.
The sibling `finance`, `marketing`, and `data_science` projects are retained as
legacy code, not reconstructed or validated: this Cloud project supplies no
artifacts for them. They depend on removed e-commerce marts and old customer
columns and will break against this recovered platform if deployed unchanged.
For example, `finance.fct_revenue` requires `platform.fct_orders`,
`marketing.customer_segments` requires the former customer attributes, and
`data_science.customer_features` requires both.

The shared Leather Luxe context documents are marked historical and must not
be used to interpret the recovered financial-services marts. Read the recovered
model and metric descriptions instead. Do not return customer names, emails,
identity fields, or personal-satellite payloads.

The eight Fivetran source tables are external inputs; they are not recreated by
this change. Their configured landing database remains the Cloud project's
`LAB_DB_SCALEFREE_WEBINAR_6ZL1MOGE_INSTR_DEV`, including when building a production
target, matching the deployed source metadata.

## Validation and publishing

Parsing and compilation do not materialize models. Run Wizard Validation before
deployment; no warehouse builds or data tests are run during recovery.

Final compilation on November 2, 2026 succeeded for all 200 resources. Comparison
against the deployed artifacts found no differences in model SQL, model descriptions,
column metadata, contracts, access, groups, effective model configurations, source
metadata, metric definitions, dependency edges, or normalized macro definitions.
The node IDs match: 55 models, 144 tests, and one seed.

The context publisher accepts `--projects platform` so it does not publish
unrelated legacy consumer definitions. Its generated load script must be reviewed
before execution; generating it does not modify Snowflake. The regenerated
`context_layer/generated/load_context_layer.sql` targets production gold relations,
contains 55 models and 28 metrics, and uses `<AGENTS_DATABASE>.AGENTS` as an explicit
placeholder: replace it with the intended Context Layer destination before execution.
