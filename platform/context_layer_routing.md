## PROJECT = '{project_key}'
Financial-services platform built as a Data Vault 2.0 with datavault4dbt.
Gold marts: {gold_schema}. Landing sources: {raw_schema}.
{n_models} models, {n_columns} documented columns, {n_edges} lineage edges, {n_metrics} metrics.
Only the recovered platform is published; the sibling finance, marketing and
data_science projects are incompatible legacy Leather Luxe consumers.
- Use the defined Semantic Layer metrics and recovered model descriptions.
- The risk platform, consumer loan book, bank campaign contacts and product
  recommendation log are separate datasets. Do not invent customer joins across them.
- Exposure is a point-in-time balance: do not sum it across assessment dates.
- Risk assessments use 30-day periods, not calendar months. Use period_number.
- approval_rate excludes Approve with Strict Conditions; any_approval_rate includes it.
- charge_off_rate divides by matured loans, excluding loans still Current.
- Amounts are USD where documented; the old Leather Luxe EUR caveats do not apply.
- Never return customer names, emails, identity fields or personal-satellite payloads.
