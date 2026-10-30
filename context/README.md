# Context layer

What an AI agent needs to answer questions about Leather Luxe correctly, in three layers.
All of it lives in git next to the models it describes.

## 1. dbt metadata (the source of truth)

Lives in the `*.yml` files of the four projects.

- Descriptions on every public model and column, with units and grain.
- `meta` on every model: `owner`, `domain`, `tier`, `pii_contains`, `freshness_sla`.
- Contracts on the public platform marts, so a column cannot change type or vanish
  without a failed build.
- The Semantic Layer: governed metrics next to the platform marts
  (`04_marts/core/*.yml`, `05_semantic_layer/`) and in the finance and marketing projects.

The dbt MCP server and dbt Wizard read this layer directly.

## 2. context/ documents (for agents and people)

| File | Use it for |
|---|---|
| [`agent_instructions.md`](agent_instructions.md) | The rules an agent follows: routing, hard rules, privacy, answer format |
| [`data_caveats.md`](data_caveats.md) | Every verified trap in the data, with the numbers |
| [`business_glossary.md`](business_glossary.md) | What each business term means and which column or metric holds it |
| [`sample_questions.md`](sample_questions.md) | 15 questions with verified answers: demo script and agent evaluation set |
| [`fivetran_context_layer_routing.md`](fivetran_context_layer_routing.md) | Template of this project's section of the Fivetran Context Layer routing note |

## 3. Fivetran Context Layer (for agents outside dbt)

`context_layer/publish_context_layer.py` reads each project's `target/manifest.json` and
`target/semantic_manifest.json` and writes one SQL script that loads models, columns,
lineage and metrics (each with a `recipe_sql` that reproduces the governed number) into
the `AGENTS.DBT_*` tables under `PROJECT = 'scalefree_webinar'`, plus this project's
section of the `AGENTS.ROOT` routing note.

```bash
# Generate the manifests with the PROD prefix so relation names match production
# (or download manifest.json and semantic_manifest.json from each project's prod job).
export DBT_DATABASE_PREFIX=PROD
for p in platform finance marketing data_science; do (cd $p && dbt parse); done

python3 context_layer/publish_context_layer.py \
    --agents-schema <DB>.AGENTS \
    --raw-schema PROD__BASE__STAGE.SOURCE_DATA__WEBSHOP
```

Review `context_layer/generated/load_context_layer.sql`, then run it with a role that can
write to `AGENTS`. Every statement is scoped to this project's rows and its own section of
the routing note, so other projects in the same Context Layer are left alone. The routing
note is shared by every project's agents: load it only into an `AGENTS` schema you are
allowed to change.

Do not pass `--gold-database` here: the marts live in several databases
(`PROD__BASE__CORE`, `PROD__MARKETING__MARTS`, `PROD__DATA_SCIENCE__MARTS`) and the
manifests already carry the right names when parsed with the PROD prefix.

## Keeping it true

When a model or metric changes, update its YAML, re-check the affected numbers in
`data_caveats.md` and `sample_questions.md`, and regenerate the Context Layer script.
