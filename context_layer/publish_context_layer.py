#!/usr/bin/env python3
"""Publish this Mesh to the Fivetran Context Layer.

Reads target/manifest.json and target/semantic_manifest.json from each dbt project
and writes one SQL script that (re)loads this project's rows into the Context
Layer's dbt tables:

    AGENTS.DBT_MODEL       one row per model
    AGENTS.DBT_COLUMN      one row per documented column
    AGENTS.DBT_DEPENDENCY  one row per lineage edge, cross-project edges included
    AGENTS.DBT_METRIC      one row per Semantic Layer metric, with a SQL recipe
    AGENTS.ROOT            this project's section in the 'dbt routing' entry

Every statement is scoped to PROJECT = <project key>, so other projects sharing
the AGENTS schema are left alone. The script only WRITES A FILE; review it, then
run it in Snowflake with a role that can write to the AGENTS schema.

Usage (after `dbt build` or `dbt docs generate` in each project):

    python3 context_layer/publish_context_layer.py \\
        --agents-schema HOL_DATABASE_1.AGENTS \\
        --gold-database PROD__BASE__CORE \\
        --raw-schema PROD__BASE__STAGE.SOURCE_DATA__WEBSHOP

--gold-database rewrites the database of every relation, for when the manifests
were produced in DEV but agents should read PROD.
"""

import argparse
import json
import pathlib
import re
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent
PROJECTS = ["platform", "finance", "marketing", "data_science"]


def q(value):
    """SQL string literal (or NULL)."""
    if value is None:
        return "null"
    return "'" + str(value).replace("\\", "\\\\").replace("'", "''") + "'"


def load(project):
    target = ROOT / project / "target"
    manifest = json.loads((target / "manifest.json").read_text())
    semantic_path = target / "semantic_manifest.json"
    semantic = json.loads(semantic_path.read_text()) if semantic_path.exists() else {}
    return manifest, semantic


def relation(node, gold_database):
    rel = node.get("relation_name")
    if not rel:
        return None
    rel = rel.replace('"', "")
    if gold_database:
        rel = gold_database + rel[rel.index("."):]
    return rel


def plain_filter(where_sql):
    """{{ Dimension('inventory_movement__movement_type') }} = 'RECEIPT'  ->  (movement_type = 'RECEIPT')"""
    return "(" + re.sub(r"\{\{\s*(?:Dimension|TimeDimension|Entity)\('[a-z_]+__([a-z_0-9]+)'[^}]*\)\s*\}\}", r"\1", where_sql).strip() + ")"


def simple_parts(metric):
    params = metric["type_params"]["metric_aggregation_params"]
    agg = params["agg"]
    expr = params.get("expr") or metric["type_params"].get("expr") or "1"
    filters = [plain_filter(f["where_sql_template"]) for f in ((metric.get("filter") or {}).get("where_filters") or [])]
    metric_filter = " and ".join(filters) if filters else None
    if agg == "count":
        definition, inner, outer = "sum(1)", "1", "sum"
    elif agg == "count_distinct":
        definition, inner, outer = f"count(distinct {expr})", expr, "count(distinct"
    elif agg == "average":
        definition, inner, outer = f"avg({expr})", expr, "average"
    else:
        definition, inner, outer = f"{agg}({expr})", expr, agg
    if metric_filter:
        value = f"case when {metric_filter} then {inner} end"
    else:
        value = inner
    if outer == "count(distinct":
        sql = f"count(distinct {value})"
    elif outer == "average":
        sql = f"avg({value})"
    else:
        sql = f"{outer}({value})"
    return params["semantic_model"], definition, metric_filter, sql


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--agents-schema", required=True, help="DATABASE.SCHEMA of the Context Layer, e.g. HOL_DATABASE_1.AGENTS")
    ap.add_argument("--project-key", default="scalefree_webinar", help="Value written to the PROJECT column")
    ap.add_argument("--gold-database", default=None, help="Rewrite relation databases to this one (e.g. your PROD database)")
    ap.add_argument("--raw-schema", default="<your STAGE database>.SOURCE_DATA__WEBSHOP",
                    help="DATABASE.SCHEMA of the raw source tables, quoted in the routing note")
    ap.add_argument("--out", default=str(ROOT / "context_layer" / "generated" / "load_context_layer.sql"))
    args = ap.parse_args()

    agents, key = args.agents_schema, args.project_key
    models, columns, deps, metrics = [], [], set(), []
    gold_by_semantic_model, agg_time_by_semantic_model, simple = {}, {}, {}
    platform_core_schema = None

    for project in PROJECTS:
        try:
            manifest, semantic = load(project)
        except FileNotFoundError:
            sys.exit(f"{project}/target/manifest.json not found: run dbt build (or dbt parse) in {project}/ first")

        for uid, node in manifest["nodes"].items():
            if node["resource_type"] != "model" or node["package_name"] != project:
                continue
            materialized = node["config"].get("materialized")
            is_gold = (node["config"].get("meta") or {}).get("tier") == "gold" or node["config"].get("access") == "public"
            gold = relation(node, args.gold_database) if is_gold and materialized != "ephemeral" else None
            if project == "platform" and gold and node["config"].get("access") == "public" and not platform_core_schema:
                platform_core_schema = gold.rsplit(".", 1)[0]
            models.append((uid, node["name"], node["schema"] if materialized != "ephemeral" else None, materialized,
                           node.get("description") or None, node["original_file_path"],
                           json.dumps(node.get("tags") or []), gold))
            for col in (node.get("columns") or {}).values():
                if col.get("description"):
                    columns.append((uid, col["name"], col.get("data_type"), col["description"]))
            for upstream in node["depends_on"]["nodes"]:
                up_type = upstream.split(".")[0]
                if up_type in ("model", "source", "seed", "snapshot"):
                    deps.add((upstream, uid, up_type, "model"))

        for sm in semantic.get("semantic_models", []):
            rel = sm["node_relation"]
            gold = (args.gold_database or rel["database"]) + "." + rel["schema_name"] + "." + rel["alias"]
            gold_by_semantic_model[sm["name"]] = gold
            agg_time_by_semantic_model[sm["name"]] = (sm.get("defaults") or {}).get("agg_time_dimension")
        # Consumer manifests re-export the platform's semantic manifest: keep one copy.
        for m in semantic.get("metrics", []):
            if m["type"] == "simple":
                simple[m["name"]] = m
            if all(existing["name"] != m["name"] for existing in metrics):
                metrics.append(m)

    metric_rows = []
    for m in metrics:
        if m["type"] == "simple":
            sem, definition, metric_filter, value_sql = simple_parts(m)
            gold = gold_by_semantic_model[sem]
            recipe = f"select {value_sql} as {m['name']} from {gold}"
        elif m["type"] == "ratio":
            num = simple[m["type_params"]["numerator"]["name"]]
            den = simple[m["type_params"]["denominator"]["name"]]
            sem_n, _, filt_n, sql_n = simple_parts(num)
            sem_d, _, filt_d, sql_d = simple_parts(den)
            if sem_n != sem_d:
                sys.exit(f"ratio {m['name']}: numerator and denominator on different semantic models, extend the script")
            sem, gold = sem_n, gold_by_semantic_model[sem_n]
            definition = f"{num['name']} / {den['name']}"
            metric_filter = None
            recipe = (f"select cast({sql_n} as double) / nullif(cast({sql_d} as double), 0) as {m['name']} from {gold}")
        elif m["type"] == "cumulative":
            # Trailing window ending at the latest date in the data (e.g. "revenue_last_7_days").
            params = m["type_params"].get("cumulative_type_params") or {}
            window = params.get("window") or m["type_params"].get("window") or {}
            base_name = (params.get("metric") or {}).get("name") or (m["type_params"].get("metrics") or [{}])[0].get("name")
            base = simple[base_name]
            sem, _, metric_filter, value_sql = simple_parts(base)
            ts = ((base["type_params"].get("metric_aggregation_params") or {}).get("agg_time_dimension")
                  or agg_time_by_semantic_model.get(sem))
            gold = gold_by_semantic_model[sem]
            if not window or not ts:
                sys.exit(f"cumulative metric {m['name']}: only trailing windows on a semantic model with an agg_time_dimension are handled")
            n, unit = window["count"], window["granularity"]
            definition = f"{base_name} over the trailing {n} {unit}(s)"
            recipe = (f"select {value_sql} as {m['name']} from {gold} "
                      f"where {ts} > dateadd({unit}, -{n}, (select max({ts}) from {gold}))")
        else:
            sys.exit(f"metric {m['name']}: type {m['type']} not handled, extend the script")
        metric_rows.append((m["name"], m.get("label"), m["type"], m.get("description"), sem, gold,
                            definition, metric_filter, recipe))

    routing_template = (ROOT / "context" / "fivetran_context_layer_routing.md").read_text()
    routing = (routing_template
               .replace("{project_key}", key)
               .replace("{gold_schema}", platform_core_schema or "<your PROD database>.CORE")
               .replace("{raw_schema}", args.raw_schema)
               .replace("{n_models}", str(len(models)))
               .replace("{n_columns}", str(len(columns)))
               .replace("{n_edges}", str(len(deps)))
               .replace("{n_metrics}", str(len(metric_rows)))).strip()

    def values(rows):
        return ",\n".join("(" + ", ".join(q(v) for v in r) + ")" for r in rows)

    sql = [f"-- Generated by context_layer/publish_context_layer.py for PROJECT = '{key}'.",
           "-- Review before running. Every statement is scoped to this project only.",
           "begin;"]
    for table in ("DBT_MODEL", "DBT_COLUMN", "DBT_DEPENDENCY", "DBT_METRIC"):
        sql.append(f"delete from {agents}.{table} where project = {q(key)};")
    sql.append(f"insert into {agents}.DBT_MODEL (unique_id, name, schema_name, materialization, description, file_path, tags, gold_table, project)\n"
               f"select $1, $2, $3, $4, $5, $6, parse_json($7), $8, {q(key)} from values\n{values(models)};")
    sql.append(f"insert into {agents}.DBT_COLUMN (model_id, column_name, data_type, description, project)\n"
               f"select $1, $2, $3, $4, {q(key)} from values\n{values(columns)};")
    sql.append(f"insert into {agents}.DBT_DEPENDENCY (upstream_id, downstream_id, upstream_type, downstream_type, project)\n"
               f"select $1, $2, $3, $4, {q(key)} from values\n{values(sorted(deps))};")
    sql.append(f"insert into {agents}.DBT_METRIC (name, label, type, description, semantic_model, gold_table, definition, metric_filter, recipe_sql, project)\n"
               f"select $1, $2, $3, $4, $5, $6, $7, $8, $9, {q(key)} from values\n{values(metric_rows)};")
    start, end = f"## PROJECT = '{key}'", f"<!-- end of PROJECT = '{key}' -->"
    section = routing + "\n" + end
    # Replace exactly this project's section (between its start and end markers) if it
    # exists, otherwise append it. Other projects' sections are never touched.
    sql.append(
        f"update {agents}.ROOT\n"
        f"set content = case\n"
        f"    when contains(content, {q(start)}) and contains(content, {q(end)})\n"
        f"        then split_part(content, {q(start)}, 1) || {q(section)} || split_part(content, {q(end)}, 2)\n"
        f"    else rtrim(content) || '\\n\\n' || {q(section)}\n"
        f"end\n"
        f"where provider = 'dbt' and key = 'routing';")
    # List the project in the routing header ("Filter every read of them by PROJECT first (...)").
    sql.append(
        f"update {agents}.ROOT\n"
        f"set content = regexp_replace(content, 'by PROJECT first \\\\(', 'by PROJECT first ({key}, ', 1, 1)\n"
        f"where provider = 'dbt' and key = 'routing'\n"
        f"  and not contains(content, 'by PROJECT first ({key}');")
    sql.append("commit;")

    out = pathlib.Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text("\n\n".join(sql) + "\n")
    print(f"wrote {out}: {len(models)} models, {len(columns)} columns, {len(deps)} edges, {len(metric_rows)} metrics")


if __name__ == "__main__":
    main()
