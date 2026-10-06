#!/usr/bin/env python3
"""Extract Glue catalog table definitions for a database into Athena-friendly DDL SQL."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Optional, Sequence

import boto3
from botocore.exceptions import ClientError

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from mimic_lakehouse import config  # noqa: E402


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Extract Glue table definitions from a database into SQL DDL files.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    config.add_dataset_arg(parser)
    parser.add_argument(
        "--database",
        default=None,
        help="Glue database name to export (default: the raw DB for --dataset)",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Output SQL file path (default: ddl/raw_export/<database>_tables.sql)",
    )
    parser.add_argument("--region", default=None, help="AWS region override")
    parser.add_argument("--profile", default=None, help="AWS CLI profile to use")
    return parser


def resolve_defaults(args: argparse.Namespace) -> None:
    p = config.dataset_profile(getattr(args, "dataset", None))
    args.database = args.database or p.raw_database
    if not args.output:
        args.output = str(Path(__file__).resolve().parent / "raw_export" / f"{args.database}_tables.sql")


def quote_identifier(name: str) -> str:
    return f"`{name.replace('`', '``')}`"


def normalize_type(type_name: Optional[str]) -> str:
    if not type_name:
        return "string"
    return type_name.strip()


def build_table_ddl(database_name: str, table: dict) -> str:
    table_name = table["Name"]
    storage_descriptor = table.get("StorageDescriptor", {})
    columns = storage_descriptor.get("Columns", [])
    partition_keys = table.get("PartitionKeys", [])

    all_columns = columns + partition_keys
    if not all_columns:
        column_lines = ["  " + quote_identifier("dummy") + " string"]
    else:
        column_lines = [
            f"  {quote_identifier(column['Name'])} {normalize_type(column.get('Type'))}"
            for column in all_columns
        ]

    lines = [f"CREATE EXTERNAL TABLE IF NOT EXISTS {quote_identifier(database_name)}.{quote_identifier(table_name)} ("]
    lines.append(",\n".join(column_lines))
    lines.append(")")

    location = storage_descriptor.get("Location")
    if location:
        lines.append(f"LOCATION '{location}'")

    serde_info = storage_descriptor.get("SerdeInfo", {})
    if serde_info.get("SerializationLibrary"):
        lines.append(f"ROW FORMAT SERDE '{serde_info['SerializationLibrary']}'")
        serde_parameters = serde_info.get("Parameters", {}) or {}
        if serde_parameters:
            props = ", ".join(
                f"'{key}'='{value}'" for key, value in serde_parameters.items()
            )
            lines.append(f"WITH SERDEPROPERTIES ({props})")
    else:
        lines.append("ROW FORMAT DELIMITED")

    stored_as = ""
    if storage_descriptor.get("InputFormat") and storage_descriptor.get("OutputFormat"):
        stored_as = (
            f"STORED AS INPUTFORMAT '{storage_descriptor['InputFormat']}' "
            f"OUTPUTFORMAT '{storage_descriptor['OutputFormat']}'"
        )
    elif storage_descriptor.get("Location"):
        stored_as = "STORED AS TEXTFILE"

    if stored_as:
        lines.append(stored_as)

    if location:
        lines.append(f"LOCATION '{location}'")

    return "\n".join(lines) + ";\n\n"


def extract_ddl(glue, database_name: str) -> list[str]:
    paginator = glue.get_paginator("get_tables")
    table_names = []
    ddl_statements = []

    for page in paginator.paginate(DatabaseName=database_name):
        for table in page.get("TableList", []):
            table_name = table["Name"]
            table_details = glue.get_table(DatabaseName=database_name, Name=table_name)
            ddl_statements.append(build_table_ddl(database_name, table_details["Table"]))
            table_names.append(table_name)

    return ddl_statements


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    resolve_defaults(args)

    session = boto3.Session(region_name=args.region, profile_name=args.profile) if args.profile or args.region else boto3.Session()
    glue = session.client("glue")

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        ddl_statements = extract_ddl(glue, args.database)
    except ClientError as exc:
        print(f"ERROR: unable to read Glue database '{args.database}': {exc}", file=sys.stderr)
        return 1

    if not ddl_statements:
        print(f"No tables found in Glue database '{args.database}'.")
        output_path.write_text("-- No tables found.\n", encoding="utf-8")
        return 0

    output_path.write_text("\n".join(ddl_statements), encoding="utf-8")
    print(f"Wrote {len(ddl_statements)} table DDL statements to {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
