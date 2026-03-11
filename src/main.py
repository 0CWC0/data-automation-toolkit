from __future__ import annotations

import argparse
import json
from pathlib import Path

from data_processor import DataProcessor, FilterOptions
from file_handler import FileHandler


def parse_csv_list(value: str | None) -> list[str] | None:
    # 쉼표 구분 문자열을 list[str]로 정규화한다.
    if value is None:
        return None
    parsed = [item.strip() for item in value.split(",") if item.strip()]
    return parsed or None


def load_mapping(mapping_file: str | None) -> dict[str, str] | None:
    if not mapping_file:
        return None
    with open(mapping_file, "r", encoding="utf-8") as file:
        payload = json.load(file)
    if not isinstance(payload, dict):
        raise ValueError("Mapping file must contain a JSON object.")
    
    # JSON object(dict)를 {str: str} 형태로 강제 정규화.
    return {str(key): str(value) for key, value in payload.items()}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Merge and clean shopping-mall sales data (CSV/Excel)."
    )
    parser.add_argument(
        "--inputs",
        nargs="+",
        required=True,
        help="Input file paths (.csv, .xlsx, .xls)",
    )
    parser.add_argument(
        "--output",
        required=True,
        help="Output file path (.xlsx or .csv)",
    )
    parser.add_argument(
        "--mapping-file",
        help="JSON file for column mapping. Example: {'주문번호':'order_id'}",
    )
    parser.add_argument(
        "--dedup-cols",
        help="Comma-separated columns used as duplicate key.",
    )
    parser.add_argument(
        "--dedup-keep",
        choices=["first", "last", "False"],
        default="first",
        help="Which duplicate to keep (False means remove all duplicate groups).",
    )
    parser.add_argument(
        "--sort-by",
        help="Comma-separated sort columns.",
    )
    parser.add_argument(
        "--descending",
        action="store_true",
        help="Sort in descending order.",
    )
    parser.add_argument(
        "--status-column",
        default="status",
        help="Status column name for include/exclude filtering.",
    )
    parser.add_argument(
        "--include-statuses",
        help="Comma-separated statuses to include only.",
    )
    parser.add_argument(
        "--exclude-statuses",
        help="Comma-separated statuses to exclude.",
    )
    parser.add_argument(
        "--date-column",
        default="order_date",
        help="Date column for period filtering.",
    )
    parser.add_argument(
        "--start-date",
        help="Start date (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--end-date",
        help="End date (YYYY-MM-DD).",
    )
    return parser


def run_pipeline(args: argparse.Namespace) -> Path:
    # data_frames(list[pd.DataFrame]): 다중 입력 파일을 일괄 적재.
    data_frames = [FileHandler.read_table(file_path) for file_path in args.inputs]

    merged = DataProcessor.merge_data(data_frames)
    mapping = load_mapping(args.mapping_file)
    mapped = DataProcessor.apply_column_mapping(merged, mapping)
    standardized = DataProcessor.ensure_columns(mapped)

    dedup_cols = parse_csv_list(args.dedup_cols)
    # args는 argparse.Namespace 자료구조이며, CLI 인자를 속성으로 보관한다.
    dedup_keep: str | bool = False if args.dedup_keep == "False" else args.dedup_keep
    deduplicated = DataProcessor.remove_duplicates(
        standardized,
        subset=dedup_cols,
        keep=dedup_keep,
    )

    filtered = DataProcessor.filter_data(
        deduplicated,
        FilterOptions(
            status_column=args.status_column,
            date_column=args.date_column,
            include_statuses=parse_csv_list(args.include_statuses),
            exclude_statuses=parse_csv_list(args.exclude_statuses),
            start_date=args.start_date,
            end_date=args.end_date,
        ),
    )

    sorted_data = DataProcessor.sort_data(
        filtered,
        by=parse_csv_list(args.sort_by),
        ascending=not args.descending,
    )

    output_path = Path(args.output)
    FileHandler.write_table(sorted_data, str(output_path))
    return output_path


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    output_path = run_pipeline(args)
    print(f"Completed: {output_path}")


if __name__ == "__main__":
    main()
