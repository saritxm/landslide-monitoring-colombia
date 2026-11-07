"""Command-line entry point for the landslide monitoring ingestion system.

Workshop #1: stub only. Historical mode is implemented in Workshop #2,
incremental mode in Workshop #3.
"""

import argparse
import sys


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Landslide monitoring ingestion")
    parser.add_argument(
        "--mode",
        required=True,
        choices=["historical", "incremental", "status"],
        help="historical: bulk load; incremental: load since last watermark; "
        "status: show watermarks, last runs and row counts",
    )
    parser.add_argument("--start", help="Start date (YYYY-MM-DD), historical mode only")
    parser.add_argument("--end", help="End date (YYYY-MM-DD), historical mode only")
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    print(f"[stub] mode={args.mode} start={args.start} end={args.end}")
    print("Not implemented yet (planned for Workshops #2 and #3).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
