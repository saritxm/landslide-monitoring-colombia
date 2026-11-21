"""Export the DAG runs and task instances of the Airflow metadata DB to JSON.

The report is built from this file, so the run evidence is versioned with the data.
Usage: python workshop_2/report/export_airflow_runs.py <path to airflow.db>
"""

import json
import sqlite3
import sys
from pathlib import Path

OUT = Path(__file__).with_name("airflow_runs.json")


def main(db_path: str) -> None:
    con = sqlite3.connect(db_path)
    runs = [dict(zip(("dag_id", "run_id", "run_type", "state", "start_date", "end_date"), r)) for r in con.execute(
        "select dag_id, run_id, run_type, state, start_date, end_date from dag_run order by start_date")]
    for run in runs:
        run["tasks"] = [dict(zip(("task_id", "state", "try_number", "duration_s"), r)) for r in con.execute(
            "select task_id, state, try_number, duration from task_instance where dag_id=? and run_id=? order by start_date",
            (run["dag_id"], run["run_id"]))]
    OUT.write_text(json.dumps(runs, indent=2), encoding="utf-8")
    print(f"exported {len(runs)} runs to {OUT}")


if __name__ == "__main__":
    main(sys.argv[1])
