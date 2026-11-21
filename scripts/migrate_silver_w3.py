"""One-off Workshop #3 migration of the Silver files written in Workshop #2.

1. Moves every datalake_silver/<topic>_YYYYMMDD_HHMMSS.parquet into datalake_silver/<topic>/
   so each topic folder has one schema and can be read with spark.read.parquet(folder).
2. Rewrites nanosecond timestamps as microseconds (Spark 3.5 cannot read TIMESTAMP(NANOS)).
   Values are at most second-precision, so nothing is lost; rows, columns and the
   lineage metadata are kept unchanged.
"""

from pathlib import Path
import re

import pyarrow as pa
import pyarrow.parquet as pq

SILVER = Path(__file__).resolve().parents[1] / "datalake_silver"
NAME = re.compile(r"^(?P<topic>.+)_\d{8}_\d{6}\.parquet$")


def to_us(table: pa.Table) -> pa.Table:
    fields = []
    for f in table.schema:
        if pa.types.is_timestamp(f.type) and f.type.unit == "ns":
            fields.append(pa.field(f.name, pa.timestamp("us", tz=f.type.tz), f.nullable, f.metadata))
        else:
            fields.append(f)
    return table.cast(pa.schema(fields, metadata=table.schema.metadata), safe=False)


def main() -> None:
    for path in sorted(SILVER.glob("*.parquet")):
        topic = NAME.match(path.name).group("topic")
        target = SILVER / topic / path.name
        target.parent.mkdir(exist_ok=True)
        table = to_us(pq.read_table(path))
        pq.write_table(table, target, compression="snappy", version="2.6")
        path.unlink()
        print(f"{path.name} -> {topic}/ ({table.num_rows} rows)")


if __name__ == "__main__":
    main()
