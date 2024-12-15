import uuid
from typing import Dict, List

import duckdb
import pyarrow as pa

SCHEMA_DEFINITION = [
    ("id", "INTEGER"),
    ("source_system", "VARCHAR"),
    ("first_name", "VARCHAR"),
    ("middle_names", "VARCHAR"),
    ("last_name", "VARCHAR"),
    ("id_3", "VARCHAR"),
    ("id_4", "VARCHAR"),
    ("date_of_birth", "DATE"),
    ("sex", "VARCHAR"),
    ("ethnicity", "VARCHAR"),
    ("first_name_alias_arr", "TEXT[]"),
    ("last_name_alias_arr", "TEXT[]"),
    ("date_of_birth_alias_arr", "DATE[]"),
    ("postcode_arr", "TEXT[]"),
    ("id1_arr", "TEXT[]"),
    ("id2_arr", "TEXT[]"),
    ("date_arr", "DATE[]"),
]


# Pre-compute SQL strings from SCHEMA_DEFINITION
SCHEMA_SQL = ",\n".join(f"{col} {type_}" for col, type_ in SCHEMA_DEFINITION)
CREATE_TABLE_SQL = f"CREATE TABLE {{}} ({SCHEMA_SQL})"

PLACEHOLDERS = ",".join(
    "?" + (f"::{t}" if t.endswith("[]") else "") for _, t in SCHEMA_DEFINITION
)
TWO_VALUE_GROUPS = f"({PLACEHOLDERS}),({PLACEHOLDERS})"


def insert_records_individually(con: duckdb.DuckDBPyConnection, records: List[Dict]):
    myuuid = str(uuid.uuid4())[:8]
    table_name = f"df_raw_{myuuid}"

    con.execute(CREATE_TABLE_SQL.format(table_name))

    query = f"INSERT INTO {table_name} VALUES {TWO_VALUE_GROUPS}"

    values = [record[c] for record in records for c, _ in SCHEMA_DEFINITION]
    con.execute(query, values)

    con.execute(f"CREATE TABLE r1_{myuuid} AS SELECT * FROM {table_name} LIMIT 1")
    con.execute(
        f"CREATE TABLE r2_{myuuid} AS SELECT * FROM {table_name} LIMIT 1 OFFSET 1"
    )

    return f"r1_{myuuid}", f"r2_{myuuid}"


def insert_records_arrow(con: duckdb.DuckDBPyConnection, records: List[Dict]):
    myuuid = str(uuid.uuid4())[:8]
    t1 = pa.Table.from_pylist([records[0]], schema=ARROW_SCHEMA)
    t2 = pa.Table.from_pylist([records[1]], schema=ARROW_SCHEMA)

    con.execute(f"CREATE TABLE r1_{myuuid} AS SELECT * FROM t1")
    con.execute(f"CREATE TABLE r2_{myuuid} AS SELECT * FROM t2")

    return f"r1_{myuuid}", f"r2_{myuuid}"


TEST_RECORDS = [
    {
        "id": 1200000,
        "source_system": "S1",
        "first_name": "JOHN",
        "middle_names": "JAMES",
        "last_name": "SMITH",
        "id_3": None,
        "id_4": "A1111AA",
        "date_of_birth": "1955-05-05",
        "sex": None,
        "ethnicity": "W",
        "first_name_alias_arr": ["JONNY", "JOHN"],
        "last_name_alias_arr": ["SMYTH", "SMYTH"],
        "date_of_birth_alias_arr": ["1955-03-28", "1955-03-28"],
        "postcode_arr": ["AB1 2CD", "XY1 9YZ"],
        "id1_arr": ["A12345699"],
        "id2_arr": None,
        "date_arr": ["2000-12-17", "2001-12-17"],
    },
    {
        "id": 1300000,
        "source_system": "S2",
        "first_name": "JANE",
        "middle_names": "ELIZABETH",
        "last_name": "DOE",
        "id_3": "X123456",
        "id_4": None,
        "date_of_birth": "1980-01-15",
        "sex": "F",
        "ethnicity": "A",
        "first_name_alias_arr": None,
        "last_name_alias_arr": None,
        "date_of_birth_alias_arr": None,
        "postcode_arr": ["EF3 4GH"],
        "id1_arr": ["B54321788"],
        "id2_arr": ["0123456A"],
        "date_arr": None,
    },
]

ARROW_SCHEMA = pa.schema(
    [
        pa.field("id", pa.int64(), nullable=True),
        pa.field("source_system", pa.string(), nullable=True),
        pa.field("first_name", pa.string(), nullable=True),
        pa.field("middle_names", pa.string(), nullable=True),
        pa.field("last_name", pa.string(), nullable=True),
        pa.field("id_3", pa.string(), nullable=True),
        pa.field("id_4", pa.string(), nullable=True),
        pa.field("date_of_birth", pa.string(), nullable=True),
        pa.field("sex", pa.string(), nullable=True),
        pa.field("ethnicity", pa.string(), nullable=True),
        pa.field("first_name_alias_arr", pa.list_(pa.string()), nullable=True),
        pa.field("last_name_alias_arr", pa.list_(pa.string()), nullable=True),
        pa.field("date_of_birth_alias_arr", pa.list_(pa.string()), nullable=True),
        pa.field("postcode_arr", pa.list_(pa.string()), nullable=True),
        pa.field("id1_arr", pa.list_(pa.string()), nullable=True),
        pa.field("id2_arr", pa.list_(pa.string()), nullable=True),
        pa.field("date_arr", pa.list_(pa.string()), nullable=True),
    ]
)
