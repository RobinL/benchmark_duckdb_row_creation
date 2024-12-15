import uuid
from typing import List

import duckdb
import pyarrow as pa

from benchmarks.utils import (
    ARROW_SCHEMA,
    PYDANTIC_TEST_RECORDS,
    PYDANTIC_TEST_RECORDS_AS_DICTS,
    TEST_RECORDS,
    insert_records_arrow,
    insert_records_individually,
)


def test_individual_inserts(benchmark):
    con = duckdb.connect()

    def run_insert(con=con, records=TEST_RECORDS):
        insert_records_individually(con, records)

    def setup():
        return (), {}  # Return empty args and kwargs

    benchmark.pedantic(
        run_insert, setup=setup, rounds=30, iterations=1, warmup_rounds=1
    )


def test_arrow_inserts(benchmark):
    con = duckdb.connect()

    def run_insert(con=con, records=PYDANTIC_TEST_RECORDS_AS_DICTS):
        insert_records_arrow(con, records)

    def setup():
        return (), {}

    benchmark.pedantic(
        run_insert, setup=setup, rounds=30, iterations=1, warmup_rounds=1
    )


def test_pydantic_inserts(benchmark):
    con = duckdb.connect()

    def run_insert(con=con, records=PYDANTIC_TEST_RECORDS):
        myuuid = str(uuid.uuid4())[:8]
        dict_records = [record.model_dump() for record in records]

        t1 = pa.Table.from_pylist([dict_records[0]], schema=ARROW_SCHEMA)
        t2 = pa.Table.from_pylist([dict_records[1]], schema=ARROW_SCHEMA)

        con.execute(f"CREATE TABLE r1_{myuuid} AS SELECT * FROM t1")
        con.execute(f"CREATE TABLE r2_{myuuid} AS SELECT * FROM t2")

        return f"r1_{myuuid}", f"r2_{myuuid}"

    def setup():
        return (), {}

    benchmark.pedantic(
        run_insert, setup=setup, rounds=30, iterations=1, warmup_rounds=1
    )
