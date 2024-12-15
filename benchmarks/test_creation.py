import uuid
from typing import List, Dict

import duckdb
import pyarrow as pa

from benchmarks.utils import (
    ARROW_SCHEMA,
    PYDANTIC_TEST_RECORDS,
    PYDANTIC_TEST_RECORDS_AS_DICTS,
    TEST_RECORDS,
    insert_records_arrow,
    insert_records_individually,
    insert_records_pandas,
    insert_records_individually_non_parameterised,
)


def test_individual_inserts(benchmark):
    con = duckdb.connect()

    def run_insert(con=con, records=TEST_RECORDS):
        insert_records_individually(con, records)

    benchmark.pedantic(
        run_insert, rounds=30, iterations=3, warmup_rounds=1
    )


def test_arrow_inserts(benchmark):
    con = duckdb.connect()

    def run_insert(con=con, records=PYDANTIC_TEST_RECORDS_AS_DICTS):
        insert_records_arrow(con, records)

    benchmark.pedantic(
        run_insert, rounds=30, iterations=3, warmup_rounds=1
    )

# For reasons I don't understand, this is slower than non-parameterised
# inserts BUT ONLY WHEN PANDAS IS NOT INSTALLED see below

# --------------------------------------------------------------------------------------------- benchmark: 4 tests ---------------------------------------------------------------------------------------------
# Name (time in ms)                                 Min                Max               Mean            StdDev             Median               IQR            Outliers       OPS            Rounds  Iterations
# --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# test_individual_inserts_non_parameterised      3.1134 (1.0)       4.2868 (1.0)       3.4487 (1.0)      0.3063 (1.0)       3.4540 (1.0)      0.4546 (1.0)           9;0  289.9641 (1.0)          30           3
# test_pydantic_inserts                          3.2456 (1.04)      4.9326 (1.15)      3.7446 (1.09)     0.5463 (1.78)      3.4802 (1.01)     0.8278 (1.82)          6;0  267.0504 (0.92)         30           3
# test_arrow_inserts                             3.4550 (1.11)      5.7284 (1.34)      4.7720 (1.38)     0.6172 (2.01)      4.7714 (1.38)     0.8438 (1.86)          8;0  209.5545 (0.72)         30           3
# test_individual_inserts                       13.7426 (4.41)     48.2895 (11.26)    16.0728 (4.66)     6.3404 (20.70)    14.6794 (4.25)     0.8344 (1.84)          2;3   62.2169 (0.21)         30           3
# --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
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

    benchmark.pedantic(
        run_insert, rounds=30, iterations=3, warmup_rounds=1
    )


def test_pandas_inserts(benchmark):
    con = duckdb.connect()

    def run_insert(con=con, records=TEST_RECORDS):
        insert_records_pandas(con, records)

    benchmark.pedantic(
        run_insert, rounds=30, iterations=3, warmup_rounds=1
    )


def test_individual_inserts_non_parameterised(benchmark):
    con = duckdb.connect()

    def run_insert(con=con, records=TEST_RECORDS):
        insert_records_individually_non_parameterised(con, records)

    benchmark.pedantic(
        run_insert, rounds=30, iterations=3, warmup_rounds=1
    )
