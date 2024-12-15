# test_performance.py
import duckdb
import pytest

from utils import (
    ARROW_SCHEMA,
    TEST_RECORDS,
    insert_records_arrow,
    insert_records_individually,
)


def test_individual_inserts(benchmark, create_table_sql):
    con = duckdb.connect()
    insert_records_individually(con, TEST_RECORDS)

    def run_insert(con=con, records=TEST_RECORDS):
        insert_records_individually(con, records)

    def setup():
        return (), {}  # Return empty args and kwargs

    benchmark.pedantic(
        run_insert, setup=setup, rounds=30, iterations=1, warmup_rounds=1
    )


def test_arrow_inserts(benchmark, create_table_sql):
    con = duckdb.connect()
    con.execute(create_table_sql)
    insert_records_arrow(con, TEST_RECORDS)

    def run_insert(con=con, records=TEST_RECORDS):
        insert_records_arrow(con, records)

    def setup():
        return (), {}

    benchmark.pedantic(
        run_insert, setup=setup, rounds=30, iterations=1, warmup_rounds=1
    )
