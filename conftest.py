# conftest.py
import json
from datetime import date
from pathlib import Path
from typing import List

import pytest


@pytest.fixture(scope="session")
def schema_definition():
    return [
        ("id", "INTEGER"),
        ("source_system", "VARCHAR"),
        ("first_name", "VARCHAR"),
        ("middle_names", "VARCHAR"),
        ("last_name", "VARCHAR"),
        ("crn", "VARCHAR"),
        ("prison_number", "VARCHAR"),
        ("date_of_birth", "DATE"),
        ("sex", "VARCHAR"),
        ("ethnicity", "VARCHAR"),
        ("first_name_alias_arr", "TEXT[]"),
        ("last_name_alias_arr", "TEXT[]"),
        ("date_of_birth_alias_arr", "DATE[]"),
        ("postcode_arr", "TEXT[]"),
        ("cro_arr", "TEXT[]"),
        ("pnc_arr", "TEXT[]"),
        ("sentence_date_arr", "DATE[]"),
    ]


@pytest.fixture(scope="session")
def create_table_sql(schema_definition):
    return (
        "CREATE TABLE IF NOT EXISTS df_raw (\n"
        + ",\n".join(f"    {c} {t}" for c, t in schema_definition)
        + "\n);"
    )


@pytest.fixture(scope="session")
def sample_records():
    current_dir = Path(__file__).parent
    with open(current_dir / "test_data" / "sample_records.json") as f:
        return json.load(f)
