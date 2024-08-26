import datetime
import json
import re as regex
from enum import Enum
from typing import Any, Generator

import psycopg2
import pytest
from psycopg2.extensions import cursor as Cursor


class ConstraintType(Enum):
    PRIMARY_KEY = "PRIMARY KEY"
    FOREIGN_KEY = "FOREIGN KEY"
    UNIQUE = "UNIQUE"
    CHECK = "CHECK"


def _json_file(file_name: str) -> dict[str, dict[str, str]]:
    try:
        with open(file_name, "r", encoding="utf-8") as file:
            return json.load(file)
    except FileNotFoundError:
        pytest.fail(reason=f"Error: The file '{file_name}' was not found.")
    except json.JSONDecodeError:
        pytest.fail(
            reason=f"Error: The file '{file_name}' does not contain valid JSON."
        )
    except Exception as exception:
        pytest.fail(reason=f"An error occurred while loading the file: {exception}")


def _main_environment_data() -> dict[str, str]:
    return _json_file(file_name="util/data.env")["environment_main"]


def date_time() -> str:
    return regex.sub(r"\D", "_", str(datetime.datetime.now()))


def _test_environment_data() -> dict[str, str]:
    data = _json_file(file_name="util/data.env")["environment_test"]
    data["dbname"] = f"environment_test_{date_time()}"
    return data


@pytest.fixture(scope="function")
def cursor() -> Generator[Cursor, Any, None]:
    main_environment_data = _main_environment_data()
    test_environment_data = _test_environment_data()
    main_environment_connection = psycopg2.connect(
        dbname=main_environment_data["dbname"],
        user=main_environment_data["user"],
        password=main_environment_data["password"],
        host=main_environment_data["host"],
        port=main_environment_data["port"],
    )
    main_environment_connection.autocommit = True
    main_environment_cursor = main_environment_connection.cursor()
    main_environment_cursor.execute(
        f"SELECT 1 FROM pg_catalog.pg_database WHERE datname = '{test_environment_data['dbname']}';"
    )
    test_environment_exists = main_environment_cursor.fetchone()
    if test_environment_exists:
        main_environment_cursor.execute(
            f"DROP DATABASE {test_environment_data['dbname']};"
        )
    main_environment_cursor.execute(
        f"CREATE DATABASE {test_environment_data['dbname']};"
    )
    test_environment_connection = psycopg2.connect(
        dbname=test_environment_data["dbname"],
        user=test_environment_data["user"],
        password=test_environment_data["password"],
        host=test_environment_data["host"],
        port=test_environment_data["port"],
    )
    test_environment_connection.autocommit = True
    test_environment_cursor = test_environment_connection.cursor()
    try:
        with open("util/assembled.sql", "r") as file:
            test_environment_cursor.execute(file.read())
        yield test_environment_cursor
    except Exception as e:
        pytest.fail(f"An error occurred while setting environment test: {e}")
    finally:
        test_environment_cursor.close()
        test_environment_connection.close()
        main_environment_cursor.execute(
            f"DROP DATABASE {test_environment_data['dbname']};"
        )
        main_environment_cursor.close()
        main_environment_connection.close()


def verify_structure_column(
    expected_table_name: str,
    expected_column_name: str,
    expected_data_type: str,
    expected_character_maximum_length: int | None,
    expected_column_default: str | None,
    expected_is_nullable: bool,
    cursor: Cursor,
) -> None:
    cursor.execute(
        f"""
            SELECT data_type, character_maximum_length, column_default, is_nullable
            FROM information_schema.columns
            WHERE table_name = '{expected_table_name}'
            AND column_name = '{expected_column_name}';
        """
    )
    result = cursor.fetchone()
    assert (
        result is not None
    ), f"Column '{expected_column_name}' not found in '{expected_table_name}' table"
    (
        present_data_type,
        present_character_maximum_length,
        present_column_default,
        present_is_nullable,
    ) = tuple[str, int | None, str | None, str](result)
    assert (
        expected_data_type == present_data_type
    ), f"Expected '{expected_column_name}' column to be type '{expected_data_type}', but present type is '{present_data_type}'"
    assert (
        expected_character_maximum_length == present_character_maximum_length
    ), f"Expected '{expected_column_name}' column to be type '{expected_character_maximum_length}', but present type is '{present_character_maximum_length}'"
    assert (
        expected_column_default == present_column_default
    ), f"Expected '{expected_column_name}' column to be default value '{expected_column_default}', but present default value is '{present_column_default}'"
    assert (
        "YES" if expected_is_nullable else "NO" == present_is_nullable
    ), f"Expected '{expected_column_name}' column to be nullable value '{expected_is_nullable}', but present nullable value is '{present_is_nullable}'"


def verify_structure_constraint(
    expected_table_name: str,
    expected_column_name: str,
    expected_constraints: dict[ConstraintType, str | None] | None,
    cursor: Cursor,
) -> None:
    if expected_constraints is not None:
        suffixes = {
            ConstraintType.PRIMARY_KEY: "pkey",
            ConstraintType.FOREIGN_KEY: "fkey",
            ConstraintType.UNIQUE: "key",
            ConstraintType.CHECK: "check",
        }
        primary_key_present = ConstraintType.PRIMARY_KEY in expected_constraints
        foreign_key_present = ConstraintType.FOREIGN_KEY in expected_constraints
        for (
            expected_constraint_type,
            expected_constraint_content,
        ) in expected_constraints.items():
            if expected_constraint_type not in suffixes:
                raise NotImplementedError(
                    f"Constraint type '{expected_constraint_type}' is not implemented."
                )
            suffix = suffixes[expected_constraint_type]
            infix = (
                f"_{expected_column_name}"
                if expected_constraint_type is not ConstraintType.PRIMARY_KEY
                and expected_constraint_type is not ConstraintType.FOREIGN_KEY
                and not primary_key_present
                and not foreign_key_present
                else ""
            )
            expected_constraint_name = f"{expected_table_name}{infix}_{suffix}"
            cursor.execute(
                f"""
                    SELECT constraint_type 
                    FROM information_schema.table_constraints 
                    WHERE constraint_catalog = current_catalog 
                    AND constraint_name = '{expected_constraint_name}'
                    AND table_name = '{expected_table_name}';
                """
            )
            result = cursor.fetchone()
            assert (
                result is not None
            ), f"Constraint '{expected_constraint_name}' not found in '{expected_table_name}' table"
            (present_constraint_type,) = tuple[str, ...](result)
            assert (
                expected_constraint_type.value == present_constraint_type
            ), f"Expected '{expected_constraint_name}' constraint to be type '{expected_constraint_type.value}', but present type is '{present_constraint_type}'"
            if expected_constraint_type is ConstraintType.CHECK:
                cursor.execute(
                    f"""
                        SELECT check_clause
                        FROM information_schema.check_constraints
                        WHERE constraint_catalog = current_catalog
                        AND constraint_name = '{expected_constraint_name}'
                    """
                )
                result = cursor.fetchone()
                assert (
                    result is not None
                ), f"Check constraint '{expected_constraint_name}' not found in '{expected_table_name}' table"
                (present_constraint_content,) = tuple[str | None, ...](result)
                assert (
                    expected_constraint_content == present_constraint_content
                ), f"Expected '{expected_constraint_name}' check constraint to be content '{expected_constraint_content}', but present content is '{present_constraint_content}'"
            if expected_constraint_type is ConstraintType.FOREIGN_KEY:
                cursor.execute(
                    f"""
                        SELECT 
                            ccu.table_name AS table_origin,
                            ccu.column_name AS column_origin,
                            kcu.column_name AS column_target 
                        FROM 
                            information_schema.key_column_usage AS kcu
                        JOIN 
                            information_schema.constraint_column_usage AS ccu 
                        ON 
                            kcu.constraint_name = ccu.constraint_name
                        WHERE 
                            kcu.constraint_catalog = current_catalog
                            AND kcu.constraint_name = '{expected_constraint_name}';
                    """
                )
                result = cursor.fetchone()
                assert (
                    result is not None
                ), f"Check constraint '{expected_constraint_name}' not found in '{expected_table_name}' table"
                table_origin, column_origin, column_target = tuple[str | None, ...](
                    result
                )
                present_constraint_content = (
                    f"({column_target})=({column_origin} in {table_origin})"
                )
                assert (
                    present_constraint_content == expected_constraint_content
                ), f"Expected '{expected_constraint_name}' foreign key to be content '{expected_constraint_content}', but present content is '{present_constraint_content}'"


def verify_structure_index(
    expected_table_name: str,
    expected_column_name: str,
    expected_is_index: bool,
    cursor: Cursor,
) -> None:
    expected_index_name = f"{expected_table_name}_{expected_column_name}_index"
    cursor.execute(
        f"""
            SELECT indexdef
            FROM pg_indexes
            WHERE tablename = '{expected_table_name}'
            AND indexname = '{expected_index_name}'
        """
    )
    result = cursor.fetchone()
    if expected_is_index:
        assert (
            result is not None
        ), f"Index '{expected_index_name}' not found in column '{expected_column_name}' on '{expected_table_name}'"
        expected_index_content = rf"CREATE INDEX {expected_index_name} ON .*?{expected_table_name} USING btree \({expected_column_name}\)"
        (present_index_content,) = tuple[str, ...](result)
        assert regex.match(
            pattern=expected_index_content,
            string=present_index_content,
        ), f"Index '{expected_index_name}' not contains '{expected_index_content}'"
    else:
        assert (
            result is None
        ), f"Index '{expected_index_name}' found in column '{expected_column_name}' on '{expected_table_name}'"


def verify_structure(
    cursor: Cursor,
    expected_is_index: bool,
    expected_data_type: str,
    expected_table_name: str,
    expected_column_name: str,
    expected_is_nullable: bool,
    expected_column_default: str | None = None,
    expected_character_maximum_length: int | None = None,
    expected_constraints: dict[ConstraintType, str | None] | None = None,
) -> None:
    verify_structure_column(
        expected_table_name=expected_table_name,
        expected_column_name=expected_column_name,
        expected_data_type=expected_data_type,
        expected_character_maximum_length=expected_character_maximum_length,
        expected_column_default=expected_column_default,
        expected_is_nullable=expected_is_nullable,
        cursor=cursor,
    )
    verify_structure_constraint(
        expected_table_name=expected_table_name,
        expected_column_name=expected_column_name,
        expected_constraints=expected_constraints,
        cursor=cursor,
    )
    verify_structure_index(
        expected_table_name=expected_table_name,
        expected_column_name=expected_column_name,
        expected_is_index=expected_is_index,
        cursor=cursor,
    )


def insert(
    cursor: Cursor, table_name: str, column_data: dict[str, str | int | None]
) -> tuple[Exception | None, str]:
    filtered_data = {
        key: value for key, value in column_data.items() if value is not None
    }
    columns = ", ".join(filtered_data.keys())
    values = ", ".join(
        (
            f"{value}"
            if (value == "null") or (not isinstance(value, str))
            else f"'{value}'"
        )
        for value in filtered_data.values()
    )
    query = f"INSERT INTO {table_name} ({columns}) VALUES ({values});"
    try:
        cursor.execute(query)
        return (None, "")
    except Exception as exception:
        return (exception, str(exception))


def select(
    cursor: Cursor,
    table_name: str,
    show_columns: list[str],
    select_conditions: dict[str, str | int | None] | None,
) -> tuple[str | int | None, ...]:
    columns = ", ".join(show_columns)
    if select_conditions:
        filtered_conditions = {
            key: value for key, value in select_conditions.items() if value is not None
        }
        conditions = " AND ".join(
            (
                f"{key} = {value}"
                if (value == "null") or (not isinstance(value, str))
                else f"{key} = '{value}'"
            )
            for key, value in filtered_conditions.items()
        )
        query = f"SELECT {columns} FROM {table_name} WHERE {conditions};"
    else:
        query = f"SELECT {columns} FROM {table_name};"
    cursor.execute(query)
    result = cursor.fetchone()
    if result:
        return tuple[str | int, ...](result)
    else:
        return (None,)
