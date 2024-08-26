from uuid import uuid4

from util.development_test_tool import *
from psycopg2.errors import (
    NotNullViolation,
    StringDataRightTruncation,
    UniqueViolation,
    CheckViolation,
)

# SETTING

default = "~#<?default?>#~"


def insert_profile(
    cursor: Cursor,
    id_profile_pk: str = default,
    email: str = default,
    password: str = f"{'*' * 60}",
    created_at: str | None = None,
) -> str:
    if id_profile_pk == default:
        id_profile_pk = str(uuid4())
    if email == default:
        email = f"email@email.domain{date_time()}"
    insert(
        cursor=cursor,
        table_name="profile",
        column_data={
            "id_profile_pk": id_profile_pk,
            "email": email,
            "password": password,
            "created_at": created_at,
        },
    )
    return id_profile_pk


def insert_writer(
    cursor: Cursor,
    id_profile_pfk: str = default,
    name: str = default,
    label: str = "Writer Label",
    description: str = "Description Text",
    birthday: str = "1990-02-24",
) -> str:
    if id_profile_pfk == default:
        id_profile_pfk = insert_profile(cursor)
    if name == default:
        name = f"w_n{date_time()}"
    insert(
        cursor=cursor,
        table_name="writer",
        column_data={
            "id_profile_pfk": id_profile_pfk,
            "name": name,
            "label": label,
            "description": description,
            "birthday": birthday,
        },
    )
    return id_profile_pfk


def insert_pending_writer(
    cursor: Cursor,
    id_writer_pfk: str = default,
    activation_key: str = f"{'*' * 192}",
    created_at: str | None = None,
) -> tuple[Exception | None, str]:
    if id_writer_pfk == default:
        id_writer_pfk = insert_writer(cursor)
    return insert(
        cursor=cursor,
        table_name="pending_writer",
        column_data={
            "id_writer_pfk": id_writer_pfk,
            "activation_key": activation_key,
            "created_at": created_at,
        },
    )


# -----> STRUCTURE : COLUMNS


def test_id_writer_pfk_column_structure(cursor: Cursor) -> None:
    verify_structure(
        expected_table_name="pending_writer",
        expected_column_name="id_writer_pfk",
        expected_data_type="uuid",
        expected_character_maximum_length=None,
        expected_column_default=None,
        expected_is_nullable=False,
        expected_constraints={
            ConstraintType.PRIMARY_KEY: None,
            ConstraintType.FOREIGN_KEY: "(id_writer_pfk)=(id_profile_pfk in writer)",
        },
        expected_is_index=False,
        cursor=cursor,
    )


def test_activation_key_column_structure(cursor: Cursor) -> None:
    verify_structure(
        expected_table_name="pending_writer",
        expected_column_name="activation_key",
        expected_data_type="character",
        expected_character_maximum_length=192,
        expected_column_default=None,
        expected_is_nullable=False,
        expected_constraints={ConstraintType.CHECK: "((length(activation_key) = 192))"},
        expected_is_index=False,
        cursor=cursor,
    )


def test_created_at_column_structure(cursor: Cursor) -> None:
    verify_structure(
        expected_table_name="pending_writer",
        expected_column_name="created_at",
        expected_data_type="timestamp with time zone",
        expected_character_maximum_length=None,
        expected_column_default="CURRENT_TIMESTAMP",
        expected_is_nullable=False,
        expected_constraints=None,
        expected_is_index=False,
        cursor=cursor,
    )


# -----> BEHAVIOR : NULL


def test_insert_null_value_in_id_writer_pfk_column(cursor: Cursor) -> None:
    exception, message = insert_pending_writer(cursor, id_writer_pfk="null")
    assert (
        isinstance(exception, NotNullViolation)
        and '"id_writer_pfk"' in message
        and '"pending_writer"' in message
    ), message


def test_insert_null_value_in_activation_key_column(cursor: Cursor) -> None:
    exception, message = insert_pending_writer(cursor, activation_key="null")
    assert (
        isinstance(exception, NotNullViolation)
        and '"activation_key"' in message
        and '"pending_writer"' in message
    ), message


def test_insert_null_value_in_created_at_column(cursor: Cursor) -> None:
    exception, message = insert_pending_writer(cursor, created_at="null")
    assert (
        isinstance(exception, NotNullViolation)
        and '"created_at"' in message
        and '"pending_writer"' in message
    ), message


# -----> BEHAVIOR : LENGTH


def test_insert_hyper_length_value_in_activation_key_column(cursor: Cursor) -> None:
    exception, message = insert_pending_writer(cursor, activation_key="e" * 193)
    assert (
        isinstance(exception, StringDataRightTruncation)
        and "character(192)" in message
    ), message


# -----> BEHAVIOR : UNIQUE


def test_insert_unique_value_in_id_writer_pfk_column(cursor: Cursor) -> None:
    id_profile_pfk = insert_writer(cursor)
    exception, message = insert_pending_writer(cursor, id_writer_pfk=id_profile_pfk)
    assert exception is None and message == "", message
    exception, message = insert_pending_writer(cursor, id_writer_pfk=id_profile_pfk)
    assert (
        isinstance(exception, UniqueViolation)
        and f"(id_writer_pfk)=({id_profile_pfk})" in message
    ), message


# -----> BEHAVIOR : CHECK


def test_insert_check_value_in_activation_key_column(cursor: Cursor) -> None:
    exception, message = insert_pending_writer(cursor, activation_key=f"{'x'*191}")
    assert (
        isinstance(exception, CheckViolation)
        and '"pending_writer_activation_key_check"' in message
        and '"pending_writer"' in message
    ), message
