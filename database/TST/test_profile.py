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
) -> tuple[Exception | None, str]:
    if id_profile_pk == default:
        id_profile_pk = str(uuid4())
    if email == default:
        email = f"email@email.domain{date_time()}"
    return insert(
        cursor=cursor,
        table_name="profile",
        column_data={
            "id_profile_pk": id_profile_pk,
            "email": email,
            "password": password,
            "created_at": created_at,
        },
    )


def select_profile(
    cursor: Cursor,
    show_columns: list[str] = ["*"],
    id_profile_pk: str | None = None,
    email: str | None = None,
) -> tuple[str | int | None, ...]:
    return select(
        cursor=cursor,
        table_name="profile",
        show_columns=show_columns,
        select_conditions={
            "id_profile_pk": id_profile_pk,
            "email": email,
        },
    )


# -----> STRUCTURE : COLUMNS


def test_id_profile_pk_column_structure(cursor: Cursor) -> None:
    verify_structure(
        expected_table_name="profile",
        expected_column_name="id_profile_pk",
        expected_data_type="uuid",
        expected_character_maximum_length=None,
        expected_column_default="uuid_generate_v4()",
        expected_is_nullable=False,
        expected_constraints={ConstraintType.PRIMARY_KEY: None},
        expected_is_index=False,
        cursor=cursor,
    )


def test_email_column_structure(cursor: Cursor) -> None:
    verify_structure(
        expected_table_name="profile",
        expected_column_name="email",
        expected_data_type="character varying",
        expected_character_maximum_length=262,
        expected_column_default=None,
        expected_is_nullable=False,
        expected_constraints={ConstraintType.UNIQUE: None},
        expected_is_index=True,
        cursor=cursor,
    )


def test_password_column_structure(cursor: Cursor) -> None:
    verify_structure(
        expected_table_name="profile",
        expected_column_name="password",
        expected_data_type="character",
        expected_character_maximum_length=60,
        expected_column_default=None,
        expected_is_nullable=False,
        expected_constraints={
            ConstraintType.CHECK: "((length(password) = 60))",
        },
        expected_is_index=False,
        cursor=cursor,
    )


def test_created_at_column_structure(cursor: Cursor) -> None:
    verify_structure(
        expected_table_name="profile",
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


def test_insert_null_value_in_id_profile_pk_column(cursor: Cursor) -> None:
    exception, message = insert_profile(cursor, id_profile_pk="null")
    assert (
        isinstance(exception, NotNullViolation)
        and '"id_profile_pk"' in message
        and '"profile"' in message
    ), message


def test_insert_null_value_in_email_column(cursor: Cursor) -> None:
    exception, message = insert_profile(cursor, email="null")
    assert (
        isinstance(exception, NotNullViolation)
        and '"email"' in message
        and '"profile"' in message
    ), message


def test_insert_null_value_in_password_column(cursor: Cursor) -> None:
    exception, message = insert_profile(cursor, password="null")
    assert (
        isinstance(exception, NotNullViolation)
        and '"password"' in message
        and '"profile"' in message
    ), message


def test_insert_null_value_in_created_at_column(cursor: Cursor) -> None:
    exception, message = insert_profile(cursor, created_at="null")
    assert (
        isinstance(exception, NotNullViolation)
        and '"created_at"' in message
        and '"profile"' in message
    ), message


# -----> BEHAVIOR : DEFAULT


def test_insert_default_value_in_id_profile_pk_column(cursor: Cursor) -> None:
    exception, message = insert_profile(cursor, email="somebody@gmail.com")
    assert exception is None and message == "", message
    id_profile_pk, email = select_profile(
        cursor,
        show_columns=["id_profile_pk", "email"],
        email="somebody@gmail.com",
    )
    assert id_profile_pk is not None and email == "somebody@gmail.com"


def test_insert_default_value_in_created_at_column(cursor: Cursor) -> None:
    exception, message = insert_profile(cursor, email="somebody@gmail.com")
    assert exception is None and message == "", message
    created_at, email = select_profile(
        cursor,
        show_columns=["created_at", "email"],
        email="somebody@gmail.com",
    )
    assert created_at is not None and email == "somebody@gmail.com"


# -----> BEHAVIOR : LENGTH


def test_insert_hyper_length_value_in_email_column(cursor: Cursor) -> None:
    exception, message = insert_profile(cursor, email="e" * 263)
    assert (
        isinstance(exception, StringDataRightTruncation)
        and "character varying(262)" in message
    ), message


def test_insert_hyper_length_value_in_password_column(cursor: Cursor) -> None:
    exception, message = insert_profile(cursor, password="e" * 61)
    assert (
        isinstance(exception, StringDataRightTruncation) and "character(60)" in message
    ), message


# -----> BEHAVIOR : UNIQUE


def test_insert_unique_value_in_id_profile_pk_column(cursor: Cursor) -> None:
    id_profile_pk = "3ac4f1a5-75a8-4357-9312-d4a5d04d5000"
    exception, message = insert_profile(cursor, id_profile_pk=id_profile_pk)
    assert exception is None and message == "", message
    exception, message = insert_profile(cursor, id_profile_pk=id_profile_pk)
    assert (
        isinstance(exception, UniqueViolation)
        and f"(id_profile_pk)=({id_profile_pk})" in message
    ), message


def test_insert_unique_value_in_email_column(cursor: Cursor) -> None:
    email = "bona_parte@email.com"
    exception, message = insert_profile(cursor, email=email)
    assert exception is None and message == "", message
    exception, message = insert_profile(cursor, email=email)
    assert (
        isinstance(exception, UniqueViolation) and f"(email)=({email})" in message
    ), message


# -----> BEHAVIOR : CHECK


def test_insert_check_value_in_password_column(cursor: Cursor) -> None:
    exception, message = insert_profile(cursor, password=f"{'x'*59}")
    assert (
        isinstance(exception, CheckViolation)
        and '"profile_password_check"' in message
        and '"profile"' in message
    ), message