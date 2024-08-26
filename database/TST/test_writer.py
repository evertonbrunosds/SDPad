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
) -> tuple[Exception | None, str]:
    if id_profile_pfk == default:
        id_profile_pfk = insert_profile(cursor)
    if name == default:
        name = f"w_n{date_time()}"
    return insert(
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


def select_writer(
    cursor: Cursor,
    show_columns: list[str] = ["*"],
    id_profile_pfk: str | None = None,
    name: str | None = None,
) -> tuple[str | int | None, ...]:
    return select(
        cursor=cursor,
        table_name="writer",
        show_columns=show_columns,
        select_conditions={
            "id_profile_pfk": id_profile_pfk,
            "name": name,
        },
    )


# -----> STRUCTURE : COLUMNS


def test_id_profile_pfk_column_structure(cursor: Cursor) -> None:
    verify_structure(
        expected_table_name="writer",
        expected_column_name="id_profile_pfk",
        expected_data_type="uuid",
        expected_character_maximum_length=None,
        expected_column_default=None,
        expected_is_nullable=False,
        expected_constraints={
            ConstraintType.PRIMARY_KEY: None,
            ConstraintType.FOREIGN_KEY: '(id_profile_pfk)=(id_profile_pk in profile)',
        },
        expected_is_index=False,
        cursor=cursor,
    )


def test_name_column_structure(cursor: Cursor) -> None:
    verify_structure(
        expected_table_name="writer",
        expected_column_name="name",
        expected_data_type="character varying",
        expected_character_maximum_length=32,
        expected_column_default=None,
        expected_is_nullable=False,
        expected_constraints={
            ConstraintType.UNIQUE: None,
            ConstraintType.CHECK: "(((name)::text ~ '^(?!_)(?!.*__)[a-z0-9_]+(?<!_)$'::text))",
        },
        expected_is_index=True,
        cursor=cursor,
    )


def test_label_column_structure(cursor: Cursor) -> None:
    verify_structure(
        expected_table_name="writer",
        expected_column_name="label",
        expected_data_type="character varying",
        expected_character_maximum_length=32,
        expected_column_default=None,
        expected_is_nullable=False,
        expected_constraints={
            ConstraintType.CHECK: "(((label)::text ~ '^(?! )(?!.*  )[a-zA-Z0-9À-ÿ ]+(?<! )$'::text))"
        },
        expected_is_index=False,
        cursor=cursor,
    )


def test_description_column_structure(cursor: Cursor) -> None:
    verify_structure(
        expected_table_name="writer",
        expected_column_name="description",
        expected_data_type="character varying",
        expected_character_maximum_length=1024,
        expected_column_default=None,
        expected_is_nullable=True,
        expected_constraints=None,
        expected_is_index=False,
        cursor=cursor,
    )


def test_birthday_column_structure(cursor: Cursor) -> None:
    verify_structure(
        expected_table_name="writer",
        expected_column_name="birthday",
        expected_data_type="date",
        expected_character_maximum_length=None,
        expected_column_default=None,
        expected_is_nullable=False,
        expected_constraints={
            ConstraintType.CHECK: "((EXTRACT(year FROM age((CURRENT_DATE)::timestamp with time zone, (birthday)::timestamp with time zone)) >= (18)::numeric))",
        },
        expected_is_index=False,
        cursor=cursor,
    )


# -----> BEHAVIOR : NULL


def test_insert_null_value_in_id_profile_pfk_column(cursor: Cursor) -> None:
    exception, message = insert_writer(cursor, id_profile_pfk="null")
    assert (
        isinstance(exception, NotNullViolation)
        and '"id_profile_pfk"' in message
        and '"writer"' in message
    ), message


def test_insert_null_value_in_name_column(cursor: Cursor) -> None:
    exception, message = insert_writer(cursor, name="null")
    assert (
        isinstance(exception, NotNullViolation)
        and '"name"' in message
        and '"writer"' in message
    ), message


def test_insert_null_value_in_label_column(cursor: Cursor) -> None:
    exception, message = insert_writer(cursor, label="null")
    assert (
        isinstance(exception, NotNullViolation)
        and '"label"' in message
        and '"writer"' in message
    ), message


def test_insert_null_value_in_birthday_column(cursor: Cursor) -> None:
    exception, message = insert_writer(cursor, birthday="null")
    assert (
        isinstance(exception, NotNullViolation)
        and '"birthday"' in message
        and '"writer"' in message
    ), message


# -----> BEHAVIOR : LENGTH


def test_insert_hyper_length_value_in_name_column(cursor: Cursor) -> None:
    exception, message = insert_writer(cursor, name="e" * 33)
    assert (
        isinstance(exception, StringDataRightTruncation)
        and "character varying(32)" in message
    ), message


def test_insert_hyper_length_value_in_label_column(cursor: Cursor) -> None:
    exception, message = insert_writer(cursor, label="e" * 33)
    assert (
        isinstance(exception, StringDataRightTruncation)
        and "character varying(32)" in message
    ), message


def test_insert_hyper_length_value_in_description_column(cursor: Cursor) -> None:
    exception, message = insert_writer(cursor, description="e" * 1025)
    assert (
        isinstance(exception, StringDataRightTruncation)
        and "character varying(1024)" in message
    ), message


# -----> BEHAVIOR : UNIQUE


def test_insert_unique_value_in_id_profile_pfk_column(cursor: Cursor) -> None:
    id_profile_pfk = insert_profile(cursor)
    exception, message = insert_writer(cursor, id_profile_pfk=id_profile_pfk)
    assert exception is None and message == "", message
    exception, message = insert_writer(cursor, id_profile_pfk=id_profile_pfk)
    assert (
        isinstance(exception, UniqueViolation)
        and f"(id_profile_pfk)=({id_profile_pfk})" in message
    ), message


def test_insert_unique_value_in_name_column(cursor: Cursor) -> None:
    exception, message = insert_writer(cursor, name="bona_parte_0")
    assert exception is None and message == "", message
    exception, message = insert_writer(cursor, name="bona_parte_0")
    assert (
        isinstance(exception, UniqueViolation) and "(name)=(bona_parte_0)" in message
    ), message


# -----> BEHAVIOR : CHECK


def test_insert_check_value_in_name_column(cursor: Cursor) -> None:
    exception, message = insert_writer(cursor, name="BonaParte")
    assert (
        isinstance(exception, CheckViolation)
        and '"writer_name_check"' in message
        and '"writer"' in message
    )
    exception, message = insert_writer(cursor, name="_bona")
    assert (
        isinstance(exception, CheckViolation)
        and '"writer_name_check"' in message
        and '"writer"' in message
    )
    exception, message = insert_writer(cursor, name="bona_")
    assert (
        isinstance(exception, CheckViolation)
        and '"writer_name_check"' in message
        and '"writer"' in message
    )
    exception, message = insert_writer(cursor, name="bona__parte")
    assert (
        isinstance(exception, CheckViolation)
        and '"writer_name_check"' in message
        and '"writer"' in message
    )
    exception, message = insert_writer(cursor, name="")
    assert (
        isinstance(exception, CheckViolation)
        and '"writer_name_check"' in message
        and '"writer"' in message
    )
    exception, message = insert_writer(cursor, name="napoleão")
    assert (
        isinstance(exception, CheckViolation)
        and '"writer_name_check"' in message
        and '"writer"' in message
    )


def test_insert_check_value_in_label_column(cursor: Cursor) -> None:
    exception, message = insert_writer(cursor, label=" Bona")
    assert (
        isinstance(exception, CheckViolation)
        and '"writer_label_check"' in message
        and '"writer"' in message
    )
    exception, message = insert_writer(cursor, label="Bona ")
    assert (
        isinstance(exception, CheckViolation)
        and '"writer_label_check"' in message
        and '"writer"' in message
    )
    exception, message = insert_writer(cursor, label="Bona  Parte")
    assert (
        isinstance(exception, CheckViolation)
        and '"writer_label_check"' in message
        and '"writer"' in message
    )
    exception, message = insert_writer(cursor, label="")
    assert (
        isinstance(exception, CheckViolation)
        and '"writer_label_check"' in message
        and '"writer"' in message
    )
    exception, message = insert_writer(cursor, label="Napoleão Bona Parte")
    assert exception is None and message == "", message


def test_insert_check_value_in_birthday_column(cursor: Cursor) -> None:
    today = datetime.date.today()
    date_under_eighteen = today.replace(year=today.year - 17)
    exception, message = insert_writer(
        cursor,
        birthday=date_under_eighteen.strftime("%Y-%m-%d"),
    )
    assert (
        isinstance(exception, CheckViolation)
        and '"writer_birthday_check"' in message
        and '"writer"' in message
    ), message
