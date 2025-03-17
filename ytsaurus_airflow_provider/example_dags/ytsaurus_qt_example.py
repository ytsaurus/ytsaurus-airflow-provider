from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Union, cast

from airflow import DAG, XComArg
from airflow.decorators import task
from airflow.io.path import ObjectStoragePath

from ytsaurus_airflow_provider.operators import CreateOperator, RunQueryOperator, SetOperator, WriteTableOperator

if TYPE_CHECKING:
    from upath import UPath

base = ObjectStoragePath("s3://ytsaurus-airflow/", conn_id="aws_default")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
}

with DAG(
    "ytsaurus_qt_example",
    default_args=default_args,
    start_date=datetime(2010, 10, 29, tzinfo=timezone.utc),
    schedule="@once",
    tags=["yt"],
) as dag:

    @task
    def execution_ypath_task() -> str:
        suffix = uuid.uuid4().__str__()[:8]
        return "//home/airflow/example-" + suffix

    # The construction is for mypy to understand that `exec_path' is `str`.
    execution_ypath: Union[str, XComArg] = execution_ypath_task()
    execution_ypath = cast("str", execution_ypath)

    create_node = CreateOperator(
        task_id="create_node",
        path=execution_ypath,
        node_type="map_node",
        recursive=True,
        ignore_existing=True,
    )

    set_ttl = SetOperator(
        task_id="set_ttl",
        path=f"{execution_ypath}/@expiration_timeout",
        value=604800000,
    )

    create_node >> set_ttl

    content = [
        {"a": 1, "b": 3, "c": 11},
        {"a": 2, "b": 2, "c": 12},
        {"a": 3, "b": 1, "c": 13},
    ]

    write_data = WriteTableOperator(
        task_id="write_data",
        path=f"{execution_ypath}/table",
        input_data=content,
    )

    create_node >> write_data

    query = f"""
    PRAGMA yt.InferSchema = '1';

    SELECT
        *
    FROM `{execution_ypath}/table`;

    SELECT
        a
    FROM `{execution_ypath}/table`;

    SELECT
        b
    FROM `{execution_ypath}/table`;

    SELECT
        c
    FROM `{execution_ypath}/table`;
    """  # noqa: S608

    run_query = RunQueryOperator(
        task_id="run_query",
        query=query,
        engine="yql",
        object_storage_paths=[None, None, None, base / "c"],
    )

    write_data >> run_query

    expected_0 = [
        {"_other": [], "a": 1, "b": 3, "c": 11},
        {"_other": [], "a": 2, "b": 2, "c": 12},
        {"_other": [], "a": 3, "b": 1, "c": 13},
    ]

    expected_1 = [
        {"a": 1},
        {"a": 2},
        {"a": 3},
    ]

    expected_2 = [
        {"b": 3},
        {"b": 2},
        {"b": 1},
    ]

    expected_3 = b'{"c": 11}{"c": 12}{"c": 13}'

    @task
    def assert_any(content: Any, expected: Any) -> None:
        assert content == expected, f"Expected {expected}, got {content}"

    @task
    def assert_file(filepath: UPath, expected: bytes) -> None:
        with filepath.open("rb") as file:
            content = file.read()
            assert expected == content, f"Expected {expected.decode()}, got {content.decode()}"

    assert_any(run_query.output["result_0"], expected_0)  # type: ignore
    assert_any(run_query.output["result_1"], expected_1)  # type: ignore
    assert_any(run_query.output["result_2"], expected_2)  # type: ignore
    assert_file_task = assert_file(base / "c", expected_3)

    run_query >> assert_file_task
