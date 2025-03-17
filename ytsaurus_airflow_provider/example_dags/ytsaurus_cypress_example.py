from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any, Union, cast

from airflow import DAG, XComArg
from airflow.decorators import task

from ytsaurus_airflow_provider.operators import (
    CreateOperator,
    GetOperator,
    ListOperator,
    ReadTableOperator,
    RemoveOperator,
    SetOperator,
    WriteTableOperator,
)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
}

with DAG(
    "ytsaurus_cypress_example",
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

    list_nodes = ListOperator(
        task_id="list_nodes",
        path=execution_ypath,
    )

    create_table = CreateOperator(
        task_id="create_table",
        path=f"{execution_ypath}/table",
        node_type="table",
        ignore_existing=True,
    )

    write_table = WriteTableOperator(
        task_id="write_table",
        input_data=[{"key": "value"}],
        path=f"{execution_ypath}/table",
    )

    read_table = ReadTableOperator(
        task_id="read_table",
        path=f"{execution_ypath}/table",
    )

    write_more = WriteTableOperator(
        task_id="write_more",
        input_data=[{"key": "value", "foo": "bar"} for _ in range(10)],
        path=f"<append=%true>{execution_ypath}/table",
    )

    read_more = ReadTableOperator(
        task_id="read_more",
        path=f"{execution_ypath}/table",
    )

    remove = RemoveOperator(
        task_id="remove",
        path=execution_ypath,
        recursive=True,
        force=True,
    )

    set_get_tests = [
        "hello",
        "hello world",
        1234,
        "hello\nworld",
        {"hello": 1234, "world": 4321},
        ["hello", 1234, "world"],
    ]

    for i, set_get_set in enumerate(set_get_tests):
        set_task = SetOperator(
            task_id=f"set_{i}",
            path=f"{execution_ypath}/@key_{i}",
            value=set_get_set,
        )

        create_node >> set_task

        get_task = GetOperator(
            task_id=f"get_{i}",
            path=f"{execution_ypath}/@key_{i}",
        )

        set_task >> get_task

        @task
        def assert_set_get_equal(value: Any, expected: Any) -> None:
            assert value == expected

        assert_task = assert_set_get_equal(get_task.output["return_value"], set_get_set)  # type: ignore

        assert_task >> remove

    create_node >> set_ttl >> list_nodes >> create_table >> write_table >> read_table >> write_more >> read_more >> remove
