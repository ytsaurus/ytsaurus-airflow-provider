from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any, Union, cast

import yt.type_info
import yt.wrapper
from airflow import DAG, XComArg
from airflow.decorators import task
from airflow.io.path import ObjectStoragePath
from yt.yson.yson_types import YsonStringProxy

from ytsaurus_airflow_provider.operators import (
    CreateOperator,
    ReadTableOperator,
    SetOperator,
    WriteTableOperator,
)

base = ObjectStoragePath("s3://ytsaurus-airflow/", conn_id="aws_default")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
}


with DAG(
    "ytsaurus_io_example",
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

    schema = [
        {"name": "key", "type": "int32"},
        {"name": "value", "type": "string"},
        {"name": "foo", "type": "any"},
    ]

    attributes = {"schema": schema}

    unserializable_value = YsonStringProxy()
    unserializable_value._bytes = b"\xfa"  # type: ignore  # noqa: SLF001

    example_data: list[dict[str, int | str | bytes | YsonStringProxy]] = [
        {"key": 1, "value": "1", "foo": 123},
        {"key": 2, "value": "2\n2", "foo": "hello world"},
        {"key": 3, "value": "3\t3\t3", "foo": b"123123"},
        {"key": 4, "value": "4\n4\t4", "foo": b"hello/\\world"},
        {"key": 5, "value": "\n\n", "foo": b'{"b"="\\xfb";}'},
        {"key": 6, "value": "\t\t", "foo": unserializable_value},
    ]

    create_table = CreateOperator(
        task_id="create_table",
        path=f"{execution_ypath}/table",
        node_type="table",
        ignore_existing=True,
        attributes=attributes,
    )

    write_tabel = WriteTableOperator(
        task_id="write_tabel",
        input_data=example_data,
        path=f"{execution_ypath}/table",
    )

    read_table = ReadTableOperator(
        task_id="read_table",
        path=f"{execution_ypath}/table",
    )

    @task
    def create_bucket_if_not_exists() -> None:
        base.mkdir(exist_ok=True)

    io_formats: list[Any] = [
        "<enable_type_conversion=true>dsv",
        yt.wrapper.SchemafulDsvFormat(columns=["key", "value", "foo"]),
        "yson",
        "json",
    ]

    create_task = create_bucket_if_not_exists()

    workflow = create_node >> create_table >> write_tabel >> read_table >> create_task

    for io_format in io_formats:
        format_str = str(io_format).split(">")[-1]
        read_table_to_storage = ReadTableOperator(
            task_id=f"read_table_to_storage_{format_str}",
            path=f"{execution_ypath}/table",
            object_storage_path=base / f"example_table_{format_str}.{format_str}",
            object_storage_format=io_format,
        )

        create_table = CreateOperator(
            task_id=f"create_table_{format_str}",
            path=f"{execution_ypath}/table_{format_str}",
            node_type="table",
            ignore_existing=True,
        )

        write_table_from_storage = WriteTableOperator(
            task_id=f"write_table_from_storage_{format_str}",
            path=f"{execution_ypath}/table_{format_str}",
            object_storage_path=base / f"example_table_{format_str}.{format_str}",
            object_storage_format=io_format,
        )

        create_task >> read_table_to_storage >> create_table >> write_table_from_storage
