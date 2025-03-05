from __future__ import annotations

from typing import TYPE_CHECKING, Optional

import pytest
import yt.wrapper as yt

from tests.conftest import patch_operator_exec
from ytsaurus_airflow_provider.operators.ytsaurus_io import (
    ReadTableOperator,
    WriteTableOperator,
)

if TYPE_CHECKING:
    from unittest.mock import MagicMock

    from airflow.utils.context import Context
    from upath import UPath

for operator_cls in [ReadTableOperator, WriteTableOperator]:
    patch_operator_exec(operator_cls)


def test_write_operator(yt_client: yt.YtClient, node_path: str, context: Context, context_mock: dict[str, MagicMock]) -> None:
    content = [{"a": 1, "b": 2}, {"a": 3, "b": 4}]
    table_path = f"{node_path}/table"
    op = WriteTableOperator(
        task_id="test_write_operator",
        path=table_path,
        input_data=content,
    )
    op.execute(context)
    context_mock["ti"].xcom_push.assert_any_call(key="path", value=table_path)
    written = list(yt_client.read_table(table_path))
    assert written == content


def test_read_operator(yt_client: yt.YtClient, node_path: str, context: Context, context_mock: dict[str, MagicMock]) -> None:
    content = [{"a": 1, "b": 2}, {"a": 3, "b": 4}]
    table_path = f"{node_path}/table"
    yt_client.write_table(table_path, content)
    op = ReadTableOperator(task_id="test_read_operator", path=table_path)
    result = op.execute(context)
    context_mock["ti"].xcom_push.assert_any_call(key="path", value=table_path)
    assert result == content


@pytest.mark.parametrize("io_format", ["dsv", "yson", "json"])
def test_read_write_storage_operator(
    yt_client: yt.YtClient,
    node_path: str,
    tmp_path: UPath,
    io_format: str,
    context: Context,
    context_mock: dict[str, MagicMock],
) -> None:
    @yt.yt_dataclass
    class Row:
        a: str
        b: str
        c: Optional[str] = None

    content = [Row(a="1", b="2"), Row(a="3", b="4"), Row(a="5", b="6", c="7")]  # type: ignore
    table_path = f"{node_path}/table_source"
    yt_client.write_table_structured(table_path, Row, content)
    tmp_path_file = tmp_path / "test_read_to_storage_operator"
    op_read = ReadTableOperator(
        task_id="test_read_to_storage_operator",
        path=table_path,
        object_storage_path=tmp_path_file,
        object_storage_format=io_format,
    )
    result = op_read.execute(context)
    context_mock["ti"].xcom_push.assert_any_call(key="path", value=table_path)
    context_mock["ti"].xcom_push.assert_any_call(key="object_storage_path", value=tmp_path_file.as_uri())
    assert result is None
    context_mock["ti"].reset_mock()
    op_write = WriteTableOperator(
        task_id="test_write_from_storage_operator",
        path=f"{node_path}/table_destination",
        object_storage_path=tmp_path_file,
        object_storage_format=io_format,
    )
    result = op_write.execute(context)
    context_mock["ti"].xcom_push.assert_any_call(key="path", value=f"{node_path}/table_destination")
    context_mock["ti"].xcom_push.assert_any_call(key="object_storage_path", value=tmp_path_file.as_uri())
    assert result is None
    written = list(
        yt_client.read_table_structured(f"{node_path}/table_destination", Row),
    )
    assert written == content


def test_write_fail(node_path: str, context: Context, tmp_path: UPath) -> None:
    with pytest.raises(ValueError, match="Either `object_storage_path` or `input_data` must be provided, but not both."):
        WriteTableOperator(
            task_id="test_write_operator",
            path=f"{node_path}/table",
            input_data=[{"a": "b"}],
            object_storage_path=tmp_path,
        ).execute(context)

    with pytest.raises(ValueError, match="Either `object_storage_path` or `input_data` must be provided, but not both."):
        WriteTableOperator(
            task_id="test_write_operator",
            path=f"{node_path}/table",
        ).execute(context)

    with pytest.raises(ValueError, match="Cannot write table from object storage when `object_storage_format` is None."):
        WriteTableOperator(
            task_id="test_write_operator",
            path=f"{node_path}/table",
            object_storage_path=tmp_path,
        ).execute(context)


def test_read_fail(node_path: str, context: Context, tmp_path: UPath) -> None:
    with pytest.raises(ValueError, match="Cannot read table to object storage when `object_storage_path` is None."):
        ReadTableOperator(
            task_id="test_read_operator",
            path=f"{node_path}/table",
            object_storage_format="yson",
        ).execute(context)

    with pytest.raises(ValueError, match="Cannot read table to object storage when `object_storage_format` is None."):
        ReadTableOperator(
            task_id="test_read_operator",
            path=f"{node_path}/table",
            object_storage_path=tmp_path,
        ).execute(context)
