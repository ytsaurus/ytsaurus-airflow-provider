from __future__ import annotations

from typing import TYPE_CHECKING

from tests.conftest import patch_operator_exec
from ytsaurus_airflow_provider.operators.ytsaurus_cypress import (
    CreateOperator,
    GetOperator,
    ListOperator,
    RemoveOperator,
    SetOperator,
)

if TYPE_CHECKING:
    from unittest.mock import MagicMock

    import yt.wrapper as yt
    from airflow.utils.context import Context


for operator_cls in [CreateOperator, GetOperator, ListOperator, RemoveOperator, SetOperator]:
    patch_operator_exec(operator_cls)


def test_create_operator(yt_client: yt.YtClient, node_path: str, context: Context, context_mock: dict[str, MagicMock]) -> None:
    test_table = f"{node_path}/table"
    op = CreateOperator(
        task_id="test_create_table",
        node_type="table",
        path=test_table,
        attributes={"a": 1, "b": 2},
    )
    op.execute(context)
    context_mock["ti"].xcom_push.assert_any_call(key="path", value=test_table)
    assert yt_client.exists(test_table), "Table was not created"
    assert yt_client.get(f"{test_table}/@a") == 1
    assert yt_client.get(f"{test_table}/@b") == 2


def test_remove_operator(yt_client: yt.YtClient, node_path: str, context: Context, context_mock: dict[str, MagicMock]) -> None:
    test_table = f"{node_path}/table"
    yt_client.create("table", test_table)
    op = RemoveOperator(
        task_id="test_remove_table",
        path=test_table,
    )
    op.execute(context)
    context_mock["ti"].xcom_push.assert_any_call(key="path", value=test_table)
    assert not yt_client.exists(test_table), "Table was not removed"


def test_list_operator(yt_client: yt.YtClient, node_path: str, context: Context, context_mock: dict[str, MagicMock]) -> None:
    for i in range(10):
        yt_client.create("table", f"{node_path}/table{i}")
    op = ListOperator(
        task_id="test_list",
        path=node_path,
    )
    result = op.execute(context)
    context_mock["ti"].xcom_push.assert_any_call(key="path", value=node_path)
    assert sorted(result) == sorted([f"table{i}" for i in range(10)])


def test_set_operator(yt_client: yt.YtClient, node_path: str, context: Context, context_mock: dict[str, MagicMock]) -> None:
    test_table = f"{node_path}/table"
    yt_client.create("table", test_table)
    op = SetOperator(
        task_id="test_set",
        path=f"{test_table}/@a",
        value=1,
    )
    op.execute(context)
    context_mock["ti"].xcom_push.assert_any_call(key="path", value=f"{test_table}/@a")
    assert yt_client.get(f"{test_table}/@a") == 1


def test_get_operator(yt_client: yt.YtClient, node_path: str, context: Context, context_mock: dict[str, MagicMock]) -> None:
    test_table = f"{node_path}/table"
    yt_client.create("table", test_table, attributes={"a": 1})
    op = GetOperator(
        task_id="test_get",
        path=f"{test_table}/@a",
    )
    result = op.execute(context)
    context_mock["ti"].xcom_push.assert_any_call(key="path", value=f"{test_table}/@a")
    assert result == 1
