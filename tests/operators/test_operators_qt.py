from __future__ import annotations

from typing import TYPE_CHECKING

from tests.conftest import patch_operator_exec
from ytsaurus_airflow_provider.operators.ytsaurus_qt import RunQueryOperator

if TYPE_CHECKING:
    from unittest.mock import MagicMock

    import yt.wrapper as yt
    from airflow.utils.context import Context


for operator_cls in [RunQueryOperator]:
    patch_operator_exec(operator_cls)


def test_run_query_sync(yt_client: yt.YtClient, node_path: str, context: Context, context_mock: dict[str, MagicMock]) -> None:
    table_path = f"{node_path}/table"
    yt_client.write_table(table_path, [{"a": 1, "b": 2}, {"a": 3, "b": 4}])

    query = f"""
    PRAGMA yt.InferSchema = '1';
    SELECT
        a
    FROM `{table_path}`
    LIMIT 50;
    SELECT
        b
    FROM `{table_path}`
    LIMIT 50;
    """  # noqa: S608

    op = RunQueryOperator(
        task_id="test_run_query_sync",
        query=query,
        engine="yql",
    )
    op.execute(context)
    context_mock["ti"].xcom_push.assert_any_call(key="result_0", value=[{"a": 1}, {"a": 3}])
    context_mock["ti"].xcom_push.assert_any_call(key="result_1", value=[{"b": 2}, {"b": 4}])
    calls = context_mock["ti"].xcom_push.call_args_list
    for call in calls:
        if call.kwargs["key"] == "meta":
            meta = call.kwargs["value"]
            break
    else:
        msg = "meta was not pushed"
        raise AssertionError(msg)
    context_mock["ti"].xcom_push.assert_any_call(key="query_id", value=meta["id"])


def test_run_query_async(yt_client: yt.YtClient, node_path: str, context: Context, context_mock: dict[str, MagicMock]) -> None:
    table_path = f"{node_path}/table"
    yt_client.write_table(table_path, [{"a": 1, "b": 2}, {"a": 3, "b": 4}])

    query = f"""
    PRAGMA yt.InferSchema = '1';
    SELECT
        a
    FROM `{table_path}`
    LIMIT 50;
    SELECT
        b
    FROM `{table_path}`
    LIMIT 50;
    """  # noqa: S608

    op = RunQueryOperator(
        task_id="test_run_query_async",
        query=query,
        engine="yql",
        sync=False,
    )
    op.execute(context)
    calls = context_mock["ti"].xcom_push.call_args_list
    for call in calls:
        if call.kwargs["key"] == "query_id":
            query_id = call.kwargs["value"]
            break
    else:
        msg = "query_id was not pushed"
        raise AssertionError(msg)
    query_data = yt_client.get_query(query_id)
    assert query_data
