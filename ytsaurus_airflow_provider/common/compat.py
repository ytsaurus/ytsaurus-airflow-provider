from __future__ import annotations

from typing import TYPE_CHECKING

from ytsaurus_airflow_provider.version_compat import AIRFLOW_V_3_0_PLUS

if AIRFLOW_V_3_0_PLUS:
    from airflow.sdk import DAG, BaseHook, BaseOperator, ObjectStoragePath, task
else:
    from airflow import DAG  # type: ignore[attr-defined,no-redef]
    from airflow.decorators import task  # type: ignore[attr-defined,no-redef]
    from airflow.hooks.base import BaseHook  # type: ignore[attr-defined,no-redef]
    from airflow.io.path import ObjectStoragePath  # type: ignore[import-not-found,no-redef]
    from airflow.models import BaseOperator  # type: ignore[attr-defined,no-redef]

if TYPE_CHECKING:
    if AIRFLOW_V_3_0_PLUS:
        from airflow.sdk import Context
    else:
        from airflow.utils.context import Context  # type: ignore[attr-defined,no-redef]  # noqa: F401

__all__ = [
    "DAG",
    "BaseHook",
    "BaseOperator",
    "ObjectStoragePath",
    "task",
]
