from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any, Literal, Sequence

from airflow.models import BaseOperator

from ytsaurus_airflow_provider.hooks import YTsaurusHook

if TYPE_CHECKING:
    from airflow.utils.context import Context
    from yt.wrapper.format import Format
    from yt.wrapper.ypath import YPath


class ListOperator(BaseOperator):
    template_fields: Sequence[str] = (
        "path",
        "max_size",
        "absolute",
        "attributes",
        "sort",
        "read_from",
        "ytsaurus_conn_id",
    )

    def __init__(
        self,
        *,
        path: str | YPath,
        max_size: None | int = None,
        absolute: None | bool = None,
        attributes: None | dict[str, Any] = None,
        sort: bool = True,
        read_from: None | str = None,
        ytsaurus_conn_id: str = YTsaurusHook.default_conn_name,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.path = path
        self.max_size = max_size
        self.absolute = absolute
        self.attributes = attributes
        self.sort = sort
        self.read_from = read_from
        self.ytsaurus_conn_id = ytsaurus_conn_id

    def execute(self, context: Context) -> dict[str, Any]:
        hook = YTsaurusHook(ytsaurus_conn_id=self.ytsaurus_conn_id)
        client = hook.get_conn()
        self.log.info("Listing node `%s`.", self.path)
        result = json.loads(
            client.list(
                path=self.path,
                max_size=self.max_size,
                format="json",
                absolute=self.absolute,
                attributes=self.attributes,
                sort=self.sort,
                read_from=self.read_from,
            ).decode()
        )
        context["ti"].xcom_push(key="path", value=self.path)
        return result


class CreateOperator(BaseOperator):
    template_fields: Sequence[str] = (
        "type",
        "path",
        "recursive",
        "ignore_existing",
        "lock_existing",
        "force",
        "attributes",
        "ignore_type_mismatch",
        "ytsaurus_conn_id",
    )

    def __init__(
        self,
        *,
        node_type: Literal[
            "table",
            "file",
            "map_node",
            "document",
            "string_node",
            "int64_node",
            "uint64_node",
            "double_node",
            "list_node",
            "boolean_node",
            "link",
        ],
        path: str | YPath,
        recursive: bool = False,
        ignore_existing: bool = False,
        lock_existing: None | bool = None,
        force: None | bool = None,
        attributes: None | dict[str, Any] = None,
        ignore_type_mismatch: bool = False,
        ytsaurus_conn_id: str = YTsaurusHook.default_conn_name,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.type = node_type
        self.path = path
        self.recursive = recursive
        self.ignore_existing = ignore_existing
        self.lock_existing = lock_existing
        self.force = force
        self.attributes = attributes
        self.ignore_type_mismatch = ignore_type_mismatch
        self.ytsaurus_conn_id = ytsaurus_conn_id

    def execute(self, context: Context) -> None:
        hook = YTsaurusHook(ytsaurus_conn_id=self.ytsaurus_conn_id)
        client = hook.get_conn()
        self.log.info("Creating an object of type `%s` with path `%s`.", self.type, self.path)
        object_id = client.create(
            type=self.type,
            path=self.path,
            recursive=self.recursive,
            ignore_existing=self.ignore_existing,
            lock_existing=self.lock_existing,
            force=self.force,
            attributes=self.attributes,
            ignore_type_mismatch=self.ignore_type_mismatch,
        )
        context["ti"].xcom_push(key="object_id", value=object_id)
        context["ti"].xcom_push(key="path", value=self.path)


class RemoveOperator(BaseOperator):
    template_fields: Sequence[str] = (
        "path",
        "recursive",
        "force",
        "ytsaurus_conn_id",
    )

    def __init__(
        self,
        *,
        path: str | YPath,
        recursive: bool = False,
        force: bool = False,
        ytsaurus_conn_id: str = YTsaurusHook.default_conn_name,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.path = path
        self.recursive = recursive
        self.force = force
        self.ytsaurus_conn_id = ytsaurus_conn_id

    def execute(self, context: Context) -> None:
        hook = YTsaurusHook(ytsaurus_conn_id=self.ytsaurus_conn_id)
        client = hook.get_conn()
        self.log.info("Removing an object with path `%s`.", self.path)
        client.remove(path=self.path, recursive=self.recursive, force=self.force)
        context["ti"].xcom_push(key="path", value=self.path)


class SetOperator(BaseOperator):
    template_fields: Sequence[str] = (
        "path",
        "value",
        "format",
        "recursive",
        "force",
        "ytsaurus_conn_id",
    )

    def __init__(
        self,
        *,
        path: str | YPath,
        value: Any,
        set_format: None | str | Format = None,
        recursive: bool = False,
        force: None | bool = None,
        ytsaurus_conn_id: str = YTsaurusHook.default_conn_name,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.path = path
        self.value = value
        self.format = set_format
        self.recursive = recursive
        self.force = force
        self.ytsaurus_conn_id = ytsaurus_conn_id

    def execute(self, context: Context) -> None:
        hook = YTsaurusHook(ytsaurus_conn_id=self.ytsaurus_conn_id)
        client = hook.get_conn()
        self.log.info("Setting value on an object with path `%s`.", self.path)
        client.set(
            path=self.path,
            value=self.value,
            format=self.format,
            recursive=self.recursive,
            force=self.force,
        )
        context["ti"].xcom_push(key="path", value=self.path)


class GetOperator(BaseOperator):
    template_fields: Sequence[str] = (
        "path",
        "max_size",
        "attributes",
        "read_from",
        "ytsaurus_conn_id",
    )

    def __init__(
        self,
        *,
        path: str | YPath,
        max_size: None | int = None,
        attributes: None | dict[str, Any] = None,
        read_from: None | str = None,
        ytsaurus_conn_id: str = YTsaurusHook.default_conn_name,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.path = path
        self.max_size = max_size
        self.attributes = attributes
        self.read_from = read_from
        self.ytsaurus_conn_id = ytsaurus_conn_id

    def execute(self, context: Context) -> dict[str, Any]:
        hook = YTsaurusHook(ytsaurus_conn_id=self.ytsaurus_conn_id)
        client = hook.get_conn()
        self.log.info("Getting an object with path `%s`.", self.path)
        result = json.loads(
            client.get(
                path=self.path,
                max_size=self.max_size,
                attributes=self.attributes,
                format="json",
                read_from=self.read_from,
            ).decode()
        )
        context["ti"].xcom_push(key="path", value=self.path)
        return result
