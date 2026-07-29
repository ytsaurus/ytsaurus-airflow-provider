from __future__ import annotations

import json
from itertools import zip_longest
from typing import TYPE_CHECKING, Any, Literal

import yt.wrapper
import yt.wrapper.query_commands

from ytsaurus_airflow_provider.common.compat import BaseOperator
from ytsaurus_airflow_provider.hooks import YTsaurusHook

if TYPE_CHECKING:
    from collections.abc import Sequence

    import yt.yson.yson_types
    from upath import UPath

    from ytsaurus_airflow_provider.common.compat import Context, ObjectStoragePath


class RunQueryOperator(BaseOperator):
    template_fields: Sequence[str] = (
        "engine",
        "query",
        "settings",
        "files",
        "stage",
        "annotations",
        "access_control_objects",
        "sync",
        "object_storage_paths",
    )

    def __init__(
        self,
        *,
        engine: Literal["ql", "yql", "chyt", "spyt"],
        query: str,
        settings: dict[str, Any] | None = None,
        files: list[dict[str, Any]] | list[yt.yson.yson_types.YsonType] | None = None,
        stage: str | None = None,
        annotations: dict[str, Any] | None = None,
        access_control_objects: list[str] | None = None,
        sync: bool = True,
        object_storage_paths: list[UPath | ObjectStoragePath | None] | None = None,
        ytsaurus_conn_id: str = YTsaurusHook.default_conn_name,
        **kwargs: Any,
    ) -> None:
        if object_storage_paths is None:
            object_storage_paths = []
        super().__init__(**kwargs)  # type: ignore
        self.engine = engine
        self.query = query
        self.settings = settings
        self.files = files
        self.stage = stage
        self.annotations = annotations
        self.access_control_objects = access_control_objects
        self.sync = sync
        self.ytsaurus_conn_id = ytsaurus_conn_id
        self.object_storage_paths = object_storage_paths

    def execute(self, context: Context) -> None:
        hook = YTsaurusHook(ytsaurus_conn_id=self.ytsaurus_conn_id)
        client = hook.get_conn()
        query_object = client.run_query(
            engine=self.engine,
            query=self.query,
            settings=self.settings,
            files=self.files,
            stage=self.stage,
            annotations=self.annotations,
            access_control_objects=self.access_control_objects,
            sync=self.sync,
        )

        meta = query_object.get_meta()
        context["ti"].xcom_push(key="meta", value=query_object.get_meta())
        context["ti"].xcom_push(key="query_id", value=meta["id"])

        if self.sync:
            for i, (object_storage_path, rows) in enumerate(
                zip_longest(self.object_storage_paths, query_object.get_results(), fillvalue=None),
            ):
                if rows is None:
                    self.log.warning("Query result index=%d is missing.", i)
                    continue
                if object_storage_path is not None:
                    self.log.info(
                        "Writing results to object storage.",
                        extra={"ResultIndex": i, "ObjectStoragePath": object_storage_path},
                    )
                    with object_storage_path.open("wb") as file:
                        for row in rows:
                            file.write(json.dumps(row).encode("utf-8"))
                else:
                    self.log.info("Writing results index=%d to XCom with key=result_%d.", i, i)
                    context["ti"].xcom_push(key=f"result_{i}", value=list(rows))
