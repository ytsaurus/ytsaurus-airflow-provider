from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any, Generator, Sequence

from airflow.models import BaseOperator

from ytsaurus_airflow_provider.hooks import YTsaurusHook

if TYPE_CHECKING:
    from airflow.utils.context import Context
    from upath import UPath
    from yt.wrapper.format import Format
    from yt.wrapper.ypath import YPath


READ_BUFFER_SIZE = 1024 * 1024


class WriteTableOperator(BaseOperator):
    template_fields: Sequence[str] = (
        "path",
        "input_data",
        "object_storage_path",
        "object_storage_format",
        "table_writer",
        "max_row_buffer_size",
        "force_create",
        "ytsaurus_conn_id",
    )

    def __init__(
        self,
        *,
        path: str | YPath,
        input_data: Any | None = None,
        object_storage_path: None | UPath = None,
        object_storage_format: None | str | Format = None,
        table_writer: dict[str, Any] | None = None,
        max_row_buffer_size: int | None = None,
        force_create: None | bool = None,
        ytsaurus_conn_id: str = YTsaurusHook.default_conn_name,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.path: str | YPath = path
        self.input_data = input_data
        self.object_storage_path = object_storage_path
        self.object_storage_format = object_storage_format
        self.table_writer = table_writer
        self.max_row_buffer_size = max_row_buffer_size
        self.force_create = force_create
        self.ytsaurus_conn_id = ytsaurus_conn_id

        self._raw = False

        if (not (self.object_storage_path or self.input_data)) or (self.object_storage_path and self.input_data):
            msg = "Either `object_storage_path` or `input_data` must be provided, but not both."
            raise ValueError(msg)

        if self.object_storage_path:
            if self.object_storage_format is None:
                msg = "Cannot write table from object storage when `object_storage_format` is None."
                raise ValueError(msg)
            self._raw = True

    def _reader(self, context: Context) -> Generator[bytes, None, None]:
        if self.object_storage_path is None:
            msg = "Cannot read from object storage as no object storage is provided."
            raise ValueError(msg)
        context["ti"].xcom_push(key="object_storage_path", value=self.object_storage_path.as_uri())
        with self.object_storage_path.open("rb") as file:
            buffer_bytes = file.read(READ_BUFFER_SIZE)
            while buffer_bytes:
                yield buffer_bytes
                buffer_bytes = file.read(READ_BUFFER_SIZE)

    def execute(self, context: Context) -> Any:
        hook = YTsaurusHook(ytsaurus_conn_id=self.ytsaurus_conn_id)
        client = hook.get_conn()
        self.log.info("Writing data to table `%s`.", self.path)

        input_stream = self.input_data if self.input_data else self._reader(context)

        client.write_table(
            table=self.path,
            input_stream=input_stream,
            format=self.object_storage_format,
            table_writer=self.table_writer,
            max_row_buffer_size=self.max_row_buffer_size,
            force_create=self.force_create,
            raw=self._raw,
        )
        context["ti"].xcom_push(key="path", value=self.path)


class ReadTableOperator(BaseOperator):
    template_fields: Sequence[str] = (
        "path",
        "object_storage_path",
        "object_storage_format",
        "table_reader",
        "control_attributes",
        "unordered",
        "response_parameters",
        "omit_inaccessible_columns",
        "ytsaurus_conn_id",
    )

    def __init__(
        self,
        *,
        path: str | YPath,
        object_storage_path: None | UPath = None,
        object_storage_format: None | str | Format = None,
        table_reader: None | dict[str, Any] = None,
        control_attributes: None | dict[str, Any] = None,
        unordered: None | bool = None,
        response_parameters: None | dict[str, Any] = None,
        omit_inaccessible_columns: None | bool = None,
        ytsaurus_conn_id: str = YTsaurusHook.default_conn_name,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)  # type: ignore
        self.path: str | YPath = path
        self.object_storage_path = object_storage_path
        self.object_storage_format = object_storage_format
        self.table_reader = table_reader
        self.control_attributes = control_attributes
        self.unordered = unordered
        self.response_parameters = response_parameters
        self.omit_inaccessible_columns = omit_inaccessible_columns
        self.ytsaurus_conn_id = ytsaurus_conn_id

        if self.object_storage_path or self.object_storage_format:
            if self.object_storage_format is None:
                msg = "Cannot read table to object storage when `object_storage_format` is None."
                raise ValueError(msg)
            if self.object_storage_path is None:
                msg = "Cannot read table to object storage when `object_storage_path` is None."
                raise ValueError(msg)

    def _dump_to_object_storage(self, context: Context, raw_rows: Generator[bytes, None, None]) -> None:
        if self.object_storage_path is None:
            msg = "Cannot dump to object storage as no object storage is provided."
            raise ValueError(msg)
        context["ti"].xcom_push(key="object_storage_path", value=self.object_storage_path.as_uri())
        with self.object_storage_path.open("wb") as file:
            for row in raw_rows:
                file.write(row)

    def execute(self, context: Context) -> list[dict[str, Any]] | None:
        hook = YTsaurusHook(ytsaurus_conn_id=self.ytsaurus_conn_id)
        client = hook.get_conn()
        self.log.info("Reading data from table `%s`.", self.path)
        context["ti"].xcom_push(key="path", value=self.path)

        read_format = self.object_storage_format if self.object_storage_path else "json"

        raw_rows = client.read_table(
            table=self.path,
            format=read_format,
            table_reader=self.table_reader,
            control_attributes=self.control_attributes,
            unordered=self.unordered,
            raw=True,
            response_parameters=self.response_parameters,
            enable_read_parallel=False,
            omit_inaccessible_columns=self.omit_inaccessible_columns,
        )

        if self.object_storage_path:
            context["ti"].xcom_push(key="object_storage_path", value=self.object_storage_path.as_uri())
            self._dump_to_object_storage(context, raw_rows)
        else:
            return list(map(json.loads, raw_rows))
        return None
