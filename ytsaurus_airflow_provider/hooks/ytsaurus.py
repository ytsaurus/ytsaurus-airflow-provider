from __future__ import annotations

import json
import logging
from typing import Any

from airflow.hooks.base import BaseHook
from yt.common import YtError
from yt.wrapper.client_impl import YtClient

log = logging.getLogger(__name__)


class YTsaurusHook(BaseHook):
    conn_name_attr = "ytsaurus_conn_id"
    default_conn_name = "ytsaurus_cluster_default"
    conn_type = "ytsaurus_cluster"
    hook_name = "YTsaurus Cluster"

    @classmethod
    def get_connection_form_widgets(cls) -> dict[str, Any]:
        from flask_appbuilder.fieldwidgets import BS3PasswordFieldWidget, BS3TextFieldWidget
        from flask_babel import lazy_gettext
        from wtforms import PasswordField, StringField

        return {
            "proxy": StringField(lazy_gettext("Cluster Proxy"), widget=BS3TextFieldWidget()),
            "token": PasswordField(lazy_gettext("Cluster Token"), widget=BS3PasswordFieldWidget()),
        }

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        """Return custom UI field behaviour for AWS Connection."""
        return {
            "hidden_fields": ["host", "schema", "login", "password", "port"],
            "relabeling": {},
            "placeholders": {
                "extra": json.dumps(
                    {"client_config": {"create_table_attributes": {"compression_codec": "brotli_3"}}},
                    indent=4,
                ),
            },
        }

    def __init__(
        self,
        ytsaurus_conn_id: str = default_conn_name,
    ) -> None:
        super().__init__()
        self.ytsaurus_conn_id = ytsaurus_conn_id

    def get_conn(self) -> YtClient:
        conn = self.get_connection(self.ytsaurus_conn_id)
        extra_dejson: dict[str, Any] = conn.extra_dejson
        if "client_config" not in extra_dejson:
            extra_dejson["client_config"] = None
        return YtClient(extra_dejson["proxy"], extra_dejson["token"], config=extra_dejson["client_config"])

    def test_connection(self) -> tuple[bool, str]:
        try:
            client = self.get_conn()
            client.list("/")
        except YtError as ex:
            return False, f"Connection failed due to: {ex}"
        return True, "Connection successfully tested"
