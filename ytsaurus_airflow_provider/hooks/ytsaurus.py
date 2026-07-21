from __future__ import annotations

import json
import logging
from typing import Any, Optional

from yt.common import YtError
from yt.wrapper.client_impl import YtClient

from ytsaurus_airflow_provider.common.compat import BaseHook
from ytsaurus_airflow_provider.version_compat import AIRFLOW_V_3_0_PLUS

log = logging.getLogger(__name__)


class YTsaurusHook(BaseHook):
    conn_name_attr = "ytsaurus_conn_id"
    default_conn_name = "ytsaurus_cluster_default"
    conn_type = "ytsaurus_cluster"
    hook_name = "YTsaurus Cluster"

    if not AIRFLOW_V_3_0_PLUS:

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
        proxy = extra_dejson["proxy"]
        token = extra_dejson["token"]
        client_config_raw: Optional[str | dict[str, Any]] = extra_dejson.get("client_config")
        if isinstance(client_config_raw, str):
            client_config: dict[str, Any] | None = json.loads(client_config_raw) if client_config_raw else None
        else:
            client_config = client_config_raw
        return YtClient(proxy, token, config=client_config)

    def test_connection(self) -> tuple[bool, str]:
        try:
            client = self.get_conn()
            client.list("/")
        except YtError as ex:
            return False, f"Connection failed due to: {ex}"
        return True, "Connection successfully tested"
