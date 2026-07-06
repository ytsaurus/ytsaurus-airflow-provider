from __future__ import annotations

import json
from typing import Any

__version__ = "0.1.0"


def get_provider_info() -> dict[str, Any]:
    return {
        "package-name": "ytsaurus-airflow-provider",
        "name": "YTsaurus",
        "description": "`YTsaurus <http://ytsaurus.tech>`__",
        "connection-types": [
            {
                "connection-type": "ytsaurus_cluster",
                "hook-class-name": "ytsaurus_airflow_provider.hooks.ytsaurus.YTsaurusHook",
                "conn-fields": {
                    "proxy": {
                        "label": "Cluster Proxy",
                        "schema": {"type": "string"},
                    },
                    "token": {
                        "label": "Cluster Token",
                        "schema": {"type": "string", "format": "password"},
                    },
                    "client_config": {
                        "label": "Client Config (JSON)",
                        "schema": {"type": ["string", "null"], "format": "json"},
                        "description": 'JSON configuration for YTsaurus client, e.g., {"create_table_attributes": {"compression_codec": "brotli_3"}}',
                    },
                },
                "ui-field-behaviour": {
                    "hidden-fields": ["host", "schema", "login", "password", "port"],
                    "relabeling": {},
                    "placeholders": {
                        "client_config": json.dumps(
                            {"create_table_attributes": {"compression_codec": "brotli_3"}},
                            indent=4,
                        ),
                    },
                },
            },
        ],
        "versions": [__version__],
    }
