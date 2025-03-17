from __future__ import annotations

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
            },
        ],
        "versions": [__version__],
    }
