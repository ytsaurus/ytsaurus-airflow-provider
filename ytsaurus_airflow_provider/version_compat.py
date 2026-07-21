from __future__ import annotations


def get_base_airflow_version_tuple() -> tuple[int, int, int]:
    from airflow import __version__
    from packaging.version import Version

    airflow_version = Version(__version__)
    return airflow_version.major, airflow_version.minor, airflow_version.micro


AIRFLOW_V_3_0_PLUS: bool = get_base_airflow_version_tuple() >= (3, 0, 0)

__all__ = ["AIRFLOW_V_3_0_PLUS"]
