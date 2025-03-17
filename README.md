# YTsaurus Airflow Provider

## Provider package

This is a apache airflow provider package for [YTsaurus](https://ytsaurus.tech/). All classes for this provider package are in `ytsaurus_airflow_provider` python package.

Documenation is available at [YTsaurus Documentation [EN]](https://ytsaurus.tech/docs/en/api/airflow/start) or [[RU]](https://ytsaurus.tech/docs/ru/api/airflow/start).

## Installation

You can install this package on top of an existing Airflow 2.9+ installation via pip:
```
pip install ytsaurus-airflow-provider
```

The package requires `Python 3.8+`.

## Requirements

PIP package                             | Version required
----------------------------------------|------------------
`apache-airflow`                        | `>=2.9.0`
`ytsaurus-client`                       | `>=0.13.23`
`ytsaurus-yson`                         | `>=0.4.3`
`apache-airflow-providers-amazon[s3fs]` | `>=3.0.0`
`s3fs`                                  | `<=2024.10.0`
