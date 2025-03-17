# Build YTsaurus Airflow Provider

## Installation from source code

```bash
pip install .
```

## How to try

- Run Airflow

```bash
cd dev
make run
```

- Follow the instructions from [documentation [EN]](https://ytsaurus.tech/docs/en/api/airflow/userdoc#howtostart) or [[RU]](https://ytsaurus.tech/docs/ru/api/airflow/userdoc#howtostart).

## How to run tests

- Install requirements

```bash
pip install -r dev/requirements.txt
```

- Style check

```bash
ruff check
ruff format
```

- Type check

```bash
mypy
```

- Pytest  

```bash
pytest .
```
