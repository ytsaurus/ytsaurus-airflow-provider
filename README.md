# YTsaurus Airflow provider


## Documentation

Documenation is available at [YTsaurus Documentation [EN]](https://ytsaurus.tech/docs/en/api/airflow/start) and [[RU]](https://ytsaurus.tech/docs/ru/api/airflow/start).

## Installations

### Requirements

- Python 3.8+
- Airflow 2.9.0+

### Installation from source code

```bash
pip install .
```

<!-- ### Installation from PyPI

```bash
pip install ytsaurus-airflow-provider
``` -->

## How to try

- Run Airflow

```bash
cd dev
make run
```

- Follow the instructions from [documentation [EN]](https://ytsaurus.tech/docs/en/api/airflow/userdoc#howtostart) and [[RU]](https://ytsaurus.tech/docs/ru/api/airflow/userdoc#howtostart).

## How to run tests

- Install requirements

```bash
pip install -r dev/requirements.txt
```

### Style check

```bash
ruff check
ruff format
```

### Type check

```bash
mypy
```

### Pytest  

```bash
pytest .
```
