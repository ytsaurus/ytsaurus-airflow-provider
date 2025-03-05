import pytest
from airflow.models import DagBag
from airflow.utils.state import State


@pytest.mark.parametrize("dag_name", ["ytsaurus_cypress_example", "ytsaurus_io_example", "ytsaurus_qt_example"])
def test_dags(dag_name: str) -> None:
    dag_bag = DagBag(dag_folder="ytsaurus_airflow_provider/example_dags")

    dag = dag_bag.get_dag(dag_name)
    assert dag is not None, "DAG not found"

    dagrun = dag.test()
    assert dagrun.state == State.SUCCESS, "DAG run failed."
