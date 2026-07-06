import pytest
from airflow.models import DagBag
from airflow.utils.state import State

from ytsaurus_airflow_provider.version_compat import AIRFLOW_V_3_0_PLUS

EXAMPLE_DAGS_FOLDER = "ytsaurus_airflow_provider/example_dags"
BUNDLE_NAME = "ytsaurus-airflow-provider-example-dags-test"


@pytest.mark.parametrize("dag_name", ["ytsaurus_cypress_example", "ytsaurus_io_example", "ytsaurus_qt_example"])
def test_dags(dag_name: str) -> None:
    dag_bag = DagBag(dag_folder=EXAMPLE_DAGS_FOLDER)

    dag = dag_bag.get_dag(dag_name)
    assert dag is not None, "DAG not found"

    if AIRFLOW_V_3_0_PLUS:
        from airflow.dag_processing.dagbag import sync_bag_to_db
        from airflow.models.dagbundle import DagBundleModel
        from airflow.utils.session import create_session

        with create_session() as session:
            if session.get(DagBundleModel, BUNDLE_NAME) is None:
                session.add(DagBundleModel(name=BUNDLE_NAME))
        sync_bag_to_db(dag_bag, BUNDLE_NAME, None)

    dagrun = dag.test()
    assert dagrun.state == State.SUCCESS, "DAG run failed."
