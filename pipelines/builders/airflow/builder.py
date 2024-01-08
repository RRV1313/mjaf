"Airflow DAG Builder Module"
import ast
from pathlib import Path

from pipelines import utils
from pipelines.builders import base
from pipelines.builders.airflow import config


# TODO: incorporate more configuration such as: image: str, service_account_name: str, slack_channel: str
class AirflowBuilder(base.Builder):
    def to_airflow(
        self,
        dag_config: config.AirflowDAGConfig | None = None,
    ) -> str:
        dag: str = utils.read_template(
            template_path=Path(__file__).parent / 'dag.py.jinja2',
            template_fields={'pipeline': self, 'dag_kwargs': dag_config or {}},
        )
        _validate_dag(dag=dag)
        return dag


def _validate_dag(dag: str) -> None:
    ast.parse(source=dag)
