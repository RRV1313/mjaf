"Module containing functions and classes for interacting with Google's BigQuery."
from collections.abc import Callable
from dataclasses import InitVar, dataclass
from typing import Any

from google.api_core import exceptions as google_exceptions
from google.cloud.bigquery import client as bq_client

from algo_features import environment
from pipelines import utils
from pipelines.resources import factory
from pipelines.resources.bigquery import utils as bq_utils

logger = utils.get_logger(name="bigquery_loader")


@dataclass
class ExternalBigQueryTable(factory.Resource):
    dataset: str
    table_name: InitVar[str | None] = None

    def __post_init__(self, table_name: str | None) -> None:
        self._table_name: str = table_name or self.name
        self.default_action: Callable[..., Any] = self.check_partition

    @utils.LazyProperty
    def client(self) -> bq_client.Client:
        return bq_utils.default_client()

    def check_partition(self, run_day: str):
        job = self.client.query(
            query=bq_utils.construct_query_from_filename(
                sql="assert_partition_exists.sql.jinja2",
                template_fields={
                    'project': environment.BQ_DATA_ENV,
                    'dataset': self.dataset,
                    'table': self._table_name,
                    'run_day': run_day,
                },
            )
        )
        try:
            job.result()
        except google_exceptions.BadRequest as e:
            raise ValueError(
                "Assertion failed. Partition does not "
                f"exist in {self._table_name} for run_day: {run_day}."
            ) from e
        logger.info(f"Partition exists in {self._table_name} for run_day: {run_day}.")
