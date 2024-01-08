"Module containing functions and classes for interacting with Google's BigQuery."
import contextlib
import functools
import typing
from collections.abc import Callable, Generator, Sequence
from dataclasses import InitVar, dataclass, field
from datetime import datetime
from typing import Any, NoReturn, ParamSpec, TypeAlias, TypeVar

import pandas as pd
import tenacity
from dateutil import parser
from google.api_core import exceptions as google_exceptions
from google.cloud.bigquery import client as bq_client
from google.cloud.bigquery import enums, job, schema
from google.cloud.bigquery import table as bq_table
from google.cloud.bigquery.job import CopyJob, QueryJob, _AsyncJob
from google.cloud.bigquery.table import RowIterator

from algo_features import environment
from pipelines import utils
from pipelines.resources import factory
from pipelines.resources.bigquery import utils as bq_utils

logger = utils.get_logger(name="bigquery_loader")

BQJob: TypeAlias = job.CopyJob | job.ExtractJob | job.QueryJob | job.LoadJob | job.UnknownJob
BQJobs = tuple[BQJob, ...]

P = ParamSpec('P')  # generic paramaters of a function
R = TypeVar('R')  # generic returns of a function
# ?: for more details, visit:  peps.python.org/pep-0612/#the-components-of-a-paramspec


accumulation_templates: dict[str, str] = {
    "cumulative_sum": 'IFNULL(a.{field.name}, 0) + IFNULL(b.{field.name}, 0) as {field.name}',
    "coalesce": 'COALESCE(a.{field.name}, b.{field.name}) as {field.name}',
    "max": 'MAX(a.{field.name}, b.{field.name}) as {field.name}',
}


def _extract_or_set_client(args, kwargs) -> bq_client.Client:
    if not args and not kwargs:
        return bq_utils.default_client()
    for arg in args:
        if isinstance(arg, bq_client.Client):
            client: bq_client.Client = arg
            break
        elif client_attr := getattr(arg, 'client', None):
            client = client_attr
    for kw, val in kwargs.items():
        if isinstance(val, bq_client.Client) or kw == 'client':
            client = val
            break
        elif client_attr := getattr(val, 'client', None):
            client = client_attr
    return client


def validate_bq_jobs(function: Callable[P, BQJobs]) -> Callable[P, BQJobs]:
    """Decorator that wraps the job validator object and provides a simple to use interface."""

    @functools.wraps(wrapped=function)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> BQJobs:
        client: bq_client.Client = _extract_or_set_client(args=args, kwargs=kwargs)

        jobs: BQJobs = function(*args, **kwargs)
        if jobs == [None]:
            logger.warning('Jobs not found. Exiting.')
        else:
            bq_job_validator = BQJobValidator(jobs=jobs, client=client)
            bq_job_validator.validate_jobs()
        return jobs

    return wrapper


class BQJobValidator:
    """Class containing methods utilized for for checking the state of GCP jobs."""

    class IncompleteJobsError(Exception):
        """Raised when a GCP job is incomplete."""

        pass

    class FailedJobsError(Exception):
        """Raised when GCP job has failed"""

        pass

    def __init__(self, jobs: BQJobs, client: bq_client.Client) -> None:
        self.completed_job_ids: set[str] = set()
        self.incomplete_job_ids: set[str] = {
            job.job_id for job in jobs if isinstance(job.job_id, str)
        }
        self.job_ids_with_errors: set[str] = set()
        self.client: bq_client.Client = client
        self.total_jobs: int = len(jobs)
        logger.info(f'Validating jobs: {self.incomplete_job_ids}')

    @tenacity.retry(
        retry=tenacity.retry_if_exception_type(exception_types=IncompleteJobsError),
        stop=tenacity.stop_after_delay(30 * 60 * 60),  # 30 minutes
        wait=tenacity.wait_exponential(multiplier=2, min=15, max=181),
    )
    def validate_jobs(self) -> None:
        """Method for checking the status of jobs. Retries if any incomplete jobs remain.

        Raises:
                                        FailedJobsError: raised if all jobs have completed and there is at least one error
        """
        self._update_jobs_status()
        if self.incomplete_job_ids:
            raise self.IncompleteJobsError
        if self.job_ids_with_errors:
            logger.info(
                f"{len(self.job_ids_with_errors)} jobs completed with errors."
                f"Job ids of jobs with errors: {self.job_ids_with_errors}"
            )
            raise self.FailedJobsError

    def _update_jobs_status(self) -> None:
        """Method for updating the status of a job.
        1. Loop through all jobs
        2. Get the status of the job using the provided client
        3. If job is done, add the job to completed jobs
                                        a. If the job has an error add to jobs with errors
        4. Update incomplete jobs by removing completed jobs from the set
        """
        for incomplete_job_id in self.incomplete_job_ids:
            job: BQJob = self.client.get_job(job_id=incomplete_job_id)
            if job.state == "DONE":
                self.completed_job_ids.add(job.job_id)
                job_result: _AsyncJob | RowIterator = job.result()
                has_error_or_no_rows: bool = (
                    job.error_result or getattr(job_result, 'total_rows', None) == 0
                )  # TODO: raise explicit error for missing data
                self._log_job_status()
                if has_error_or_no_rows:
                    self.job_ids_with_errors.add(job.job_id)
        self.incomplete_job_ids -= self.completed_job_ids

    def _log_job_status(self) -> None:
        """Helper method for logging job status."""
        logger.info(
            f"{len(self.completed_job_ids)}/{self.total_jobs} "
            f"of jobs have completed. ({round(len(self.completed_job_ids) / self.total_jobs * 100) }%) "
            f"Remaining jobs: {self.incomplete_job_ids - self.completed_job_ids}"
        )


@dataclass
class ManagedBigQueryTable(factory.Resource):
    partition_field: str
    start_date: str
    table_name: InitVar[str | None] = None
    sql: InitVar[str | None] = None
    udfs: list[str] | None = None
    recursive: bool = False
    accumulation_method: str = "coalesce"
    join_key: str | None = None
    dry_run: bool = False
    template: str | None = None
    template_fields: dict[str, Any] = field(default_factory=dict)
    upstream_table_names: list[str] = field(default_factory=list)
    recursive_suffix: str | None = None

    def __post_init__(self, table_name: str | None, sql: str | None) -> None:
        self._table_name: str = table_name or self.name
        self._sql: str = sql or f'{self._table_name}.sql.jinja2'
        self.destination_table_id: str = environment.BQ_DESTINATION.format(
            table_id=self._table_name
        )
        self.concurrency: int = (
            1 if self.recursive or self.template else 5
        )  # TODO: _get_concurrency_from_slot_consumption
        self.default_action: Callable[..., Any] = self.update
        if self.recursive and not self.join_key:
            raise AttributeError(
                "Cannot create recursive table without specifying `join_key`.\n"
                "Please add `join_key` to the resource definition in your config."
            )

    @utils.LazyProperty
    def client(self) -> bq_client.Client:
        return bq_utils.default_client()

    def get_query(self, run_day: str, recursive: bool | None = None) -> str:
        if self.template:
            return self._generate_query_from_template(run_day=run_day)
        if recursive is None:
            recursive = self.recursive
        query: str = bq_utils.construct_query_from_filename(
            sql=self._sql,
            udfs=self.udfs or [],
            template_fields={
                'run_day': run_day,
                'partition_field': self.partition_field,
                'data_env': environment.BQ_DATA_ENV,
                'upstream_tables': self.upstream_table_names,
                'algo_project': environment.ALGO_PROJECT,
                'start_date': self.start_date,
                'destination_table_id': self.destination_table_id,
            }
            | self.template_fields,
        )
        if recursive:
            return self._generate_recursive_query(run_day=run_day, query=query)
        else:
            return query

    def _generate_recursive_query(self, run_day: str, query: str) -> str:
        return utils.read_template(
            template_path=environment.CONFIG_SQL_DIR / 'recursive_template.sql.jinja2',
            template_fields={
                'run_day': run_day,
                'start_date': self.start_date,
                'partition_field': self.partition_field,
                'schema': self.schema,
                'query': query,
                'destination_table_id': self.sideload_destination or self.destination_table_id,
                'join_key': self.join_key,
                'accumulation_method': self._get_accumulation_method(),
            },
        )

    def _generate_query_from_template(self, run_day: str):
        if len(self.upstream_table_names) == 1:
            return utils.read_template(
                template_path=environment.TEMPLATE_PATH / f'{self.template}.sql.jinja2',
                template_fields={
                    'run_day': run_day,
                    'start_date': self.start_date,
                    'destination_table_id': self.sideload_destination or self.destination_table_id,
                    'upstream_table': environment.BQ_DESTINATION.format(
                        table_id=self.upstream_table_names[0]
                    ),
                }
                | self.template_fields,
            )
        else:
            raise NotImplementedError

    @utils.log_return
    def _get_accumulation_method(self) -> str:
        return ',\n'.join(
            [
                self._append_recursive_suffix(
                    accumulation_sql=accumulation_templates[self.accumulation_method].format(
                        field=field
                    )
                )
                for field in self.schema
                if field.name not in (self.partition_field, self.join_key)
            ]
        )

    def _append_recursive_suffix(self, accumulation_sql: str) -> str:
        return '_'.join(
            filter(
                None,
                [
                    accumulation_sql,
                    self.recursive_suffix,
                ],
            )
        )

    @utils.log_return
    def _get_expected_partitions(self, run_day: str) -> list[str]:
        return list(
            pd.date_range(start=self.start_date, end=run_day).strftime(date_format='%Y-%m-%d')
        )

    @utils.LazyProperty
    def new_upstream_table_since_last_update(self) -> bool:
        return any(
            self.table.modified < comparison_table.created
            for comparison_table in self.upstream_tables
            if comparison_table.created
        )

    @utils.LazyProperty
    def schema(self) -> list[schema.SchemaField]:
        return bq_utils.get_schema_from_query_dry_run(
            client=self.client,
            query=self.get_query(run_day=self.start_date, recursive=False),
        )

    @utils.LazyProperty
    def query_schema_matches_table_schema(self) -> bool:
        return self.table.schema == self.schema

    @utils.LazyProperty
    def requires_backfill(self) -> bool:
        return bool(
            not self.table_exists
            or not self.partition_field_matches_table_partition_field
            or self.new_upstream_table_since_last_update
            or not self.query_schema_matches_table_schema
        )

    @utils.LazyProperty
    def partition_field_matches_table_partition_field(self) -> bool:
        return self.partition_field == self.table.time_partitioning.field

    @utils.LazyProperty
    def hashed_query(self) -> int:
        return hash((environment.CONFIG_SQL_DIR / self._sql).read_text())

    @utils.LazyProperty
    def sideload_destination(self) -> str | None:
        if self.requires_backfill:
            return f'{self.destination_table_id}_backfill_{self.hashed_query}'
        else:
            return None

    @utils.LazyProperty
    def table(self) -> bq_table.Table:
        return self.client.get_table(table=self.destination_table_id)

    @utils.LazyProperty
    def table_exists(self) -> bool:
        try:
            self.table
            return True
        except google_exceptions.NotFound:
            return False

    @utils.LazyProperty
    def upstream_tables(self) -> list[bq_table.Table]:
        return [
            self.client.get_table(table=environment.BQ_DESTINATION.format(table_id=table_id))
            for table_id in self.upstream_table_names
        ]

    def update(self, run_day: str) -> None:
        if partitions_to_update := self._get_partitions_to_update(run_day=run_day):
            destination: str = self.sideload_destination or self.destination_table_id
            logger.info(f"Writing results to: {destination}")
            self._load_partitions_from_query_results(
                partitions_to_update=partitions_to_update,
                destination_table_id=destination,
            )
        if self.sideload_destination:
            self._replace_table(
                source_table=self.sideload_destination,
                destination_table=self.destination_table_id,
            )
        logger.info(f"Update of {self._table_name} complete.")

    @utils.log_return
    def _get_partitions_to_update(self, run_day: str) -> list[str] | None | NoReturn:
        if not (missing_partitions := self._get_missing_partitions(run_day=run_day)):
            return None

        if self.requires_backfill:
            return self._get_remaining_partitions_from_sideload_table(run_day=run_day)
        elif self.recursive and (
            date_strings_are_discontinuous(date_strings=missing_partitions)
            or run_day not in missing_partitions
        ):
            return [str(day) for day in pd.date_range(start=min(missing_partitions), end=run_day)]
        else:
            return missing_partitions

    @utils.log_return
    def _get_missing_partitions(self, run_day: str) -> list[str]:
        existing_partitions: set[str] = self._get_existing_partitions(run_day=run_day)
        return [
            partition
            for partition in self._get_expected_partitions(run_day=run_day)
            if partition not in existing_partitions
        ]

    @utils.log_return
    def _get_remaining_partitions_from_sideload_table(self, run_day: str) -> list[str]:
        return sorted(
            set(self._get_expected_partitions(run_day=run_day))
            - self._get_existing_partitions_from_sideload_table(run_day=run_day)
        )

    def _get_existing_partitions_from_sideload_table(self, run_day: str) -> set[str]:
        return self._get_existing_partitions(
            destination_table_id=self.sideload_destination, run_day=run_day
        )

    def delete_user_data(
        self,
        id_column: str,
        id: str,
    ) -> None:
        logger.info(f'Deleting user data from: {self.destination_table_id}...')
        query: str = "DELETE FROM " f"{self.destination_table_id} " f"WHERE {id_column} = '{id}'"
        try:
            job: QueryJob = self.client.query(query=query)
            job.result()
            logger.info('Done.')
        except Exception:
            for e in job.errors:
                logger.info(f'Error: {e["message"]}.')

    def _get_query_job_config(
        self,
        run_day: str,
        destination_table_id: str,
        dry_run: bool | None = None,
    ) -> job.QueryJobConfig:
        destination = '$'.join([destination_table_id, run_day.replace('-', '')])
        logger.info(f'destination: {destination}')
        return job.QueryJobConfig(
            destination=destination,
            time_partitioning=bq_table.TimePartitioning(
                type_=bq_table.TimePartitioningType.DAY, field=self.partition_field
            ),
            priority=enums.QueryPriority.INTERACTIVE,
            dry_run=dry_run or self.dry_run,
            write_disposition=job.WriteDisposition.WRITE_TRUNCATE,
            use_query_cache=False,
            use_legacy_sql=False,
        )

    @tenacity.retry(
        retry=tenacity.retry_if_exception_type(exception_types=BQJobValidator.FailedJobsError),
        wait=tenacity.wait_exponential(multiplier=2, min=2, max=8),
    )
    def _load_partitions_from_query_results(
        self, partitions_to_update: list[str], destination_table_id: str
    ) -> None:
        for batch in get_batches(iterable=partitions_to_update, batch_size=self.concurrency):
            logger.info(f"Updating batch: {batch}.")
            self._run_query_over_run_days(
                run_days=typing.cast(list[str], batch), destination_table_id=destination_table_id
            )

    @validate_bq_jobs
    def _run_query_over_run_days(self, run_days: list[str], destination_table_id: str) -> BQJobs:
        return tuple(
            self.client.query(
                query=self.get_query(run_day=run_day),
                job_config=self._get_query_job_config(
                    run_day=run_day,
                    destination_table_id=destination_table_id,
                ),
            )
            for run_day in run_days
        )

    def _replace_table(
        self,
        source_table: str,
        destination_table: str,
    ) -> None:
        """Function that replaces a destination table with a source table,
        deleting the destination if it exists.
        """
        logger.info(f"Replacing {destination_table} with {source_table}.")
        with contextlib.suppress(google_exceptions.NotFound):
            self.client.delete_table(table=destination_table)
        self._copy_table(source=source_table, destination=destination_table)
        self.client.delete_table(table=source_table)

    @utils.log_return
    def _get_existing_partitions(
        self, run_day: str, destination_table_id: str | None = None
    ) -> set[str]:
        project, dataset, table = (destination_table_id or self.destination_table_id).split('.')

        try:
            job: QueryJob = self.client.query(
                query=utils.read_template(
                    template_path=environment.CONFIG_SQL_DIR / 'get_existing_partitions.sql.jinja2',
                    template_fields={
                        'project': project,
                        'dataset': dataset,
                        'table': table,
                        'run_day': run_day,
                    },
                )
            )
            return {row['days'].strftime('%Y-%m-%d') for row in job.result()}
        except google_exceptions.NotFound:
            return set()

    def cancel_jobs(self, destinations: list[str]) -> None:
        """Helper function for canceling bq jobs."""
        for state in ["running", "pending"]:
            for found_job in self.client.list_jobs(state_filter=state):
                destination = found_job.destination.to_api_repr()
                project = destination.get("projectId")
                table = destination.get("tableId")
                dataset = destination.get("datasetId")
                job_destination: str = f"{project}.{dataset}.{table}"
                # cancel job only if the original feature set destination matches the input features
                if any(dest in job_destination for dest in destinations):
                    canceled_job = self.client.cancel_job(job_id=found_job.job_id, location="us")
                    logger.info(f"{canceled_job.location}:{canceled_job.job_id} cancelled")

    @validate_bq_jobs
    def _copy_table(self, source: str, destination: str) -> tuple[job.CopyJob]:
        copy_job: CopyJob = self.client.copy_table(
            sources=source,
            destination=destination,
        )
        return (copy_job,)


def _get_concurrency_from_slot_consumption(completed_job: QueryJob) -> int:
    """Helper function for calculating average slot usage of a query.
    Source: https://cloud.google.com/blog/topics/developers-practitioners/monitoring-bigquery-reservations-and-slot-utilization-information_schema
    """
    average_slot_usage: float = completed_job.slot_millis / (
        job_duration := (completed_job.ended - completed_job.started).total_seconds() * 1000
    )
    max_concurrency: int = max(int(environment.TOTAL_SLOTS // average_slot_usage), 1)
    logger.info(
        f'Average Slot Usage: {average_slot_usage}, duration: {job_duration} seconds, recommended concurrency: {max_concurrency}'
    )
    return max_concurrency


def get_batches(iterable: Sequence[Any], batch_size: int) -> Generator[Sequence[Any], Any, None]:
    for i in range(0, len(iterable), batch_size):
        yield iterable[i : i + batch_size]


def date_strings_are_discontinuous(date_strings: list[str]) -> bool:
    dates: list[datetime] = [parser.parse(date_string) for date_string in date_strings]
    expected_range: pd.DatetimeIndex = pd.date_range(start=min(dates), end=max(dates))
    return len(dates) != len(expected_range)
