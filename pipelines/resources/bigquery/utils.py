from typing import Any

from google.cloud.bigquery import client as bq_client
from google.cloud.bigquery import job, schema

from algo_features import environment
from pipelines import utils


def default_client() -> bq_client.Client:
    return bq_client.Client(project=environment.BQ_BILLING_PROJECT)


def construct_query_from_filename(
    sql: str, udfs: list[str] | None = None, template_fields: dict[str, Any] | None = None
) -> str:
    return '\n'.join(
        [
            *[
                utils.read_template(
                    template_path=environment.CONFIG_UDFS_DIR / udf,
                    template_fields=template_fields,
                )
                for udf in udfs or []
            ],
            utils.read_template(
                template_path=environment.CONFIG_SQL_DIR / sql,
                template_fields=template_fields,
            ),
        ],
    )


def get_schema_from_query_dry_run(
    client: bq_client.Client,
    query: str,
) -> list[schema.SchemaField]:
    query_job: job.QueryJob = client.query(
        query=query,
        job_config=job.QueryJobConfig(dry_run=True),
    )
    # TODO: this feels dangerous, but is currently the only way to pull this information without running a query
    return [
        schema.SchemaField(name=field['name'], field_type=field['type'], mode=field['mode'])
        for field in query_job._properties['statistics']['query']['schema']['fields']
    ]
