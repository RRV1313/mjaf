"Module for environment dependent variables."
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Final, Literal

BQ_DATASET: Final[Literal['algo_features']] = 'algo_features'

os.environ.setdefault(key='AIRFLOW__CORE__DAGS_FOLDER', value=str(object=Path.cwd()))
ALGO_PROJECT: Final[str] = os.environ.get(
    'AIRFLOW_VAR_ALGO_PROJECT', default='nbcu-ds-algo-int-001'
)
BQ_BILLING_PROJECT: Final[str] = ALGO_PROJECT


PROJECT_DIR: Path = Path(os.environ['AIRFLOW__CORE__DAGS_FOLDER']) / 'algo_features'
CONFIG_DIR: Path = PROJECT_DIR / 'configs'
CONFIG_ASSETS_DIR: Path = CONFIG_DIR / 'assets'
CONFIG_UDFS_DIR: Path = CONFIG_ASSETS_DIR / 'udfs'
CONFIG_SQL_DIR: Path = CONFIG_ASSETS_DIR / 'sql'
CONFIG_VALIDATION_DIR: Path = CONFIG_ASSETS_DIR / 'validation_queries'


match ALGO_PROJECT:
    case 'nbcu-ds-algo-int-001':
        BQ_DATA_ENV = 'nbcu-ds-prod-001'
        TOTAL_SLOTS: int = 100
        START_DATE: str = os.environ.get(
            'AIRFLOW_VAR_START_DATE', (datetime.now() - timedelta(days=2)).strftime('%Y-%m-%d')
        )
        PROD: bool = False
    case 'nbcu-ds-algo-int-nft-001':
        BQ_DATA_ENV = 'nbcu-ds-prod-001'
        TOTAL_SLOTS = 100
        START_DATE = os.environ.get(
            'AIRFLOW_VAR_START_DATE', (datetime.now() - timedelta(days=2)).strftime('%Y-%m-%d')
        )
        PROD = False
    case 'nbcu-ds-algo-prod-001':
        BQ_DATA_ENV = 'nbcu-ds-prod-001'
        TOTAL_SLOTS = 300
        START_DATE = os.environ.get('AIRFLOW_VAR_START_DATE', "2023-04-01")
        PROD = True
    case _:
        msg_0: str = 'wrong value supplied for AIRFLOW_VAR_ALGO_PROJECT'
        raise ValueError(msg_0)

BQ_DESTINATION: str = f'{BQ_BILLING_PROJECT}.{BQ_DATASET}.{{table_id}}'
