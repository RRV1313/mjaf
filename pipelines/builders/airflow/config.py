import dataclasses
from typing import Any


def _default_args() -> dict[str, int | bool]:
    return {'retries': 3, 'depends_on_past': False, 'max_active_runs': 1}


@dataclasses.dataclass
class AirflowDAGConfig:
    default_args: dict[str, Any] = dataclasses.field(default_factory=_default_args)
    description: str | None = None
    max_active_runs: int = 1
    catchup: bool = False
    # TODO: add on failure callback
