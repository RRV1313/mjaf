import dataclasses
from typing import Any

from pipelines import registry


@dataclasses.dataclass
class Action:
    """
    action (str): the action to take.
    name (str): unique identifier for the action. This field is required because you may take the same action in several contexts.
        Without a unique identifier for each Action, there would be naming collisions when generating a pipeline.
    upstream (str): list of all upstream action names.
    parameters (str): parameters for the action.
    """

    action: str
    name: str
    upstream: list[str]
    parameters: dict[str, Any]


def create_actions(action_configs: list[registry.Action]) -> list[Action]:
    return [Action(**action_config) for action_config in action_configs]
