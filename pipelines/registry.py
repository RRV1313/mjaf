import itertools
import typing
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any, TypedDict

from pipelines import pure, utils

ActionParameters = dict[str, Any | list[Any]]
RawActions = dict[str, ActionParameters]


# TODO: reimplement schedule typed dict
class Schedule(TypedDict):
    cron: str
    start: str
    end: str | None


class Action(TypedDict):
    """Action type."""

    name: str
    action: str
    upstream: list[str]
    parameters: dict[str, Any]


class RawConfig(TypedDict):
    """Raw config type."""

    name: str
    upstream: list[str] | None
    resource: dict[str, Any]
    actions: RawActions | list[str] | None
    schedule: dict[str, str | None] | None


class Config(TypedDict):
    """Config type."""

    name: str
    upstream: list[str]
    resource: dict[str, Any]
    actions: list[Action]
    schedule: dict[str, str | None] | None


config_loaders: dict[str, Callable[..., list[dict[str, Any]]]] = {
    '.yaml': utils.read_yaml,
}


class ConfigRegistry:
    """Registry for storing configs."""

    def __init__(self) -> None:
        self.registry: dict[str, list[Config]] = {}

    def __getitem__(self, key: str) -> list[Config]:
        if key not in self.registry:
            raise KeyError(
                f"Config: {key}, does not exist in the registry. Check your config file for errors."
            )
        return self.registry[key]

    def load(self, config_path: Path) -> None:
        """Load configs into registry."""
        self._add_configs_to_registry(configs=_load_config(config_path=config_path))
        self._reset_upstream()

    def refresh(self, config_path: Path) -> None:
        """Refresh internal registry."""
        self.registry = {}
        self.load(config_path=config_path)

    @property
    def configs(self) -> Iterator[Config]:
        """Returns configs in the internal registry."""
        return itertools.chain.from_iterable(self.registry.values())

    @classmethod
    def from_directory(cls, directory: Path):
        config_registry: ConfigRegistry = cls()
        for file_path in directory.iterdir():
            if file_path.is_file():
                config_registry.load(config_path=directory / file_path)
        return config_registry

    def _add_configs_to_registry(self, configs: list[RawConfig]) -> None:
        """Adds configs to the internal registry."""
        for raw_config in configs:
            self.registry[raw_config['name']] = [
                {
                    'name': raw_config['name'],
                    'upstream': raw_config.get('upstream') or [],
                    'resource': raw_config['resource'],
                    'actions': _expand_actions(
                        config_name=raw_config['name'],
                        raw_actions=_get_raw_actions(raw_config=raw_config),
                    ),
                    'schedule': raw_config.get('schedule'),
                }
            ]

    def _reset_upstream(
        self,
    ) -> None:
        for config in self.configs:
            config['upstream'] = [
                config['name']
                for upstream_config_name in config.get('upstream', [])
                for config in self.registry[upstream_config_name]
            ]


def get_registry(config_path: Path) -> tuple[ConfigRegistry, Callable[..., Iterator[Config]]]:
    """Creates registry and refresh function."""
    registry = ConfigRegistry()
    registry.load(config_path=config_path)

    def refresh_configs() -> Iterator[Config]:
        nonlocal registry
        registry.refresh(config_path=config_path)
        return registry.configs

    return registry, refresh_configs


def _load_config(config_path: Path) -> list[RawConfig]:
    loaded_configs: list[dict[str, Any]] = _load_config_from_path(config_path=config_path)
    return _cast_loaded_configs_to_raw_configs(loaded_configs=loaded_configs)


def _cast_loaded_configs_to_raw_configs(loaded_configs) -> list[RawConfig]:
    if _loaded_configs_are_raw_configs(loaded_configs=loaded_configs):
        return typing.cast(list[RawConfig], loaded_configs)
    else:
        raise ValueError(
            f'Incorrect config specification. All configs must adhere to: {RawConfig.__annotations__}.'
        )


def _load_config_from_path(config_path: Path) -> list[dict[str, Any]]:
    loader: Callable[..., list[dict[str, Any]]] = config_loaders[config_path.suffix]
    return loader(config_path)


def _loaded_configs_are_raw_configs(loaded_configs: list[dict[str, Any]]) -> bool:
    annotations: dict[str, set[type]] = utils.get_generics_from_annotations(
        annotations=RawConfig.__annotations__
    )

    return all(
        key in annotations and type(config[key]) in annotations[key]
        for config in loaded_configs
        for key in config
    )


def _get_raw_actions(raw_config: RawConfig) -> RawActions:
    match raw_actions := raw_config.get('actions'):
        case list():
            return {action: {} for action in raw_actions}
        case dict():
            return raw_actions
        case None:
            return {"default_action": {}}
        case _:
            raise ValueError(
                f'Incorrect action specification. Must be of type list, dict or None. Received: {type(raw_actions)}'
            )


def _expand_actions(config_name: str, raw_actions: RawActions) -> list[Action]:
    expanded_actions: list[Action] = []
    previous_actions: list[Action] = []
    for action, parameters in raw_actions.items():
        expanded_actions.extend(
            previous_actions := _expand_action(
                config_name=config_name,
                upstream=[action['name'] for action in previous_actions],
                action=action,
                parameters=parameters,
            )
        )
    return expanded_actions


def _expand_action(
    config_name: str, upstream: list[str], action: str, parameters: dict[str, Any]
) -> list[Action]:
    return [
        _get_action_config(
            config_name=config_name,
            upstream=upstream,
            action=action,
            parameters=parameters,
        )
        for parameters in pure.expand_dictionary(dictionary=parameters)
    ]


def _get_action_config(
    config_name: str,
    action: str,
    upstream: list[str] | None = None,
    parameters: dict[str, Any] | None = None,
) -> Action:
    parameters = parameters or {}
    return {
        'name': '_'.join(
            filter(None, [config_name, action, pure.convert_dict_to_str(d=parameters)])
        ),
        'action': action,
        'upstream': upstream or [],
        'parameters': parameters,
    }
