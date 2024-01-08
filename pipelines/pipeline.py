from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

from dagviz import dagre

from pipelines import action, graph, pipe, registry, schedule, utils
from pipelines.builders import base


class Pipeline(base.BuilderMixin):
    """Composite containing Pipes ordered topologically in a DAG."""

    def __init__(  # noqa: PLR0913
        self,
        name: str,
        schedule: schedule.Schedule | None,
        pipes: list[pipe.Pipe],
        refresh_configs: Callable[..., Iterator[registry.Config]],
        config_path: str,
    ) -> None:
        super().__init__()
        self.name: str = name
        self.schedule = schedule
        self.pipes: graph.DAG[pipe.Pipe] = graph.DAG(nodes=pipes)
        self._refresh_configs: Callable[..., Iterator[registry.Config]] = refresh_configs
        self.config_path: str = config_path

    def __getattr__(self, attr: str) -> Any:
        return getattr(self.pipes, attr)

    @property
    def actions(self) -> graph.DAG[action.Action]:
        return pipe.combine_pipe_dags(pipes=self.pipes)

    def run(
        self,
        pipe: str | None = None,
        action: str | None = None,
        parameters: dict[str, Any] | None = None,
        refresh: bool = True,
        **kwargs: dict[str, Any],
    ) -> None:
        """Runs each pipe in topological order."""
        if refresh:
            self.refresh_configs()
        if pipe:
            self.pipes[pipe].run(action=action, parameters=parameters, **kwargs)
        else:
            for node in self.pipes:
                node.run(action=None, **kwargs)

    def show(self, refresh: bool = True) -> dagre.Dagre:
        """Renders a graphical representation of the pipeline."""
        if refresh:
            self.refresh_configs()
        return self.actions.show()

    def refresh_configs(self) -> None:
        """Refreshes the configs from which the Pipeline is generated."""
        self.pipes = graph.DAG(nodes=pipe.create_pipes(configs=self._refresh_configs()))


def create_pipeline(config_path: str) -> Pipeline:
    """Creates a pipeline from the definition found in path to its config file."""

    config_registry, refresh_configs = registry.get_registry(config_path=Path(config_path))

    return Pipeline(
        name=(name := utils.get_filename_from_path(path=Path(config_path))),
        schedule=schedule.get_schedule_from_config(config=config_registry[name][0]),
        pipes=pipe.create_pipes(configs=config_registry.configs),
        refresh_configs=refresh_configs,
        config_path=config_path,
    )
