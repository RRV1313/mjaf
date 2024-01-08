from collections.abc import Iterator
from typing import Any

from pipelines import action, graph, registry
from pipelines.resources import factory


class Pipe:
    """Single unit of a Pipeline. One pipe maps to one config."""

    def __init__(
        self,
        name: str,
        upstream: list[str],
        actions: list[action.Action],
        resource: dict[str, Any],
    ) -> None:
        self.name: str = name
        self.upstream: list[str] = upstream
        self.actions: graph.DAG[action.Action] = graph.DAG(nodes=actions)
        self.resource_name: str = resource['name']
        self.resource_config: dict[str, Any] = {
            k: v for k, v in resource.items() if k != 'name'
        } | {'name': self.name, 'upstream': self.upstream}

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, Pipe) and vars(self) == vars(other)

    def __getattr__(self, attr: str) -> Any:
        return getattr(self.actions, attr)

    @property
    def resource(self) -> Any:
        return factory.ResourceFactory.get_resource(name=self.resource_name)(**self.resource_config)

    def run(
        self,
        action: str | None = None,
        parameters: dict[str, Any] | None = None,
        **kwargs: dict[str, Any],
    ) -> None:
        """Runs user defined actions in topological order using the specified resource."""
        if action:
            self._run_action(action=action, parameters=parameters or {}, **kwargs)
        elif self.actions:
            for node in self.actions:
                self._run_action(action=node.action, parameters=node.parameters, **kwargs)
        else:
            self._run_action(action="default_action", parameters=parameters or {}, **kwargs)

    def _run_action(
        self, action: str, parameters: dict[str, Any], **kwargs: dict[str, Any]
    ) -> None:
        getattr(
            self.resource,
            action,
        )(**parameters, **kwargs)


def create_pipes(configs: Iterator[registry.Config]) -> list[Pipe]:
    """Creates Pipes from configs."""
    return [
        Pipe(
            name=config['name'],
            upstream=config['upstream'],
            actions=action.create_actions(action_configs=config['actions']),
            resource=config['resource'],
        )
        for config in configs
    ]


def combine_pipe_dags(pipes: graph.DAG[Pipe]) -> graph.DAG[action.Action]:
    """Combines an ordered DAG of Pipes into a single DAG containing their actions in topological order."""
    # TODO: this belongs in graph, not pipe
    pipe_map: dict[str, Pipe] = {pipe.name: pipe for pipe in pipes.nodes}
    dag: graph.DAG[action.Action] = graph.DAG(nodes=[])
    for pipe in pipes.nodes:
        dag += pipe.actions
        for upstream in pipe.upstream:
            for root in pipe.roots:
                dag.node_map[root].upstream.extend(pipe_map[upstream].leaves)
    return dag
