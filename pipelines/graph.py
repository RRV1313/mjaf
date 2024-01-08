import itertools
from collections.abc import Iterator
from typing import Any, Generic, Protocol, TypeVar

import networkx as nx
from dagviz import dagre
from typing_extensions import Self


class Node(Protocol):
    name: str
    upstream: list[str]


NodeType = TypeVar('NodeType', bound=Node)


class DAG(Generic[NodeType]):
    """DAG Class."""

    def __init__(self, nodes: list[NodeType]) -> None:
        self.nodes: list[NodeType] = nodes

    def __getitem__(self, key: str) -> NodeType:
        return self.node_map[key]

    def __iter__(self) -> Iterator[NodeType]:
        for node in nx.topological_sort(G=self.graph):
            yield self.node_map[node]

    def __iadd__(self, other: Any) -> Self:
        if not isinstance(other, DAG):
            raise ValueError(
                f'DAG objects can only be added to other DAG objects. Received: {type(other)}'
            )
        self.nodes += other.nodes
        return self

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, DAG):
            return self.nodes == other.nodes and self.graph.edges == other.graph.edges
        else:
            return False

    @property
    def graph(self) -> nx.DiGraph:
        """Internal networkx DiGraph."""

        if not hasattr(self, '_graph'):
            self._graph = nx.DiGraph()
            for node in self.nodes:
                self._graph.add_node(node_for_adding=node.name)
                if node.upstream:
                    self._graph.add_edges_from(
                        ebunch_to_add=set(zip(node.upstream, itertools.repeat(object=node.name)))
                    )
        return self._graph

    @property
    def roots(self) -> list[str]:
        """Gets roots of DAG."""
        return [node for node in self.graph.nodes() if self.graph.in_degree(nbunch=node) == 0]

    @property
    def edges(self) -> list[str]:
        return sorted(self.graph.edges)

    @property
    def leaves(self) -> list[str]:
        """Gets leaf nodes of DAG."""
        return [node for node in self.graph.nodes() if self.graph.out_degree(nbunch=node) == 0]

    @property
    def node_map(self) -> dict[str, NodeType]:
        return {node.name: node for node in self.nodes}

    def show(self) -> dagre.Dagre:
        """Displays graphical representation of DAG in a Jupyter Notebook."""
        return dagre.Dagre(G=self.graph)
