from dataclasses import dataclass

from pipelines import utils

logger = utils.get_logger(name="resources")


class ResourceFactory:
    """Factory for registering and retrieving resources"""

    registry: dict[str, type] = {}

    @classmethod
    def register(cls, resource: type, overwrite: bool = False) -> None:
        """Registers a class to the internal registry.

        Args:
            wrapped_resource (_type_): The class to insert
            overwrite (bool, optional): Whether or not the resource should be overwritten. Defaults to False.

        Raises:
            ValueError: Raises if overwrite is False and resource already exists in the registry.
        """
        if cls.registry.get(resource.__name__) and not overwrite:
            logger.warning(
                f'{resource.__name__} already exists in the registry and is being overwritten.'
            )
        cls.registry[resource.__name__] = resource

    @classmethod
    def get_resource(cls, name: str) -> type:
        """Retrieves a resource from the registry and initializes it using supplied config.

        Raises:
            NotImplementedError: Raises if resources is not found in the registry.

        Returns:
            base.Base: Resource object.
        """

        try:
            return cls.registry[name]
        except KeyError as e:
            raise NotImplementedError(f"Executor: {name} does not exist in the registry") from e


@dataclass
class Resource:
    name: str
    upstream: str

    def __init_subclass__(cls, **kwargs) -> None:
        super().__init_subclass__(**kwargs)
        ResourceFactory.register(resource=cls)
