class Builder:
    ...


class BuilderMixin:
    def __init__(self) -> None:
        self.include_subclass_methods()

    def include_subclass_methods(self):
        for subclass in Builder.__subclasses__():
            for key, value in subclass.__dict__.items():
                if callable(value) and not hasattr(self.__class__, key):
                    setattr(self.__class__, key, value)
