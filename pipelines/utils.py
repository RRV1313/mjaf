"Module containing utility functions."
import logging
import re
from collections.abc import Callable
from pathlib import Path
from typing import Any, NoReturn

import jinja2
import yaml


def read_yaml(file_path: Path) -> list[dict[str, Any]]:
    """Function for reading yaml files.

    Args:
        file_path (str): path to yaml file

    Returns:
        (str): parsed yaml file
    """
    with file_path.open() as f:
        return list(yaml.safe_load_all(stream=f))


def get_filename_from_path(path: Path) -> str:
    return path.name.replace(path.suffix, '')


def read_template(template_path: Path, template_fields: dict[str, Any] | None = None) -> str:
    """Function for reading and formatting a jinja template.

    Args:
        template_path (list[str]): path to the template
        template_fields (Optional[dict], optional): fields to template. Defaults to None.

    Returns:
        (str): formatted template
    """
    with template_path.open() as file:
        template: str = jinja2.Template(source=file.read()).render(**(template_fields or {}))
    return template


def get_generics_from_annotations(annotations: dict[str, type]):
    generics: dict[str, set[type]] = {}

    def _get_generic_from_annotation(annotation: type):
        if hasattr(annotation, '__origin__'):
            return {annotation.__origin__}
        elif hasattr(annotation, '__args__'):
            _annotations = set()
            for arg in annotation.__args__:
                for _annotation in _get_generic_from_annotation(annotation=arg):
                    _annotations.add(_annotation)
            return _annotations

        else:
            return {annotation}

    for name, annotation in annotations.items():
        generics[name] = _get_generic_from_annotation(annotation=annotation)

    return generics


def get_logger(name: str):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    if not logger.hasHandlers():
        # define formatter
        formatter = logging.Formatter("%(name)s - %(levelname)s - %(message)s")

        # define file handler
        file_handler = logging.FileHandler(
            f"{name}.log",
            encoding="utf-8",
            mode="w",
        )
        log_handler(file_handler, formatter, logger)
        # define stream handler
        stream_handler = logging.StreamHandler()
        log_handler(stream_handler, formatter, logger)
    return logger


def log_handler(handler, formatter, logger):
    handler.setLevel(logging.INFO)
    handler.setFormatter(formatter)
    logger.addHandler(handler)


logger: logging.Logger = get_logger("utils")


def update_markdown_section(
    path: Path,
    section_header: str,
    content: str | list[str],
    code_block_syntax: str | None = None,
) -> None:
    markdown: str = load_file(path=path)
    section_content: list[str] = _get_section_content(
        markdown=markdown,
        section_header=section_header,
    )
    processed_content: str = _preprocess_content(
        content=content, code_block_syntax=code_block_syntax
    )
    if not section_content:
        _create_markdown_section(
            path=path, section_header=section_header, content=processed_content
        )
    elif section_content != processed_content:
        _replace_markdown_section(
            path=path,
            section_header=section_header,
            markdown=markdown,
            replacement_content=processed_content,
        )
    else:
        logger.info(f"No updates required for {path.name}.")


def _preprocess_content(content: str | list[str], code_block_syntax: str | None) -> str:
    template: str = (
        f"```{code_block_syntax}\n{{content}}\n```" if code_block_syntax else f"{content}"
    )
    match content:
        case str():
            return template.format(content=content)
        case list():
            return '\n'.join([template.format(content=item) for item in content])
        case _:
            raise ValueError("Incorrect content specification. Must be of type: str or list[str].")


def _create_markdown_section(path: Path, section_header: str, content: str) -> None:
    section: str = f"{section_header}\n{content}\n"
    with path.open(mode='a') as file:
        file.write(section)


def _replace_markdown_section(
    path: Path, markdown: str, section_header: str, replacement_content: str
) -> None:
    pattern: str = _get_section_pattern(section_header=section_header)
    replacement_pattern: str = rf'{section_header}\n{replacement_content}\n'
    with path.open(mode='w') as file:
        file.write(re.sub(pattern=pattern, repl=replacement_pattern, string=markdown))


def load_file(path: Path) -> str:
    with path.open(mode="r") as file:
        return file.read()


def _get_section_content(markdown: str, section_header: str) -> list[str]:
    return re.findall(pattern=_get_section_pattern(section_header=section_header), string=markdown)


def _get_section_pattern(section_header: str) -> str:
    return f"^{section_header}\n(.*(?:\n(?!#).*)*)"


def log_return(func: Callable[..., Any]) -> Callable[..., Any]:
    def wrapper(*args, **kwargs) -> Any:
        value: Any = func(*args, **kwargs)
        logger: logging.Logger = get_logger(name=func.__name__)
        logger.info(f'{func.__name__}: {value}')
        return value

    return wrapper


class LazyProperty:
    def __init__(self, function: Callable[..., Any]) -> None:
        self.function: Callable[..., Any] = function
        self.name: str = f"_{function.__name__}"

    def __get__(self, instance: Any | None, owner: type[Any]) -> Any:
        if instance is None:
            return self
        value = instance.__dict__.get(self.name)
        if value is None:
            value = self.function(instance)
            if self.name not in ('_schema', '_client'):
                logger = get_logger(self.__class__.__name__)
                logger.info(f'{self.name}: {value}')
            instance.__dict__[self.name] = value
        return value

    def __set__(self, instance: Any | None, owner: type[Any]) -> NoReturn:
        raise AttributeError("Cannot set attribute.")
