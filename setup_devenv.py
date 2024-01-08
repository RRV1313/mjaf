import shlex
import subprocess
from collections.abc import Iterable
from pathlib import Path
from typing import Final

ROOT_DIR: Final[Path] = Path(__file__).parent
VENV_DIR: Final[Path] = ROOT_DIR / '.venv'
VENV_PYTHON: Final[Path] = VENV_DIR / 'bin' / 'python'


def create_venv(path: Path = VENV_DIR) -> None:
    create_venv_cmd: Iterable[str] = shlex.split(f'python -m venv {path}')
    subprocess.run(create_venv_cmd, check=True)


def install_main_deps(
    path: Path = VENV_DIR, requirements_file: Path = (ROOT_DIR / 'requirements.txt')
) -> None:
    if not path.exists():
        msg: str = f'.venv/ dir does not exist: {path}'
        raise ValueError(msg)

    venv_pip: Path = VENV_DIR / 'bin' / 'pip'
    install_main_deps_cmd: Iterable[str] = shlex.split(f'{venv_pip} install -r {requirements_file}')
    subprocess.run(install_main_deps_cmd, check=True)


def install_dev_deps(path: Path = VENV_DIR) -> None:
    if not path.exists():
        msg: str = f'.venv/ dir does not exist: {path}'
        raise ValueError(msg)

    venv_pip: Path = VENV_DIR / 'bin' / 'pip'

    cmd: Iterable[str] = shlex.split(
        f'{venv_pip} install pytest ruff== 0.0.291 pep8-naming mypy==1.2.0 coverage pytest-cov pytest-mock pytest-mock-generator'
    )

    subprocess.run(cmd, check=True)


def setup_precommit(path: Path = VENV_DIR) -> None:
    if not path.exists():
        msg: str = f'.venv/ dir does not exist: {path}'
        raise ValueError(msg)

    venv_pip: Path = VENV_DIR / 'bin' / 'pip'
    venv_precommit: Path = VENV_DIR / 'bin' / 'pre-commit'
    install_cmd: Iterable[str] = shlex.split(f'{venv_pip} install pre-commit')
    setup_cmd: Iterable[str] = shlex.split(f'{venv_precommit} install')

    subprocess.run(install_cmd, check=True)
    subprocess.run(setup_cmd, check=True)


if __name__ == '__main__':
    create_venv()
    install_main_deps()
    install_dev_deps()
    setup_precommit()
