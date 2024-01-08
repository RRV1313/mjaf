from pathlib import Path

from pipelines import utils
from pipelines.builders import base


class MermaidBuilder(base.Builder):
    def to_mermaid(self) -> str:
        return utils.read_template(
            template_path=Path(__file__).parent / 'diagram.jinja2',
            template_fields={
                'pipeline': self,
            },
        )
