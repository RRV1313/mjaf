from algo_features import environment
from pipelines import pipeline, utils

diagrams: list[str] = [
    f'```mermaid\n{pipeline.create_pipeline(config_path=str(file.absolute())).to_mermaid()}\n```'
    for file in environment.CONFIG_DIR.glob(pattern="*.yaml")
]


def main() -> None:
    utils.update_markdown_section(
        path=environment.CONFIG_DIR / 'README.md',
        section_header="## Pipelines Currently Managed by algo features",
        content='\n'.join(diagrams),
    )


if __name__ == '__main__':
    main()
