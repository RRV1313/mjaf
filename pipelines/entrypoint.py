import argparse
import json
import sys

from pipelines import pipeline


def parse_args(args: list[str]) -> argparse.Namespace:
    parser: argparse.ArgumentParser = argparse.ArgumentParser()
    parser.add_argument("--config_path", type=str)
    parser.add_argument("--pipe", type=str)
    parser.add_argument("--action", type=str)
    parser.add_argument("--parameters", type=json.loads)
    return parser.parse_args(args)


def main(args: argparse.Namespace | None = None) -> None:
    if args is None:
        args: argparse.Namespace = parse_args(args=sys.argv[1:])  # type: ignore[no-redef]
        assert isinstance(args, argparse.Namespace)

    pipeline.create_pipeline(config_path=args.config_path).run(
        pipe=args.pipe, action=args.action, parameters=args.parameters
    )


if __name__ == "__main__":
    main()
