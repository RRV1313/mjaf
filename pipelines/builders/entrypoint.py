import argparse
import logging
import sys

from pipelines import pipeline, utils

LOGGER: logging.Logger = utils.get_logger(name='builder_entrypoint')


def parse_args(args: list[str]) -> argparse.Namespace:
    parser: argparse.ArgumentParser = argparse.ArgumentParser()
    parser.add_argument("--config_path", type=str)
    parser.add_argument("--builder", type=str)
    return parser.parse_args(args)


def main(args: argparse.Namespace | None = None) -> None:
    if args is None:
        args: argparse.Namespace = parse_args(args=sys.argv[1:])  # type: ignore[no-redef]
        assert isinstance(args, argparse.Namespace)
    LOGGER.info(f'Building from {args.config_path}')
    print(getattr(pipeline.create_pipeline(config_path=args.config_path), f'to_{args.builder}')())


if __name__ == "__main__":
    main()
