import dataclasses

from pipelines import registry, utils


@dataclasses.dataclass(frozen=True)
class Schedule:
    cron: str
    start: str
    end: str | None = None


def get_schedule_from_config(config: registry.Config) -> Schedule | None:
    schedule: dict[str, str | None] | None = config.get('schedule', {})
    if not isinstance(schedule, dict):
        return None
    annotations: dict[str, set[type]] = utils.get_generics_from_annotations(
        annotations=registry.Schedule.__annotations__
    )
    if all(key in annotations and type(schedule[key]) in annotations[key] for key in schedule) and (
        schedule['cron'] and schedule['start']
    ):
        return Schedule(cron=schedule['cron'], start=schedule['start'], end=schedule.get('end'))
    else:
        raise ValueError(
            f"Incorrect Schedule specification. Received {schedule}. Must adhere to: {registry.Schedule}."
        )
