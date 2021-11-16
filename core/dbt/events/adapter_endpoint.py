from dataclasses import dataclass
from dbt.events.functions import fire_event
from dbt.events.types import (
    AdapterEventDebug, AdapterEventInfo, AdapterEventWarning, AdapterEventError
)
from typing import Any, Optional


@dataclass
class AdapterLogger():
    name: str

    def debug(self, *args, **kwargs):
        event = AdapterEventDebug(self.name, args, kwargs)

        event.exc_info = or_none(kwargs, 'exc_info')
        event.stack_info = or_none(kwargs, 'stack_info')
        event.extra = or_none(kwargs, 'extra')

        fire_event(event)

    def info(self, *args, **kwargs):
        event = AdapterEventInfo(self.name, args, kwargs)

        event.exc_info = or_none(kwargs, 'exc_info')
        event.stack_info = or_none(kwargs, 'stack_info')
        event.extra = or_none(kwargs, 'extra')

        fire_event(event)

    def warning(self, *args, **kwargs):
        event = AdapterEventWarning(self.name, args, kwargs)

        event.exc_info = or_none(kwargs, 'exc_info')
        event.stack_info = or_none(kwargs, 'stack_info')
        event.extra = or_none(kwargs, 'extra')

        fire_event(event)

    def error(self, *args, **kwargs):
        event = AdapterEventError(self.name, args, kwargs)

        event.exc_info = or_none(kwargs, 'exc_info')
        event.stack_info = or_none(kwargs, 'stack_info')
        event.extra = or_none(kwargs, 'extra')

        fire_event(event)

    def exception(self, *args, **kwargs):
        event = AdapterEventError(self.name, args, kwargs)

        # defaulting exc_info=True if it is empty is what makes this method different
        x = or_none(kwargs, 'exc_info')
        event.exc_info = x if x else True
        event.stack_info = or_none(kwargs, 'stack_info')
        event.extra = or_none(kwargs, 'extra')

        fire_event(event)


def or_none(x: dict, key: str) -> Optional[Any]:
    try:
        return x[key]
    except KeyError:
        return None
