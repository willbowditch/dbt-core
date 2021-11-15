from dataclasses import dataclass
from dbt.events.functions import fire_event
from dbt.events.types import (
    AdapterEventDebug, AdapterEventInfo, AdapterEventWarning, AdapterEventError
)
from typing import Any


@dataclass
class AdapterLogger():
    name: str

    def debug(
        self,
        msg: str,
        exc_info: Any = None,
        stack_info: Any = None,
        extra: Any = None
    ):
        event = AdapterEventDebug(self.name, msg)

        event.exc_info = exc_info
        event.stack_info = stack_info
        event.extra = extra

        fire_event(event)

    def info(
        self,
        msg: str,
        exc_info: Any = None,
        stack_info: Any = None,
        extra: Any = None
    ):
        event = AdapterEventInfo(self.name, msg)

        event.exc_info = exc_info
        event.stack_info = stack_info
        event.extra = extra

        fire_event(event)

    def warning(
        self,
        msg: str,
        exc_info: Any = None,
        stack_info: Any = None,
        extra: Any = None
    ):
        event = AdapterEventWarning(self.name, msg)

        event.exc_info = exc_info
        event.stack_info = stack_info
        event.extra = extra

        fire_event(event)

    def error(
        self,
        msg: str,
        exc_info: Any = None,
        stack_info: Any = None,
        extra: Any = None
    ):
        event = AdapterEventError(self.name, msg)

        event.exc_info = exc_info
        event.stack_info = stack_info
        event.extra = extra

        fire_event(event)

    def exception(
        self,
        msg: str,
        exc_info: Any = True,  # this default is what makes this method different
        stack_info: Any = None,
        extra: Any = None
    ):
        event = AdapterEventError(self.name, msg)

        event.exc_info = exc_info
        event.stack_info = stack_info
        event.extra = extra

        fire_event(event)
