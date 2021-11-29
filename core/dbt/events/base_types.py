from abc import ABCMeta, abstractmethod, abstractproperty
from dataclasses import dataclass
from datetime import datetime
import os
from typing import Any, Optional

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# These base types define the _required structure_ for the concrete event #
# types defined in types.py                                               #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #


# in preparation for #3977
class TestLevel():
    def level_tag(self) -> str:
        return "test"


class DebugLevel():
    def level_tag(self) -> str:
        return "debug"


class InfoLevel():
    def level_tag(self) -> str:
        return "info"


class WarnLevel():
    def level_tag(self) -> str:
        return "warn"


class ErrorLevel():
    def level_tag(self) -> str:
        return "error"


@dataclass
class Node():
    node_path: str
    node_name: str
    unique_id: str
    resource_type: str
    materialized: str
    node_status: str
    node_started_at: str
    node_finished_at: str
    type: str = 'node_status'


@dataclass
class ShowException():
    # N.B.:
    # As long as we stick with the current convention of setting the member vars in the
    # `message` method of subclasses, this is a safe operation.
    # If that ever changes we'll want to reassess.
    def __post_init__(self):
        self.exc_info: Any = True
        self.stack_info: Any = None
        self.extra: Any = None


# TODO add exhaustiveness checking for subclasses
# can't use ABCs with @dataclass because of https://github.com/python/mypy/issues/5374
# top-level superclass for all events
class Event(metaclass=ABCMeta):
    # fields that should be on all events with their default implementations
    log_version: int = 1
    ts: Optional[datetime] = None  # use getter for non-optional
    pid: Optional[int] = None  # use getter for non-optional
    node_info: Optional[Node]

    # four digit string code that uniquely identifies this type of event
    # uniqueness and valid characters are enforced by tests
    @abstractproperty
    @staticmethod
    def code() -> str:
        raise Exception("code() not implemented for event")

    # do not define this yourself. inherit it from one of the above level types.
    @abstractmethod
    def level_tag(self) -> str:
        raise Exception("level_tag not implemented for Event")

    # Solely the human readable message. Timestamps and formatting will be added by the logger.
    # Must override yourself
    @abstractmethod
    def message(self) -> str:
        raise Exception("msg not implemented for Event")

    # exactly one time stamp per concrete event
    def get_ts(self) -> datetime:
        if not self.ts:
            self.ts = datetime.now()
        return self.ts

    # exactly one pid per concrete event
    def get_pid(self) -> int:
        if not self.pid:
            self.pid = os.getpid()
        return self.pid

    @classmethod
    def get_invocation_id(cls) -> str:
        from dbt.events.functions import get_invocation_id
        return get_invocation_id()

    def get_node_info(self):
        return None


@dataclass  # type: ignore
class NodeInfo(Event, metaclass=ABCMeta):
    report_node_data: Any  # Union[ParsedModelNode, ...] TODO: resolve circular imports
    node_status: str
    # node_started_at: datetime

    def get_node_info(self):
        node_info = Node(
            node_path=self.report_node_data.path,
            node_name=self.report_node_data.name,
            unique_id=self.report_node_data.unique_id,
            resource_type=self.report_node_data.resource_type,
            materialized=self.report_node_data.config.materialized,
            node_status=self.node_status,
            node_started_at="TODO",  # TODO: where to fill?  leverage/convert TimestampNamed
                                     # in logger.py?
            node_finished_at="TODO"
        )
        return vars(node_info)  # TODO: can just return node_info after #4326 is merged in


class File(Event, metaclass=ABCMeta):
    # Solely the human readable message. Timestamps and formatting will be added by the logger.
    def file_msg(self) -> str:
        # returns the event msg unless overriden in the concrete class
        return self.message()


class Cli(Event, metaclass=ABCMeta):
    # Solely the human readable message. Timestamps and formatting will be added by the logger.
    def cli_msg(self) -> str:
        # returns the event msg unless overriden in the concrete class
        return self.message()
