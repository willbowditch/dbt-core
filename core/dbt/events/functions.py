
from dbt.events.history import EVENT_HISTORY
from dbt.events.types import CliEventABC, Event, ShowException
import dbt.logger as logger  # TODO remove references to this logger
import dbt.flags as flags
import logging.config
import os
import structlog

# will be set up with CLI inputs via setup_event_logger
# DO NOT IMPORT AND USE THIS DIRECTLY
global LOG
LOG = structlog.get_logger()


def setup_event_logger(log_path):
    logger.make_log_dir_if_missing(log_path)
    json: bool = flags.LOG_FORMAT == 'json'
    timestamper = structlog.processors.TimeStamper("%H:%M:%S")
    # for events not from structlog
    pre_chain = [
        structlog.stdlib.add_log_level,
        timestamper,
    ]

    print(f"log format: {flags.LOG_FORMAT}")

    # see: https://docs.python.org/3/library/logging.config.html#logging-config-dictschema
    logging.config.dictConfig({
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "plain": {
                "()": structlog.stdlib.ProcessorFormatter,
                "processor": structlog.dev.ConsoleRenderer(colors=False),
                "foreign_pre_chain": pre_chain,
            },
            "colored": {
                "()": structlog.stdlib.ProcessorFormatter,
                "processor": structlog.dev.ConsoleRenderer(colors=True),
                "foreign_pre_chain": pre_chain,
            },
            "json": {
                "()": structlog.stdlib.ProcessorFormatter,
                "processor": structlog.processors.JSONRenderer,
                "foreign_pre_chain": pre_chain,
            },
        },
        "handlers": {
            "console": {
                "level": "DEBUG",
                "class": "logging.StreamHandler",
                "formatter": "colored",
            },
            "file": {
                "level": "DEBUG",
                "class": "logging.handlers.WatchedFileHandler",
                # TODO this default should live somewhere better
                "filename": os.path.join(logger.LOG_DIR, 'dbt.log'),
                "formatter": "plain",
            },
            "json-console": {
                "level": "DEBUG",
                "class": "logging.StreamHandler",
                "formatter": "json",
            },
            "json-file": {
                "level": "DEBUG",
                "class": "logging.handlers.WatchedFileHandler",
                # TODO this default should live somewhere better
                "filename": os.path.join(logger.LOG_DIR, 'dbt.log.json'),
                "formatter": "json",
            },
        },
        "loggers": {
            "": {
                "handlers": ["json-console", "json-file"] if json else ["console", "file"],
                "level": "DEBUG" if flags.DEBUG else "INFO",
                "propagate": True,
            },
        }
    })

    if json:
        structlog.configure(
            processors=[
                structlog.stdlib.filter_by_level,
                structlog.stdlib.add_logger_name,
                structlog.stdlib.add_log_level,
                structlog.stdlib.PositionalArgumentsFormatter(),
                structlog.processors.TimeStamper(fmt="iso"),
                structlog.processors.StackInfoRenderer(),
                structlog.processors.format_exc_info,
                structlog.processors.UnicodeDecoder(),
                structlog.processors.JSONRenderer()
            ],
            wrapper_class=structlog.stdlib.BoundLogger,
            logger_factory=structlog.stdlib.LoggerFactory(),
            cache_logger_on_first_use=True,
        )
    else:
        structlog.configure(
            processors=[
                structlog.stdlib.add_log_level,
                structlog.stdlib.PositionalArgumentsFormatter(),
                timestamper,
                structlog.processors.StackInfoRenderer(),
                structlog.processors.format_exc_info,
                structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
            ],
            logger_factory=structlog.stdlib.LoggerFactory(),
            wrapper_class=structlog.stdlib.BoundLogger,
            cache_logger_on_first_use=True,
        )

    LOG = structlog.get_logger()  # noqa: F841


# top-level method for accessing the new eventing system
# this is where all the side effects happen branched by event type
# (i.e. - mutating the event history, printing to stdout, logging
# to files, etc.)
def fire_event(e: Event) -> None:
    EVENT_HISTORY.append(e)
    level_tag = e.level_tag()
    if isinstance(e, CliEventABC):
        log_line = e.cli_msg()
        if isinstance(e, ShowException):
            event_dict = {
                'exc_info': e.exc_info,
                'stack_info': e.stack_info,
                'extra': e.extra
            }
            if level_tag == 'test':
                # TODO after implmenting #3977 send to new test level
                LOG.debug(log_line, event_dict)
            elif level_tag == 'debug':
                LOG.debug(log_line, event_dict)
            elif level_tag == 'info':
                LOG.info(log_line, event_dict)
            elif level_tag == 'warn':
                LOG.warning(log_line, event_dict)
            elif level_tag == 'error':
                LOG.error(log_line, event_dict)
            else:
                raise AssertionError(
                    f"Event type {type(e).__name__} has unhandled level: {e.level_tag()}"
                )
        # CliEventABC but not ShowException
        else:
            if level_tag == 'test':
                # TODO after implmenting #3977 send to new test level
                LOG.debug(log_line)
            elif level_tag == 'debug':
                LOG.debug(log_line)
            elif level_tag == 'info':
                LOG.info(log_line)
            elif level_tag == 'warn':
                LOG.warning(log_line)
            elif level_tag == 'error':
                LOG.error(log_line)
            else:
                raise AssertionError(
                    f"Event type {type(e).__name__} has unhandled level: {e.level_tag()}"
                )
