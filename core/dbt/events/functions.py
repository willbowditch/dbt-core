
from dbt.events.history import EVENT_HISTORY
from dbt.events.types import CliEventABC, Event, ShowException
import dbt.logger as logger  # TODO remove references to this logger
import dbt.flags as flags
import logging.config
import os
import structlog
import sys


# these two loggers be set up with CLI inputs via setup_event_logger
# DO NOT IMPORT AND USE THESE DIRECTLY

global STDOUT_LOGGER
STDOUT_LOGGER = structlog.get_logger()

global FILE_LOGGER
FILE_LOGGER = structlog.get_logger()


def setup_event_logger(log_path):
    logger.make_log_dir_if_missing(log_path)
    json: bool = flags.LOG_FORMAT == 'json'
    # USE_COLORS can be None if the app just started and the cli flags
    # havent been applied yet
    colors: bool = True if flags.USE_COLORS else False
    # TODO this default should live somewhere better
    log_dest = os.path.join(logger.LOG_DIR, 'dbt.log')

    # see: https://docs.python.org/3/library/logging.config.html#logging-config-dictschema
    # logging.config.dictConfig({
    #     "version": 1,
    #     "disable_existing_loggers": False,
    #     "formatters": {
    #         "plain": {
    #             "()": structlog.stdlib.ProcessorFormatter,
    #             "processor": structlog.dev.ConsoleRenderer(colors=False),
    #             "foreign_pre_chain": pre_chain,
    #         },
    #         "colored": {
    #             "()": structlog.stdlib.ProcessorFormatter,
    #             "processor": structlog.dev.ConsoleRenderer(colors=True),
    #             "foreign_pre_chain": pre_chain,
    #         },
    #         "json": {
    #             "()": structlog.stdlib.ProcessorFormatter,
    #             "processor": structlog.processors.JSONRenderer(),
    #             "foreign_pre_chain": pre_chain,
    #         },
    #     },
    #     "handlers": {
    #         "console": {
    #             "level": "DEBUG",
    #             "class": "logging.StreamHandler",
    #             "formatter": "colored",
    #         },
    #         "file": {
    #             "level": "DEBUG",
    #             "class": "logging.handlers.WatchedFileHandler",
    #             # TODO this default should live somewhere better
    #             "filename": os.path.join(logger.LOG_DIR, 'dbt.log'),
    #             "formatter": "plain",
    #         },
    #         "json-console": {
    #             "level": "DEBUG",
    #             "class": "logging.StreamHandler",
    #             "formatter": "json",
    #         },
    #         "json-file": {
    #             "level": "DEBUG",
    #             "class": "logging.handlers.WatchedFileHandler",
    #             # TODO this default should live somewhere better
    #             "filename": os.path.join(logger.LOG_DIR, 'dbt.log.json'),
    #             "formatter": "json",
    #         },
    #     },
    #     "loggers": {
    #         "": {
    #             "handlers": ["json-console", "json-file"] if json else ["console", "file"],
    #             "level": "DEBUG" if flags.DEBUG else "INFO",
    #             "propagate": True,
    #         },
    #     }
    # })

    # set-up global logging configurations
    structlog.configure(
        wrapper_class=structlog.stdlib.BoundLogger,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=False,
    )

    # name common processors
    common_processors = [
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter()
    ]

    breakpoint()
    # configure the stdout logger
    STDOUT_LOGGER = structlog.wrap_logger(
        #logger=structlog.PrintLogger(),
        logger=None,
        processors=common_processors + [
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.UnicodeDecoder(),
            structlog.processors.JSONRenderer()
        ]
    )
    formatter = structlog.stdlib.ProcessorFormatter(
        processor=structlog.dev.ConsoleRenderer(colors=colors),
    )
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)
    STDOUT_LOGGER.addHandler(handler)

    # configure the json file handler
    if json:
        FILE_LOGGER = structlog.wrap_logger(
            #logger=structlog.PrintLogger(),
            logger=None,
            processors=common_processors + [
                structlog.processors.TimeStamper(fmt="iso"),
                structlog.processors.StackInfoRenderer(),
                structlog.processors.format_exc_info,
                structlog.processors.UnicodeDecoder(),
                structlog.processors.JSONRenderer()
            ]
        )

        formatter = structlog.stdlib.ProcessorFormatter(
            processor=structlog.processors.JSONRenderer(),
        )
        handler = logging.handlers.WatchedFileHandler(filename=log_dest)
        handler.setFormatter(formatter)
        FILE_LOGGER.addHandler(handler)

    # configure the plaintext file handler
    else:
        # TODO follow pattern from above ^^
        FILE_LOGGER = structlog.wrap_logger(
            #logger=structlog.PrintLogger(),
            logger=None,
            processors=common_processors + [
                structlog.processors.TimeStamper("%H:%M:%S"),
                structlog.processors.StackInfoRenderer(),
                structlog.processors.format_exc_info,
                structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
            ]
        )

        formatter = structlog.stdlib.ProcessorFormatter(
            processor=structlog.dev.ConsoleRenderer(colors=False),
        )
        handler = logging.handlers.WatchedFileHandler(filename=log_dest)
        handler.setFormatter(formatter)
        FILE_LOGGER.addHandler(handler)


# top-level method for accessing the new eventing system
# this is where all the side effects happen branched by event type
# (i.e. - mutating the event history, printing to stdout, logging
# to files, etc.)
def fire_event(e: Event) -> None:
    EVENT_HISTORY.append(e)
    level_tag = e.level_tag()
    if isinstance(e, CliEventABC):
        log_line: str = e.cli_msg()
        if isinstance(e, ShowException):
            event_dict = {
                'exc_info': e.exc_info,
                'stack_info': e.stack_info,
                'extra': e.extra
            }
            if level_tag == 'test':
                # TODO after implmenting #3977 send to new test level
                STDOUT_LOGGER.debug(log_line, event_dict)
                FILE_LOGGER.debug(log_line, event_dict)
            elif level_tag == 'debug':
                STDOUT_LOGGER.debug(log_line, event_dict)
                FILE_LOGGER.debug(log_line, event_dict)
            elif level_tag == 'info':
                STDOUT_LOGGER.info(log_line, event_dict)
                FILE_LOGGER.info(log_line, event_dict)
            elif level_tag == 'warn':
                STDOUT_LOGGER.warning(log_line, event_dict)
                FILE_LOGGER.warning(log_line, event_dict)
            elif level_tag == 'error':
                STDOUT_LOGGER.error(log_line, event_dict)
                FILE_LOGGER.error(log_line, event_dict)
            else:
                raise AssertionError(
                    f"Event type {type(e).__name__} has unhandled level: {e.level_tag()}"
                )
        # CliEventABC but not ShowException
        else:
            if level_tag == 'test':
                # TODO after implmenting #3977 send to new test level
                STDOUT_LOGGER.debug(log_line)
                FILE_LOGGER.debug(log_line)
            elif level_tag == 'debug':
                STDOUT_LOGGER.debug(log_line)
                FILE_LOGGER.debug(log_line)
            elif level_tag == 'info':
                STDOUT_LOGGER.info(log_line)
                FILE_LOGGER.info(log_line)
            elif level_tag == 'warn':
                STDOUT_LOGGER.warning(log_line)
                FILE_LOGGER.warning(log_line)
            elif level_tag == 'error':
                STDOUT_LOGGER.error(log_line)
                FILE_LOGGER.error(log_line)
            else:
                raise AssertionError(
                    f"Event type {type(e).__name__} has unhandled level: {e.level_tag()}"
                )
