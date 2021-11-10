from dbt.events import AdapterLogger
from dbt.events.base_types import Cli, File
import inspect
from unittest import TestCase


class TestAdapterLogger(TestCase):

    def setUp(self):
        pass

    # this interface is documented for adapter maintainers to plug into
    # so we should test that it at the very least doesn't explode.
    def test_adapter_logging_interface(self):
        logger = AdapterLogger("dbt_tests")
        logger.debug("debug message")
        logger.info("info message")
        logger.warning("warning message")
        logger.error("error message")
        logger.exception("exception message")
        self.assertTrue(True)


class TestEventCodes(TestCase):

    def setUp(self):
        pass

    # this interface is documented for adapter maintainers to plug into
    # so we should test that it at the very least doesn't explode.
    def test_event_codes(self):
        all_concrete = set(Cli.__subclasses__()) \
            .union(set(File.__subclasses__()))
        all_codes = {}

        for event in all_concrete:
            if not inspect.isabstract(event):
                # must be in the form 1 capital letter, 3 digits
                self.assertTrue('^[A-Z][0-9]{3}', event.code())
                # cannot have been used already
                self.assertFalse(event.code() in all_codes)
                all_codes.add(event.code())
