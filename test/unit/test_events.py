from unittest import mock, TestCase


class TestFlags(TestCase):

    def setUp(self):
        pass

    # this interface is documented for adapter maintainers to plug into
    # so we should test that it at the very least doesn't explode.
    def test_adapter_logging_interface(self):
        from dbt.events import AdapterLogger
        logger = AdapterLogger("dbt_tests")
        logger.debug("debug message")
        logger.info("info message")
        logger.warning("warning message")
        logger.error("error message")
        logger.exception("exception message")
        self.assertTrue(True)
