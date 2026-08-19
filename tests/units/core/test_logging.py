import logging

from pg2pyrquet.core.logging import get_logger


def test_repeated_calls_do_not_stack_handlers():
    name = "pg2pyrquet-test-repeat"
    logging.getLogger(name).handlers.clear()

    get_logger(name=name)
    logger = get_logger(name=name)

    assert len(logger.handlers) == 1


def test_logger_does_not_propagate_to_the_root():
    name = "pg2pyrquet-test-propagate"
    logging.getLogger(name).handlers.clear()

    logger = get_logger(name=name)

    assert logger.propagate is False
