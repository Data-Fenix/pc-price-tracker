"""
Centralised logging configuration using structlog.

Usage
-----
    from utils.logger import get_logger
    logger = get_logger(__name__)
    logger.info("scrape_started", source="amazon_de", category="laptops")
"""
from __future__ import annotations

import logging
import logging.handlers
import sys
from pathlib import Path

import structlog

from config import settings


def _configure_stdlib_logging() -> None:
    log_level = getattr(logging, settings.LOG_LEVEL, logging.INFO)

    handlers: list[logging.Handler] = [logging.StreamHandler(sys.stdout)]

    log_path = Path(settings.LOG_FILE)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    file_handler = logging.handlers.RotatingFileHandler(
        log_path, maxBytes=10 * 1024 * 1024, backupCount=5, encoding="utf-8"
    )
    handlers.append(file_handler)

    logging.basicConfig(
        format="%(message)s",
        level=log_level,
        handlers=handlers,
    )


def _configure_structlog() -> None:
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.stdlib.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.dev.ConsoleRenderer() if settings.LOG_LEVEL == "DEBUG"
            else structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(
            getattr(logging, settings.LOG_LEVEL, logging.INFO)
        ),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
    )


# Run once on import
_configure_stdlib_logging()
_configure_structlog()


def get_logger(name: str) -> structlog.BoundLogger:
    """Return a bound structlog logger for *name*."""
    return structlog.get_logger(name)
