"""Logging and progress bars for socio4health.

All modules log through the ``socio4health`` logger instead of configuring the root logger, so the
package never changes the logging setup of the application that uses it.

- INFO and DEBUG messages go to **stdout**. Jupyter shows stderr in red, so sending informational
  messages there made them look like errors.
- WARNING and above go to **stderr**.
- Progress bars also write to stdout, and messages logged while a bar is running are printed above it
  instead of breaking it.

Change the verbosity with :func:`s4h_set_log_level`, e.g. ``s4h_set_log_level("WARNING")`` to only see
problems, or ``s4h_set_log_level("DEBUG")`` to see every downloaded and extracted file.
"""

import logging
import sys
from contextlib import contextmanager
from typing import Any, Callable, Iterable, Optional, Union

from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm

logger = logging.getLogger("socio4health")


class _MaxLevelFilter(logging.Filter):
    def __init__(self, max_level: int):
        super().__init__()
        self.max_level = max_level

    def filter(self, record: logging.LogRecord) -> bool:
        return record.levelno <= self.max_level


def _configure() -> None:
    if getattr(logger, "_s4h_configured", False):
        return
    formatter = logging.Formatter("%(asctime)s %(levelname)s: %(message)s", datefmt="%H:%M:%S")

    # Look up sys.stdout/sys.stderr on every write: Jupyter replaces them per cell.
    class _StdoutHandler(logging.StreamHandler):
        @property
        def stream(self):
            return sys.stdout

        @stream.setter
        def stream(self, value):
            pass

    class _StderrHandler(logging.StreamHandler):
        @property
        def stream(self):
            return sys.stderr

        @stream.setter
        def stream(self, value):
            pass

    out_handler = _StdoutHandler()
    out_handler.setLevel(logging.DEBUG)
    out_handler.addFilter(_MaxLevelFilter(logging.INFO))
    out_handler.setFormatter(formatter)

    err_handler = _StderrHandler()
    err_handler.setLevel(logging.WARNING)
    err_handler.setFormatter(formatter)

    logger.addHandler(out_handler)
    logger.addHandler(err_handler)
    logger.setLevel(logging.INFO)
    # Our handlers already print the messages; do not print them a second time through the root logger.
    logger.propagate = False
    logger._s4h_configured = True


_configure()


def s4h_set_log_level(level: Union[int, str]) -> None:
    """Set how much socio4health prints.

    Parameters
    ----------
    level : int or str
        A :mod:`logging` level such as ``"DEBUG"`` (every file), ``"INFO"`` (default: one line per step),
        ``"WARNING"`` (only problems) or ``"ERROR"``.
    """
    logger.setLevel(level.upper() if isinstance(level, str) else level)


@contextmanager
def progress_bar(total: Optional[int], desc: str, unit: str = "file"):
    """Progress bar on stdout that prints log messages above the bar instead of through it.

    The bar is hidden when the logger is set above INFO.
    """
    with logging_redirect_tqdm(loggers=[logger]):
        bar = tqdm(
            total=total,
            desc=desc,
            unit=unit,
            file=sys.stdout,
            dynamic_ncols=True,
            disable=not logger.isEnabledFor(logging.INFO),
        )
        try:
            yield bar
        finally:
            bar.close()


def progress(iterable: Iterable, desc: str, total: Optional[int] = None, unit: str = "file",
             label: Optional[Callable[[Any], str]] = None):
    """Iterate with :func:`progress_bar`. ``label(item)`` is shown next to the bar while that item is
    being processed, so long steps (a large archive) do not look frozen."""
    with progress_bar(total, desc, unit) as bar:
        for item in iterable:
            if label is not None:
                bar.set_postfix_str(label(item))
            yield item
            bar.update(1)
        if label is not None:
            bar.set_postfix_str("")
