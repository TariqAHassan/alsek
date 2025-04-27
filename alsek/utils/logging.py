"""

    Logging

"""

import inspect
import logging
from functools import wraps
from typing import Any, Callable

LOGGING_FORMAT: str = (
    "[%(asctime)s.%(msecs)03d] [%(processName)s] [%(threadName)s] "
    "[%(levelname)s] %(name)s: %(message)s"
)
LOGGING_DATEFMT: str = "%Y-%m-%d %H:%M:%S"


def get_logger() -> logging.Logger:
    """Get the Alsek logger.

    Returns:
        logger (logging.Logger): Alsek logger

    """
    return logging.getLogger("alsek")


def setup_logging(level: int) -> None:
    """Setup Alsek-style logging.

    Args:
        level (int): logging level to use

    Returns:
        None

    """
    # First configure the root logger
    root_logger = logging.getLogger()
    
    # Clear existing handlers to avoid duplicates
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
        
    # Add a handler to the root logger
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter(fmt=LOGGING_FORMAT, datefmt=LOGGING_DATEFMT))
    root_logger.addHandler(console_handler)
    root_logger.setLevel(level)
    
    # Configure all alsek loggers to use the root logger's handler
    for name in logging.root.manager.loggerDict:
        if name.startswith('alsek'):
            logger = logging.getLogger(name)
            logger.setLevel(level)
            # Enable propagation to use root logger's handler
            logger.propagate = True
            # Clear any direct handlers to avoid duplicates
            for handler in logger.handlers[:]:
                logger.removeHandler(handler)


def _magic_parser(
    function_raw: Any,
    args_raw: tuple[Any, ...],
    kwargs_raw: dict[str, Any],
) -> tuple[Callable[..., Any], tuple[Any, ...], dict[str, Any]]:
    if hasattr(function_raw, "__func__"):
        return function_raw.__func__, args_raw[1:], kwargs_raw
    else:
        return function_raw, args_raw, kwargs_raw


def _merge_args_kwargs(
    function: Callable[..., Any],
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
) -> dict[str, Any]:
    args_dict = dict(zip(inspect.signature(function).parameters, args))
    return {**args_dict, **kwargs}


def _run_func(function: Callable[..., Any], **kwargs: Any) -> Any:
    function_params = inspect.signature(function).parameters
    return function(**{k: v for k, v in kwargs.items() if k in function_params})


def magic_logger(
    before: Callable[..., Any] = lambda: None,
    after: Callable[..., Any] = lambda: None,
) -> Callable[..., Any]:
    """Logging decorator.

    Args:
        before (callable): function to log a message before function
            execution. This callable will be passed only those parameters
                which it shares with the deocrated function.
        after (callable): function to log a message after function
            execution. This callable will be passed:
                * ``input_``: a dictionary where the key is a parameter accepted
                  by the wrapped function and the value is the value passed. If not
                  present in the signature of ``after`` this data will not be provided.
                * ``output``: the output of the function. If not present in the signature
                  of ``after`` this data will not be provided.

    Returns:
        wrapper (callable): wrapped ``function``

    Examples:
        >>> import logging
        >>> from alsek.utils.logging import magic_logger

        >>> log = logging.getLogger(__name__)

        >>> @magic_logger(
        >>>    before=lambda a: log.debug(a),
        >>>    after=lambda input_, output: log.info(output)
        >>> )
        >>> def add99(a: int) -> int:
        >>>    return a + 99

    """

    def wrapper(function_raw: Callable[..., Any]) -> Callable[..., Any]:
        @wraps(function_raw)
        def inner(*args_raw: Any, **kwargs_raw: Any) -> Any:
            function, args, kwargs = _magic_parser(
                function_raw,
                args_raw=args_raw,
                kwargs_raw=kwargs_raw,
            )
            full_kwargs = _merge_args_kwargs(function, args=args, kwargs=kwargs)
            _run_func(before, **full_kwargs)
            output = function(*args, **kwargs)
            _run_func(after, input_=full_kwargs, output=output)
            return output

        return inner

    return wrapper
