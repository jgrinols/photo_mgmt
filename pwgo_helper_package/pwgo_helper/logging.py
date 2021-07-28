"""container module for custom log formatter class"""
import logging, asyncio, datetime as dt

from slack_logger import SlackHandler

# pylint: disable=invalid-name
_log_level = "NOTSET"
_lib_log_level = "ERROR"

def __getattr__(name):
    if name == "log_level":
        return _log_level
    if name == "lib_log_level":
        return _lib_log_level

    raise AttributeError(name)

def __setatttr__(name, value):
    if name == "log_level":
        _log_level = value
        # pylint: disable=no-member
        log_dict = logging.root.manager.loggerDict
        pwgo_lgrs = [logging.getLogger(nm) for nm in log_dict if nm.startsWith("pwgo_helper")]
        for logger in pwgo_lgrs:
            logger.setLevel(_log_level)

    if name == "lib_log_level":
        _lib_log_level = value
        # pylint: disable=no-member
        log_dict = logging.root.manager.loggerDict
        lib_lgrs = [logging.getLogger(nm) for nm in log_dict if not nm.startsWith("pwgo_helper")]
        for logger in lib_lgrs:
            logger.setLevel(_lib_log_level)

    raise AttributeError(name)

def _initialize():
    root_logger = logging.getLogger()
    root_logger.setLevel("NOTSET")
    console_hndlr = logging.StreamHandler()
    console_hndlr.setLevel("NOTSET")
    console_hndlr.addFilter(TaskFilter())
    console_hndlr.setFormatter(
        CustomFormatter("%(asctime)s - %(levelname)s - %(taskname)s: %(message)s", datefmt='%Y-%m-%d %H:%M:%S.%f')
    )
    root_logger.addHandler(console_hndlr)

def attach_alert_handler(url):
    """adds a slack error alert handler"""
    root_logger = logging.getLogger()
    alert_hndlr = SlackHandler(url)
    alert_hndlr.setLevel("ERROR")
    # pylint: disable=line-too-long
    alert_hndlr.setFormatter(CustomFormatter(
            '%(levelname)s - logger %(name)s generated message from [%(module)s].[%(funcName)s] (%(lineno)s) at %(asctime)s',
            datefmt='%Y-%m-%d %H:%M:%S.%f'
        ))
    root_logger.addHandler(alert_hndlr)

class CustomLogger(logging.Logger):
    """implements custom logger instantiation logic"""
    _logger = logging.getLogger(__name__)

    def __init__(self, name: str) -> None:
        if name.startswith("pwgo_helper"):
            level = __getattr__("log_level")
            pass
        else:
            level = __getattr__("lib_log_level")
            pass
        CustomLogger._logger.debug("initializing logger %s with level %s", name, level)
        super().__init__(name, level=level)

class CustomFormatter(logging.Formatter):
    """custom log formatter"""
    converter=dt.datetime.fromtimestamp

    def formatTime(self, record, datefmt=None):
        ct = self.converter(record.created)
        if datefmt:
            s = ct.strftime(datefmt)
        else:
            t = ct.strftime("%Y-%m-%d %H:%M:%S")
            s = "%s,%03d" % (t, record.msecs)
        return s

class TaskFilter(logging.Filter):
    """adds the current (if any) asyncio task name as an available formatter field"""
    def filter(self, record) -> bool:
        record.taskname = "default"
        curr_task = None
        try:
            loop = asyncio.get_running_loop()
            curr_task = asyncio.current_task(loop)
            if curr_task:
            # this attribute should exist if task was built
            # with the custom task factory in the custom asyncio module
                if hasattr(curr_task, "qualified_name"):
                    record.taskname = curr_task.qualified_name()
                else:
                    record.taskname = curr_task.get_name()
        except RuntimeError:
            pass

        return True

_initialize()
