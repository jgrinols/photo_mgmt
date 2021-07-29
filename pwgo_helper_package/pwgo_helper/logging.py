"""container module for custom log formatter class"""
import logging, asyncio, datetime as dt

from slack_logger import SlackHandler

# pylint: disable=invalid-name
_user_log_level = "NOTSET"
_lib_log_level = "ERROR"

def set_log_level(level):
    """sets the logging level for loggers created from user code"""
    # pylint: disable=global-statement
    global _user_log_level
    _logger = logging.getLogger(__name__)
    _logger.debug("setting user log level to %s", level)
    _user_log_level = level
    # pylint: disable=no-member
    log_dict = logging.root.manager.loggerDict
    for logger_nm in [nm for nm in log_dict if nm.startswith("pwgo_helper")]:
        _logger.debug("setting log level on logger %s to %s", logger_nm, _user_log_level)
        logger = logging.getLogger(logger_nm)
        logger.setLevel(_user_log_level)

def set_lib_log_level(level):
    """sets the logging level for loggers created from
    third party or std lib code"""
    # pylint: disable=global-statement
    global _lib_log_level
    _logger = logging.getLogger(__name__)
    _logger.debug("setting lib log level to %s", level)
    _lib_log_level = level
    # pylint: disable=no-member
    log_dict = logging.root.manager.loggerDict
    for logger_nm in [nm for nm in log_dict if not nm.startswith("pwgo_helper")]:
        _logger.debug("setting log level on logger %s to %s", logger_nm, _lib_log_level)
        logger = logging.getLogger(logger_nm)
        logger.setLevel(_lib_log_level)

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
            level = _user_log_level
        else:
            level = _lib_log_level
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
