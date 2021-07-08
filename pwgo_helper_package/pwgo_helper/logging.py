"""container module for custom log formatter class"""
import logging, asyncio
import datetime as dt

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
