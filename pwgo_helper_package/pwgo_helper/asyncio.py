"""custom asyncio components"""
import asyncio, types

def get_task(loop, coro):
    """custom asyncio task factory that provides a way to get a fully qualified
    task name when tasks are nested"""
    new_task = asyncio.tasks.Task(coro, loop=loop)
    setattr(new_task, 'parent_task', asyncio.current_task(loop=loop))

    def get_qualified_name(self: asyncio.Task) -> str:
        name = f"[{self.get_name()}]"
        if self.parent_task:
            return f"{self.parent_task.qualified_name()}.{name}"

        return f"{name}"

    # setattr isn't what we want here because the method doesn't get bound
    # when attached directly to an instance so "self" would have to be
    # manually passed when calling the method
    new_task.qualified_name = types.MethodType(get_qualified_name, new_task)
    return new_task
