
class PomsetError(Exception):
    pass

class ExecutionError(PomsetError):
    pass

class UserStoppedExecution(ExecutionError):
    pass

class UserPausedExecution(ExecutionError):
    pass

class NodeNotExistError(PomsetError):
    pass

class InvalidValueError(PomsetError):
    pass

