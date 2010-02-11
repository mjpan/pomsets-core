
class PomsetError(Exception):
    pass

class ExecutionError(PomsetError):
    pass

class NodeNotExistError(PomsetError):
    pass

class InvalidValueError(PomsetError):
    pass

