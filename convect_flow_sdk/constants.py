from enum import Enum
class RunStatus(Enum):
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    UNKNOWN = "UNKNOWN"
    CANCELLED = "CANCELLED"


class DataType(Enum):
    INPUT = "INPUT"
    OUTPUT = "OUTPUT"


class LangType(Enum):
    EN = "EN"
    ZH = "ZH"