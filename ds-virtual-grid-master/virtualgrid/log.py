from typing import NamedTuple, Any, Deque, Dict, List, Optional

class LogEntry(NamedTuple):
    timestamp: List[int]
    node: str
    message: str
