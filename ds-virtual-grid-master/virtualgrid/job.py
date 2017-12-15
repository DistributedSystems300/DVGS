from datetime import timedelta
from typing import NamedTuple


class Job(NamedTuple):
    id: int
    duration: timedelta
