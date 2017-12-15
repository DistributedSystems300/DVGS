from typing import List, NamedTuple, Optional

from virtualgrid.job import Job


class StartJobMessage(NamedTuple):
    job: Job
    clock: List[int]


class JobStartedMessage(NamedTuple):
    job: Job
    clock: List[int]


class GetStatusMessage(NamedTuple):
    clock: List[int]


class StatusMessage(NamedTuple):
    status: str
    clock: List[int]


class GetLoadMessage(NamedTuple):
    clock: List[int]


class LoadMessage(NamedTuple):
    load: int
    clock: List[int]


class JobMessage(NamedTuple):
    job: Job
    rm_id: Optional[int] = None
    clock: Optional[List[int]] = []


class OptionalJobMessage(NamedTuple):
    job: Optional[Job]
    clock: List[int]


class JobAcceptedMessage(NamedTuple):
    clock: List[int]


class JobNotAcceptedMessage(NamedTuple):
    clock: List[int]


class NoRmIdMessage(NamedTuple):
    clock: List[int]


class GiveJobMessage(NamedTuple):
    clock: List[int]


class ScheduleJobsMessage(NamedTuple):
    clock: List[int]


class UnknownMessageError(NamedTuple):
    clock: List[int]
