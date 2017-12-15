from datetime import datetime
from typing import Any, List, Optional

from virtualgrid.base_node import BaseNode
from virtualgrid.job import Job
from virtualgrid.log import LogEntry
from virtualgrid.messages import (GetStatusMessage, JobStartedMessage, StartJobMessage, StatusMessage,
                                  UnknownMessageError)
from virtualgrid.vector_clock import VectorClock

NODE_STATUSES = ['IDLE', 'BUSY', 'DOWN']


class Node(BaseNode):
    def __init__(self, port: int, clock: VectorClock) -> None:
        super().__init__(clock, port)

        self.job: Optional[Job] = None
        self.job_start_time: Optional[datetime] = None

    @property
    def status(self) -> str:
        """
        Get the current status.

        A node is 'BUSY' when there's a job associated with it,
        and the job's duration is bigger than the difference between
        the current time and the job's start time. Otherwise it's 'IDLE'.
        """

        if self.job and self.job.duration > datetime.now() - self.job_start_time:
            return 'BUSY'
        else:
            return 'IDLE'

    def start_job(self, job: Job) -> None:
        """
        Start a job.

        Starting a job means setting it as a current job, and storing
        the start date and time.

        Caution! After the job is finished, neither `self.job` nor
        `self.job_start_time` are set to None.
        """
        timestamp = self.clock.register_event()
        message = f'Starting job: {job}'
        self._log(timestamp, message)

        self.job = job
        self.job_start_time = datetime.now()

    def _handle_valid_message(self, message: Any) -> Any:
        """
        Handle a valid (already unpickled) message, and return a message
        to send back.

        In case there's no action associated with the message,
        UnknownMessageError is sent back.
        """
        if isinstance(message, StartJobMessage):
            self.clock.receive_message(message.clock)
            self.start_job(message.job)
            timestamp = self.clock.send_message()
            return JobStartedMessage(message.job, timestamp)
        elif isinstance(message, GetStatusMessage):
            self.clock.receive_message(message.clock)
            timestamp = self.clock.send_message()
            return StatusMessage(self.status, timestamp)
        else:
            timestamp = self.clock.register_event()
            return UnknownMessageError(timestamp)

    def _listen_action(self):
        """
        Nodes have no specific listen action.
        """
        return

    def __repr__(self):
        return f"Node(job={self.job}, job_start_time={self.job_start_time}, status={self.status})"
