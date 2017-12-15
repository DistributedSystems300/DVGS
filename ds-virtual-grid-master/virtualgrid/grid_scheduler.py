import pickle
import time
from collections import deque
from typing import Any, Deque, Dict, Optional, Tuple, NamedTuple, List

import zmq

from virtualgrid.base_node import BaseNode
from virtualgrid.job import Job
from virtualgrid.log import LogEntry
from virtualgrid.messages import (JobNotAcceptedMessage, GetLoadMessage, JobAcceptedMessage, LoadMessage, JobMessage,
                                  UnknownMessageError, NoRmIdMessage, GiveJobMessage,
                                  OptionalJobMessage)
from virtualgrid.vector_clock import VectorClock


class RmLoad(NamedTuple):
    rm_address: str
    load: int


class GridScheduler(BaseNode):
    def __init__(self,
                 rm_addresses: Dict[int, str],
                 clock: VectorClock,
                 port: Optional[int]
                 ) -> None:
        """
        Initialize a grid scheduler.
        """
        super().__init__(clock, port)

        self.context = zmq.Context()
        self.job_queue: Deque[Tuple[Job, str]] = deque()
        self.rm_addresses = rm_addresses

    def _listen_action(self):
        self._schedule_jobs()

    def _schedule_jobs(self) -> None:
        """
        Sort all of the resource managers based on their current load,
        and try to schedule a job on one of them. We don't consider
        the source resource manager, because we know it can't accept
        the job.

        If submitting a job to any resource manager succeeds, return True.
        False, otherwise.
        """

        if not self.job_queue:
            return

        loads = self._get_rm_loads()

        job, source_rm_address = self.job_queue.popleft()

        for load in loads:
            if load.rm_address == source_rm_address:  # TODO: Maybe a better solution?
                continue

            success = self._submit_job(job, load.rm_address)

            if success and self.job_queue:
                job, source_rm = self.job_queue.popleft()
            elif not success:
                continue
            else:
                break

    def run_rescheduling(self):
        """
        Keep looking for load imbalances among clusters, and try to even them out.
        """
        # Log start of service.
        timestamp = self.clock.register_event()
        message = f"Starting a grid scheduler with rescheduling"
        self._log(timestamp, message)

        while True:
            self._reschedule_job()
            time.sleep(3)

    def _reschedule_job(self):
        """
        Find the most occupied cluster, take a job from it,
        and schedule it on the least occupied cluster.
        """

        loads = self._get_rm_loads(reverse=True)

        # We want to reschedule jobs only if the most occupied RM
        # has a higher load than the least occupied one
        if loads[0].load > loads[-1].load:
            job = self._take_job(loads[0].rm_address)

            if job is None:
                timestamp = self.clock.register_event()
                message = 'Tried rescheduling, but got no job :('
                self._log(timestamp, message)
                return

            timestamp = self.clock.register_event()
            message = f'Rescheduling a job ID={job.id} from {loads[0].rm_address} to {loads[-1].rm_address}'
            self._log(timestamp, message)
            self._submit_job(job, loads[-1].rm_address)

    def _handle_valid_message(self, message: Any) -> Any:
        """
        Handle a valid (already unpickled) message, and return a message
        to send back.

        In case there's no action associated with the message,
        UnknownMessageError is sent back.
        """

        if isinstance(message, JobMessage):

            if message.rm_id is None:
                timestamp = self.clock.receive_message(message.clock)
                log_message = f"Received message: {message} with no RM ID"
                self._log(timestamp, log_message)

                timestamp = self.clock.send_message()
                return NoRmIdMessage(timestamp)
            else:
                timestamp = self.clock.receive_message(message.clock)
                log_message = f"Accepting job: {message.job} from RM: {message.rm_id}"
                self._log(timestamp, log_message)

                self._add_job(message.job, message.rm_id)
                timestamp = self.clock.send_message()
                return JobAcceptedMessage(timestamp)
        else:
            timestamp = self.clock.register_event()
            log_message = f"Received unknown message {message}"
            self._log(timestamp, log_message)

            timestamp = self.clock.send_message()
            return UnknownMessageError(timestamp)

    def _add_job(self, job: Job, rm_id: int) -> None:
        """
        Add a job to queue, because the source resource manager couldn't handle it.
        """
        self.job_queue.append((job, rm_id))

    def _get_rm_loads(self, reverse: bool = False) -> List[RmLoad]:
        """
        Query all of the resource managers for their loads,
        and return a sorted list of resource manager addresses with their loads.

        All of the resource managers that don't reply with a valid
        message are not take into account.
        """

        loads = []

        for rm_id, rm_address in self.rm_addresses.items():
            load = self._get_rm_load(rm_address)

            if load is not None:
                loads.append(RmLoad(rm_address, load))

        return sorted(loads, key=lambda e: e.load, reverse=reverse)

    def _get_rm_load(self, rm_address: str) -> Optional[int]:
        """
        Query one resource manager for its load.

        In case no valid message is received, None is returned.
        """
        socket = self.context.socket(zmq.REQ)
        socket.connect(f'tcp://{rm_address}')

        # TODO: Handle unpickling errors
        timestamp = self.clock.send_message()
        socket.send_pyobj(GetLoadMessage(timestamp))
        response = socket.recv_pyobj()

        if isinstance(response, LoadMessage):
            self.clock.receive_message(response.clock)
            return response.load
        else:
            timestamp = self.clock.register_event()
            message = f"Received: {response} (expected: {LoadMessage.__name__})"
            self._log(timestamp, message)
            return None

    def _submit_job(self, job: Job, rm_address: str) -> bool:
        """
        Submit a job to a resource manager.

        Returns True if submitting a job was successful, False otherwise.
        """

        socket = self.context.socket(zmq.REQ)
        socket.connect(f'tcp://{rm_address}')

        timestamp = self.clock.send_message()
        message = f"Sending job: {job} to {rm_address}"
        self._log(timestamp, message)

        socket.send_pyobj(JobMessage(job, clock=timestamp))
        response = socket.recv_pyobj()

        if isinstance(response, JobAcceptedMessage):
            timestamp = self.clock.receive_message(response.clock)
            message = f"RM {rm_address} accepted job {job}"
            self._log(timestamp, message)
            return True
        elif isinstance(response, JobNotAcceptedMessage):
            timestamp = self.clock.receive_message(response.clock)
            message = f"RM {rm_address} declinded job {job}"
            self._log(timestamp, message)
            return False
        else:
            timestamp = self.clock.register_event()
            message = f"Received: {response} (expected: {JobAcceptedMessage.__name__} or " \
                      f"{JobNotAcceptedMessage.__name__})"
            self._log(timestamp, message)
            return False

    def _take_job(self, rm_address: str) -> Optional[Job]:
        """
        Take one job from a resource manager's queue.
        """

        socket = self.context.socket(zmq.REQ)
        socket.connect(f'tcp://{rm_address}')

        timestamp = self.clock.send_message()
        message = f"Taking job from {rm_address}"
        self._log(timestamp, message)

        socket.send_pyobj(GiveJobMessage(timestamp))
        response = socket.recv_pyobj()

        if isinstance(response, OptionalJobMessage):
            timestamp = self.clock.receive_message(response.clock)
            message = f"Received {response} from {rm_address}"
            self._log(timestamp, message)
            return response.job
        else:
            print(f"Received: {response} (expected: {OptionalJobMessage.__name__}")
            return None

    def __repr__(self):
        return f"GridScheduler(queue_size={len(self.job_queue)}, " \
               f"rm_loads={[load.load for load in self._get_rm_loads()]})"
