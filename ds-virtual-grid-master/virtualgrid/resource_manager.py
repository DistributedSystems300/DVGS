import queue
from time import sleep
from typing import Any, Dict, List, Optional, Tuple

import zmq  # type: ignore

from virtualgrid.base_node import BaseNode
from virtualgrid.job import Job
from virtualgrid.messages import (JobNotAcceptedMessage, GetLoadMessage, GetStatusMessage, JobAcceptedMessage,
                                  JobStartedMessage, LoadMessage, JobMessage, StartJobMessage, StatusMessage,
                                  UnknownMessageError, GiveJobMessage, OptionalJobMessage)
from virtualgrid.vector_clock import VectorClock

SCHEDULER_SLEEP_TIME = 5  # seconds


class ResourceManager(BaseNode):
    def __init__(self,
                 id_: int,
                 max_job_queue_size: int,
                 gs_address: str,
                 node_addresses: List[str],
                 port: int,
                 clock: VectorClock
                 ) -> None:
        super().__init__(clock, port)
        self.context = zmq.Context()
        
        self.id_ = id_
        self.gs_address = gs_address
        self.node_addresses = node_addresses

        self.job_queue = queue.Queue(max_job_queue_size)

    def run_job_scheduler(self):
        """
        Run the job scheduler.

        This method tries to schedule a jobs using _schedule_job().
        If scheduling a job is successful, it tries to schedule another
        one immediately. In case it fails, it sleeps for SCHEDULER_SLEEP_TIME
        seconds.
        """

        timestamp = self.clock.register_event()
        log_message = "Started the job scheduler"
        self._log(timestamp, log_message)

        while True:
            job, node_address = self._schedule_job()
            timestamp = self.clock.register_event()

            if job:
                log_message = f"Scheduled job {job} on the node {node_address}"
                self._log(timestamp, log_message)
            else:
                log_message = f"Couldn't schedule a job, waiting {SCHEDULER_SLEEP_TIME}s"
                self._log(timestamp, log_message)
                sleep(SCHEDULER_SLEEP_TIME)

    def _schedule_job(self) -> Tuple[Optional[Job], Optional[str]]:
        """
        Try to schedule a job from the queue.

        Returns a tuple containing the scheduled job and the address of the node
        on which it was scheduled.

        If scheduling was not successful (i.e. there was no job in the queue
        or there was no free node to schedule it on), two Nones are returned.
        """

        if self.job_queue.empty():
            return None, None

        node_address = self._get_idle_node()

        if node_address is None:
            return None, None

        try:
            job = self.job_queue.get(block=False)
        except queue.Empty:
            return None, None

        self._submit_job(job, node_address)
        return job, node_address

    def _handle_valid_message(self, message: Any) -> Any:
        """
        Handle a valid (already unpickled) message, and return a message
        to send back.

        In case there's no action associated with the message,
        UnknownMessageError is sent back.
        """

        if isinstance(message, GetLoadMessage):
            self.clock.receive_message(message.clock)
            return self._make_message(LoadMessage, self.get_load())
        elif isinstance(message, JobMessage):
            timestamp = self.clock.receive_message(message.clock)
            log_message = f"Received job {message.job}"
            self._log(timestamp, log_message)
            queued = self._queue_job(message.job)

            if queued:
                return self._make_message(JobAcceptedMessage)
            else:
                return self._offload_job_to_gs(message.job)
        elif isinstance(message, GiveJobMessage):
            timestamp = self.clock.receive_message(message.clock)
            log_message = f"Received give job message"
            self._log(timestamp, log_message)

            try:
                job = self.job_queue.get(block=False)
            except queue.Empty:
                job = None

            return self._make_message(OptionalJobMessage, job)
        else:
            timestamp = self.clock.register_event()
            log_message = f"Received unknown message: {message}"
            self._log(timestamp, log_message)
            return self._make_message(UnknownMessageError)

    def _queue_job(self, job: Job) -> bool:
        """
        Try to schedule a job using this resource manager. Return value True
        means it was successful, False means it wasn't.

        If a job can be scheduled on this cluster, the job is accepted by this
        resource manager.
        """

        try:
            self.job_queue.put(job, block=False)
            timestamp = self.clock.register_event()
            message = f"Queued {job}"
            self._log(timestamp, message)
            return True
        except queue.Full:
            return False

    def _offload_job_to_gs(self, job: Job) -> Any:
        """
        Offload a job to the grid scheduler.

        A situation like that can happen whenever the resource manager thinks
        he can't handle more jobs, and wants this job to be scheduled on a different
        cluster by the grid scheduler.
        """
        timestamp = self.clock.register_event()
        log_message = f"Can't accept job ID={job.id}, offloading it to the grid scheduler"
        self._log(timestamp, log_message)

        socket = self.context.socket(zmq.REQ)
        socket.connect(f'tcp://{self.gs_address}')
        socket.RCVTIMEO = 1000

        message = self._make_message(JobMessage, job, rm_id=self.id_)

        try:
            socket.send_pyobj(message)
            response = socket.recv_pyobj()
        except zmq.error.Again:
            timestamp = self.clock.register_event()
            log_message = f"Cannot offload to GS {self.gs_address})"
            self._log(timestamp, log_message)
            return self._make_message(JobNotAcceptedMessage)

        if isinstance(response, JobAcceptedMessage):
            timestamp = self.clock.receive_message(response.clock)
            log_message = f"Offloaded job {job.id} to GS {self.gs_address}"
            self._log(timestamp, log_message)
            return self._make_message(JobAcceptedMessage)
        elif isinstance(response, JobNotAcceptedMessage):
            timestamp = self.clock.receive_message(response.clock)
            log_message = f"Could not offload job {job.id} to GS {self.gs_address}"
            self._log(timestamp, log_message)
            return self._make_message(JobNotAcceptedMessage)
        else:
            timestamp = self.clock.register_event()
            log_message = f"Got unexepected resonse {response} from GS {self.gs_address}"
            self._log(timestamp, log_message)
            return self._make_message(UnknownMessageError)

    def _get_statuses(self) -> Dict[str, str]:
        """
        Query each node for its status, and return a dictionary
        which keys are node addresses and values are their statuses.

        A node is omitted if it sends an incomprehensible message.
        """

        statuses = {}

        for node_address in self.node_addresses:
            socket = self.context.socket(zmq.REQ)
            socket.connect(f'tcp://{node_address}')

            # TODO: Handle unpickling errors
            status_message = self._make_message(GetStatusMessage)
            socket.send_pyobj(status_message)
            response = socket.recv_pyobj()

            if isinstance(response, StatusMessage):
                self.clock.receive_message(response.clock)
                statuses[node_address] = response.status
            else:
                timestamp = self.clock.register_event()
                log_message = f"Received: {response} (expected: {StatusMessage.__name__})"
                self._log(timestamp, log_message)

        return statuses

    def _get_idle_node(self) -> Optional[str]:
        """
        Query the nodes for their status, pick one which status is
        'IDLE', and return it's address. If no nodes are free,
        return None.
        """

        statuses = self._get_statuses()

        for address, status in statuses.items():
            if status == 'IDLE':
                return address
        else:
            return None

    def _submit_job(self, job: Job, node_address: str) -> bool:
        """
        Submit a job to a node. Return True if the node responds
        with JobStartedMessage, and False otherwise.
        """

        socket = self.context.socket(zmq.REQ)
        socket.connect(f'tcp://{node_address}')

        # TODO: Handle unpickling errors
        message = self._make_message(StartJobMessage, job=job)
        socket.send_pyobj(message)
        response = socket.recv_pyobj()

        if isinstance(response, JobStartedMessage):
            timestamp = self.clock.receive_message(response.clock)
            log_message = f'Submitted {job} to {node_address}'
            self._log(timestamp, log_message)
            return True
        else:
            timestamp = self.clock.register_event()
            log_message = f'Submitting {job} to {node_address} failed\n'
            self._log(timestamp, log_message)
            return False

    def _get_number_running_jobs(self) -> int:
        statuses = self._get_statuses()
        return sum(1 for status in statuses.values() if status == 'BUSY')

    def get_load(self) -> int:
        return self.job_queue.qsize()

    def _listen_action(self):
        return

    def __repr__(self):
        return f"ResourceManager(queue_size={self.job_queue.qsize()})"
