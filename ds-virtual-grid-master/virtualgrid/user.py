from datetime import timedelta

import zmq
import time
import random

from virtualgrid.job import Job
from virtualgrid.messages import JobMessage, GetLoadMessage, ScheduleJobsMessage


class User:
    def __init__(self, rms, delay=1.0):
        self.rms = rms
        self.delay = delay

    def run(self):
        while True:
            response = self.queue_job(random.choice(self.rms), 5)

            print(f'User got response {response}')

            time.sleep(self.delay)
            
    def queue_job(self, rm_address: str, duration: int) -> None:
        job = Job(1, duration=timedelta(seconds=duration))
        message = JobMessage(job)

        context = zmq.Context()
        socket = context.socket(zmq.REQ)
        socket.connect(f'tcp://{rm_address}')

        socket.send_pyobj(message)
        return socket.recv_pyobj()




def get_rm_load(rm_address: str):
    message = GetLoadMessage()

    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    socket.connect(f'tcp://{rm_address}')

    socket.send_pyobj(message)
    return socket.recv_pyobj()


def schedule_gs_jobs(gs_address: str):
    # TODO: This is just a temporary solution
    message = ScheduleJobsMessage()

    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    socket.connect(f'tcp://{gs_address}')

    socket.send_pyobj(message)
    return socket.recv_pyobj()
