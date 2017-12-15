import pickle
from typing import List, Optional
import zmq

from virtualgrid.log import LogEntry
from virtualgrid.messages import UnknownMessageError
from virtualgrid.vector_clock import VectorClock


class BaseNode:
    """
    The basic node has port listening and logging capabilities.
    When implementing the `_listen_action` and `_handle_valid_message` methods have to be implemented.
    """
    def __init__(self,
                 clock: VectorClock,
                 port: Optional[int]
                 ) -> None:
        self.clock = clock
        self.logs: List[LogEntry] = []

        self.port = port
        self.socket = zmq.Context().socket(zmq.REP)
        self.socket.bind(f'tcp://*:{port}')

    def listen(self):
        """
        Listen for messages, try to unpickle them, run the associated operations,
        and return the results.

        This method assumes that a received message can be unpickled. If not,
        an UnknownMessageError is sent back. This message is also sent back
        if the message can be unpickled, but there's no action associated with it.
        """

        timestamp = self.clock.register_event()
        self._log(timestamp, f'Listening on port {self.port}...')

        while True:
            self._listen_action()

            raw_message = self.socket.recv()

            try:
                message = pickle.loads(raw_message)
                response = self._handle_valid_message(message)
            except pickle.UnpicklingError:
                timestamp = self.clock.register_event()
                message = f"Received: {raw_message} (couldn't unpickle)"
                self._log(timestamp, message)

                timestamp = self.clock.send_message()
                response = UnknownMessageError(timestamp)

            self.socket.send_pyobj(response)

    def _listen_action(self):
        raise NotImplementedError

    def _handle_valid_message(self, message):
        raise NotImplementedError

    def _log(self, timestamp: List[int], message: str) -> None:
        """
        Append a log to the list of logs.
        """
        log = LogEntry(timestamp, f"{self.__class__.__name__}<{self.port}>", message)
        self.logs.append(log)
        print(log)

    def _make_message(self, msg_type, *args, **kwargs):
        timestamp = self.clock.send_message()
        return msg_type(*args, **kwargs, clock=timestamp)
