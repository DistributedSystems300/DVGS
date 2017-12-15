from typing import List

class VectorClock:
    """
    Vector clock implementation.
    Based on: https://github.com/eugene-eeo/vclock
    """
    def __init__(self, process_id: int, size: int) -> None:
        """
        Initialize a vector clock with process id and a certain size.
        """
        self._pid = process_id
        self._clock = [0] * size

    def get_vector(self) -> List[int]:
        """
        Get the current state of the vector clock.
        """
        return self._clock

    def send_message(self) -> List[int]:
        """
        Increment own counter and returns the new vector clock state
        """
        self.register_event()

        return self._clock

    def receive_message(self, other_clock: List[int]) -> List[int]:
        """
        Merge the clock state of the received message with clock state and increment own clock,
        """
        # Expand the clock if new processes have registered.
        if len(other_clock) > len(self._clock):
            self._expand_clock(len(other_clock))

        for i, value in enumerate(other_clock):
            self._clock[i] = max(self._clock[i], value)

        self.register_event()

        return self._clock

    def register_event(self) -> List[int]:
        """
        Register an evenet by incrmenting the own counter.
        """
        self._clock[self._pid] += 1

        return self._clock

    def _expand_clock(self, length: int) -> None:
        """
        Expand the vector clock to be of the correct length.
        """
        diff = length - len(self._clock)
        self._clock.extend([0] * diff)

    @staticmethod
    def compare(a: List[int], b: List[int]) -> int:
        """
        Compare the vectors of two vector clocks.

        Return:
            -1: if a < b
             0: if a = b or a ||| b
            +1: if a > b
        """
        # Make sure the lists have the same length
        if len(a) > len(b):
            b.extend([0] * (len(a) - len(b)))
        elif len(b) > len(a):
            a.extend([0] * (len(b) - len(a)))

        gt = False
        lt = False

        for i, j in zip(a,b):
            gt |= i > j
            lt |= j > i

            # We have conflicting timestamps - so a ||| b.
            if gt and lt:
                break

        return int(gt) - int(lt)

    def __repr__(self):
        return f"VectorClock(pid={self._pid}, clock={self._clock})"
