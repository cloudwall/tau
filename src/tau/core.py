import asyncio
import time

from abc import ABC, abstractmethod
from functools import total_ordering
from queue import PriorityQueue
from typing import Any

# noinspection PyPackageRequirements
from graph import Graph


class Event(ABC):
    """
    An action that happens at a moment in time.
    """
    @abstractmethod
    def on_activate(self) -> bool:
        """
        Callback made whenever this event gets activated.
        :returns: True if subsequent nodes in the graph should be activated
        """
        pass


class Signal(Event, ABC):
    """
    An Event with a value associated with it.
    """
    def __init__(self, initial_value: Any = None):
        super().__init__()
        self.value = initial_value
        self.modified = False

    def is_valid(self) -> bool:
        """
        :return: True if the value is non-None (default behavior; may be subclassed)
        """
        return self.value is not None

    def get_value(self) -> Any:
        """
        Gets the current value of the signal; may be None.
        """
        return self.value

    def _update(self, value):
        """
        Internal method for updating the value of the signal; used it subclasses.
        """
        self.value = value
        self.modified = True


class MutableSignal(Signal):
    """
    A signal whose value can be updated programmatically.
    """
    def __init__(self, initial_value: Any = None):
        super().__init__(initial_value)

    def on_activate(self):
        if self.modified:
            self.modified = False
            return True
        else:
            return False

    def set_value(self, value: Any):
        self._update(value)


class Network:
    """
    A graph network connecting Events.
    """
    def __init__(self):
        self.graph = Graph()
        self.activation_flags = dict()
        self.current_id = 0
        self.node_id_map = {}

    def attach(self, evt: Event):
        if evt in self.node_id_map:
            return
        else:
            node_id = self.__next_id()
            self.node_id_map[evt] = node_id
            self.graph.add_node(node_id, evt)

    def connect(self, evt1: Event, evt2: Event):
        self.activation_flags[evt1] = False
        self.activation_flags[evt2] = False
        self.attach(evt1)
        self.attach(evt2)
        self.graph.add_edge(self.node_id_map[evt1], self.node_id_map[evt2])

    def disconnect(self, evt1: Event, evt2: Event):
        del self.activation_flags[evt1]
        del self.activation_flags[evt2]
        self.graph.del_edge(self.node_id_map[evt1], self.node_id_map[evt2])

    def has_activated(self, evt: Event):
        return self.activation_flags[evt]

    # noinspection PyCallingNonCallable
    def activate(self, evt: Event):
        self.__clear_activation_flags()

        def scan_nodes(node_id) -> bool:
            current_evt = self.graph.node(node_id)
            self.activation_flags[current_evt] = True
            return current_evt.on_activate()

        self.graph.depth_scan(self.node_id_map[evt], scan_nodes)

    def __next_id(self):
        self.current_id += 1
        return self.current_id

    def __clear_activation_flags(self):
        self.activation_flags = self.activation_flags.fromkeys(self.activation_flags, False)


class NetworkScheduler(ABC):
    """
    Abstract base class for the event scheduler.
    """
    def __init__(self, network: Network):
        self.network = network

    def get_network(self):
        return self.network

    @abstractmethod
    def get_time(self) -> int:
        pass

    @abstractmethod
    def schedule_event(self, evt: Event, offset_millis: int = 0):
        pass

    @abstractmethod
    def schedule_event_at(self, evt: Event, time_millis: int = 0):
        pass

    @abstractmethod
    def schedule_update(self, signal: MutableSignal, value: Any, offset_millis: int = 0):
        pass

    @abstractmethod
    def schedule_update_at(self, signal: MutableSignal, value: Any, time_millis: int = 0):
        pass


class RealtimeNetworkScheduler(NetworkScheduler):
    """
    A real-time event scheduler sitting on top of asyncio that provides natural operations for
    scheduling events connected in a Network.
    """
    def __init__(self, network: Network = Network()):
        super().__init__(network)

    def get_time(self) -> int:
        return int(round(time.time() * 1000))

    def schedule_event(self, evt: Event, offset_millis: int = 0):
        self._schedule(lambda: self.network.activate(evt), offset_millis)

    def schedule_event_at(self, evt: Event, time_millis: int = 0):
        self._schedule(lambda: self.network.activate(evt), time_millis - self.get_time())

    def schedule_update(self, signal: MutableSignal, value: Any, offset_millis: int = 0):
        def set_and_activate():
            signal.set_value(value)
            self.network.activate(signal)
        self._schedule(set_and_activate, offset_millis)

    def schedule_update_at(self, signal: MutableSignal, value: Any, time_millis: int = 0):
        def set_and_activate():
            signal.set_value(value)
            self.network.activate(signal)
        self._schedule(set_and_activate, time_millis - self.get_time())

    @staticmethod
    def _schedule(callback, offset_millis: int):
        if offset_millis == 0:
            asyncio.get_event_loop().call_soon(callback)
        elif offset_millis < 0:
            raise ValueError("Unable to schedule in the past using RealtimeNetworkScheduler")
        else:
            asyncio.get_event_loop().call_later(offset_millis / 1000, callback)


@total_ordering
class HistoricalEvent:
    def __init__(self, time_millis: int, action: Any):
        self.time_millis = time_millis
        self.action = action

    def get_time_millis(self) -> int:
        return self.time_millis

    def __eq__(self, other):
        return self.get_time_millis() == other.get_time_millis()

    def __ne__(self, other):
        return self.get_time_millis() != other.get_time_millis()

    def __lt__(self, other):
        return self.get_time_millis() < other.get_time_millis()


class HistoricNetworkScheduler(NetworkScheduler):
    """
    A historical mode scheduler suitable for backtesting.
    """
    def __init__(self, network: Network = Network()):
        super().__init__(network)
        self.event_queue = PriorityQueue()
        self.now = 0
        self.start_time = 0
        self.end_time = 0

    def get_time(self) -> int:
        return self.now

    def get_start_time(self) -> int:
        return self.start_time

    def get_end_time(self) -> int:
        return self.end_time

    def schedule_event(self, evt: Event, offset_millis: int = 0):
        event_time = self.get_time() + offset_millis
        hist_event = HistoricalEvent(event_time, lambda: self.network.activate(evt))
        self.event_queue.put(hist_event)

    def schedule_event_at(self, evt: Event, time_millis: int = 0):
        event_time = time_millis
        hist_event = HistoricalEvent(event_time, lambda: self.network.activate(evt))
        self.event_queue.put(hist_event)

    def schedule_update(self, signal: MutableSignal, value: Any, offset_millis: int = 0):
        event_time = self.get_time() + offset_millis

        def set_and_activate():
            signal.set_value(value)
            self.network.activate(signal)

        hist_event = HistoricalEvent(event_time, set_and_activate)
        self.event_queue.put(hist_event)

    def schedule_update_at(self, signal: MutableSignal, value: Any, time_millis: int = 0):
        event_time = time_millis

        def set_and_activate():
            signal.set_value(value)
            self.network.activate(signal)

        hist_event = HistoricalEvent(event_time, set_and_activate)
        self.event_queue.put(hist_event)

    def run(self, start_time_millis: int, end_time_millis: int):
        self.now = start_time_millis
        self.start_time = start_time_millis
        self.end_time = end_time_millis
        while True:
            if self.event_queue.empty():
                return
            event = self.event_queue.get_nowait()
            if event.time_millis > end_time_millis:
                # end of time
                return
            elif event.time_millis < start_time_millis:
                # pre-history
                continue

            self.now = event.time_millis
            event.action()

