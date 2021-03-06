import asyncio
import logging
import queue
import sys
import time

from abc import ABC, abstractmethod
from datetime import datetime, timedelta, tzinfo
from functools import total_ordering
from queue import PriorityQueue
from threading import Thread
from typing import Any, Callable

import tzlocal

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


class Clock:
    """
    API that provides Python-friendly datetime representations for the native milliseconds used in NetworkScheduler.
    Note that if timezone is not specified for TZ-specific operations the local TZ reported by tzlocal API is used.
    """
    def __init__(self, scheduler):
        self.scheduler = scheduler
        self.local_zone = tzlocal.get_localzone()

    def get_tz(self) -> tzinfo:
        return self.local_zone

    def get_time(self, tz: tzinfo = None) -> datetime:
        return self._convert_millis_to_datetime(self.scheduler.get_time(), tz)

    def get_start_time(self, tz: tzinfo = None) -> datetime:
        return self._convert_millis_to_datetime(self.scheduler.get_start_time(), tz)

    def get_end_time(self, tz: tzinfo = None) -> datetime:
        return self._convert_millis_to_datetime(self.scheduler.get_end_time(), tz)

    @staticmethod
    def to_millis_offset(interval: timedelta) -> int:
        return int(interval.total_seconds() * 1000.0)

    @staticmethod
    def to_millis_time(at_time: datetime) -> int:
        return int(at_time.timestamp() * 1000.0)

    def _convert_millis_to_datetime(self, millis: int, tz: tzinfo):
        if tz is None:
            tz = self.local_zone
        return datetime.fromtimestamp(millis / 1000, tz)


class NetworkScheduler(ABC):
    """
    Abstract base class for the event scheduler.
    """
    def __init__(self, network: Network):
        self.network = network
        self.clock = Clock(self)

    def get_network(self) -> Network:
        return self.network

    def get_clock(self) -> Clock:
        return self.clock

    @abstractmethod
    def get_time(self) -> int:
        pass

    @abstractmethod
    def get_start_time(self) -> int:
        pass

    @abstractmethod
    def get_end_time(self) -> int:
        pass

    @abstractmethod
    def schedule(self, action, offset_millis: int = 0):
        pass

    @abstractmethod
    def schedule_at(self, action, time_millis: int):
        pass

    @abstractmethod
    def schedule_event(self, evt: Event, offset_millis: int = 0):
        pass

    @abstractmethod
    def schedule_event_at(self, evt: Event, time_millis: int):
        pass

    @abstractmethod
    def schedule_update(self, signal: MutableSignal, value: Any, offset_millis: int = 0):
        pass

    @abstractmethod
    def schedule_update_at(self, signal: MutableSignal, value: Any, time_millis: int):
        pass


class RealtimeNetworkScheduler(NetworkScheduler):
    """
    A real-time event scheduler sitting on top of asyncio that provides natural operations for
    scheduling events connected in a Network.
    """

    logger = logging.getLogger(__name__)

    def __init__(self, network: Network = Network()):
        super().__init__(network)
        self.q = queue.Queue()
        self.loop = asyncio.get_event_loop()

        class ExecutionQueueThread(Thread):
            def __init__(self, scheduler: RealtimeNetworkScheduler, exec_q: queue.Queue):
                super().__init__(name="XQ-Thread", daemon=True)
                self.scheduler = scheduler
                self.exec_q = exec_q
                self.running = True

            def run(self):
                while self.running:
                    runnable = self.exec_q.get()

                    # noinspection PyBroadException
                    try:
                        runnable()
                    except BaseException as error:
                        self.scheduler.logger.error('Uncaught exception in ExecutionQueueThread', exc_info=error)

        self.consumer = ExecutionQueueThread(self, self.q)
        self.consumer.start()
        self.start_time = self.get_time()

    def get_time(self) -> int:
        return int(round(time.time() * 1000))

    def get_start_time(self) -> int:
        return self.start_time

    def get_end_time(self) -> int:
        return sys.maxsize

    def schedule(self, action, offset_millis: int = 0):
        self._schedule(action, offset_millis)

    def schedule_at(self, action, time_millis: int = 0):
        self._schedule(action, time_millis - self.get_time())

    def schedule_event(self, evt: Event, offset_millis: int = 0):
        self._schedule(lambda: self.network.activate(evt), offset_millis)

    def schedule_event_at(self, evt: Event, time_millis: int = 0):
        self._schedule(lambda: self.network.activate(evt), time_millis - self.get_time())

    def schedule_update(self, signal: MutableSignal, value: Any, offset_millis: int = 0):
        def set_and_activate():
            if isinstance(value, Callable):
                signal.set_value(value())
            else:
                signal.set_value(value)
            self.network.activate(signal)
        self._schedule(set_and_activate, offset_millis)

    def schedule_update_at(self, signal: MutableSignal, value: Any, time_millis: int = 0):
        def set_and_activate():
            if isinstance(value, Callable):
                signal.set_value(value())
            else:
                signal.set_value(value)
            self.network.activate(signal)
        self._schedule(set_and_activate, time_millis - self.get_time())

    def shutdown(self):
        self.loop.stop()
        self.consumer.running = False

    def _schedule(self, callback, offset_millis: int):
        def producer_callback():
            self.q.put(callback)

        if offset_millis == 0:
            self.loop.call_soon_threadsafe(producer_callback)
        elif offset_millis < 0:
            raise ValueError("Unable to schedule in the past using RealtimeNetworkScheduler")
        else:
            self.loop.call_later(offset_millis / 1000, producer_callback)


class SignalGenerator(ABC):
    """
    A class that runs *before* historical time starts which takes care of loading historical signal data
    into the scheduler.
    """
    def generate(self, scheduler: NetworkScheduler):
        pass


@total_ordering
class HistoricalEvent:
    def __init__(self, time_millis: int, cycle: int, action: Any):
        self.time_millis = time_millis
        self.cycle = cycle
        self.action = action

    def get_time_millis(self) -> int:
        return self.time_millis

    def get_cycle(self) -> int:
        return self.cycle

    def __eq__(self, other):
        return (self.get_time_millis() == other.get_time_millis()) and (self.get_cycle() == other.get_cycle())

    def __ne__(self, other):
        return (self.get_time_millis() != other.get_time_millis()) and (self.get_cycle() != other.get_cycle())

    def __lt__(self, other):
        if self.get_time_millis() < other.get_time_millis():
            return True
        elif self.get_time_millis() > other.get_time_millis():
            return False
        else:
            return self.get_cycle() < other.get_cycle()


class HistoricNetworkScheduler(NetworkScheduler):
    """
    A historical mode scheduler suitable for backtesting.
    """

    logger = logging.getLogger(__name__)

    @classmethod
    def new_instance(cls, start_time: str, end_time: str):
        """
        Creates a HistoricNetworkScheduler based on %Y-%m-%dT%H:%M:%S format time strings for start & end time.
        """
        timestamp_fmt = '%Y-%m-%dT%H:%M:%S'
        start_time_millis = int(time.mktime(datetime.strptime(start_time, timestamp_fmt).timetuple()) * 1000)
        end_time_millis = int(time.mktime(datetime.strptime(end_time, timestamp_fmt).timetuple()) * 1000)
        return HistoricNetworkScheduler(start_time_millis, end_time_millis)

    def __init__(self, start_time_millis: int, end_time_millis: int, network: Network = Network()):
        super().__init__(network)
        self.event_queue = PriorityQueue()
        self.start_time = start_time_millis
        self.end_time = end_time_millis
        self.now = self.start_time
        self.cycle = 0
        self.generators = []

    def get_time(self) -> int:
        return self.now

    def get_start_time(self) -> int:
        return self.start_time

    def get_end_time(self) -> int:
        return self.end_time

    def add_generator(self, generator: SignalGenerator):
        self.generators.append(generator)

    def schedule(self, action, offset_millis: int = 0):
        event_time = self.get_time() + offset_millis
        hist_event = self.__create_event(event_time, action)
        self.event_queue.put(hist_event)

    def schedule_at(self, action, time_millis: int = 0):
        event_time = time_millis
        hist_event = self.__create_event(event_time, action)
        self.event_queue.put(hist_event)

    def schedule_event(self, evt: Event, offset_millis: int = 0):
        event_time = self.get_time() + offset_millis
        hist_event = self.__create_event(event_time, lambda: self.network.activate(evt))
        self.event_queue.put(hist_event)

    def schedule_event_at(self, evt: Event, time_millis: int = 0):
        event_time = time_millis
        hist_event = self.__create_event(event_time, lambda: self.network.activate(evt))
        self.event_queue.put(hist_event)

    def schedule_update(self, signal: MutableSignal, value: Any, offset_millis: int = 0):
        event_time = self.get_time() + offset_millis

        def set_and_activate():
            if isinstance(value, Callable):
                signal.set_value(value())
            else:
                signal.set_value(value)
            self.network.activate(signal)

        hist_event = self.__create_event(event_time, set_and_activate)
        self.event_queue.put(hist_event)

    def schedule_update_at(self, signal: MutableSignal, value: Any, time_millis: int = 0):
        event_time = time_millis

        def set_and_activate():
            if isinstance(value, Callable):
                signal.set_value(value())
            else:
                signal.set_value(value)
            self.network.activate(signal)

        hist_event = self.__create_event(event_time, set_and_activate)
        self.event_queue.put(hist_event)

    def run(self):
        self.logger.debug('Running generators in pre-historic time')
        for generator in self.generators:
            generator.generate(self)

        self.now = self.start_time
        self.logger.debug(f'Starting historic run: now={self.now}')
        while True:
            if self.event_queue.empty():
                return
            event = self.event_queue.get_nowait()
            if event.time_millis > self.end_time:
                # end of time
                return
            elif event.time_millis < self.start_time:
                # pre-history
                continue

            self.now = event.time_millis
            event.action()
        self.logger.debug(f'Completed historic run: now={self.now}')

    def __create_event(self, event_time, action):
        self.cycle += 1
        return HistoricalEvent(event_time, self.cycle, action)
