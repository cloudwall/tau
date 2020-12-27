import asyncio
import datetime
from abc import abstractmethod
from datetime import timedelta
from typing import Callable, Any, List

from tau.core import Signal, Network, MutableSignal, NetworkScheduler, Event


class Function(Signal):
    """
    Base class for streaming functions with zero or more streaming input signals.
    """
    def __init__(self, network: Network, parameters: Any):
        super().__init__()
        if type(parameters) is not list:
            parameters = [parameters]
        self.parameters = parameters
        for param in parameters:
            network.connect(param, self)

    def on_activate(self) -> bool:
        self.modified = False
        self._call()
        return self.modified

    @abstractmethod
    def _call(self):
        pass


class BufferWithCount(Function):
    """
    Operators that transforms a stream of values into batched values, as lists. This particular
    implementation corresponds to rxpy's buffer_with_count operator.

    .. seealso:: http://reactivex.io/documentation/operators/buffer.html
    """
    def __init__(self, network: Network, values: Signal, count: int):
        super().__init__(network, [values])
        self.values = values
        self.count = count
        self.buffer = list()

    def _call(self):
        if self.values.is_valid():
            self.buffer.append(self.values.get_value())
            if len(self.buffer) == self.count:
                self._update(self.buffer.copy())
                self.buffer.clear()


class BufferWithTime(Function):
    """
    Operators that transforms a stream of values into batched values, as lists. This particular
    implementation corresponds to rxpy's buffer_with_time operator.

    .. seealso:: http://reactivex.io/documentation/operators/buffer.html
    """
    def __init__(self, scheduler: NetworkScheduler, values: Signal, interval: timedelta):
        super().__init__(scheduler.get_network(), [values])
        self.values = values
        self.interval = interval
        self.buffer = list()
        self.timed_out = False

        class FireTimer(Event):
            def __init__(self, buffer: BufferWithTime):
                self.buffer = buffer

            def on_activate(self) -> bool:
                self.buffer.timed_out = True
                next_timer = FireTimer(self.buffer)
                scheduler.get_network().attach(next_timer)
                scheduler.schedule_event(next_timer, int(self.buffer.interval.total_seconds() * 1000))
                return False

        timer = FireTimer(self)
        scheduler.get_network().attach(timer)
        scheduler.schedule_event(timer, int(self.interval.total_seconds() * 1000))

    def _call(self):
        if self.values.is_valid():
            if self.timed_out:
                self._update(self.buffer.copy())
                self.buffer.clear()
                self.timed_out = False
            self.buffer.append(self.values.get_value())


class Filter(Function):
    """
    Simple operator function that applies a filtering predicate to a stream of values
    and only returns matching values.
    """
    def __init__(self, network: Network, values: Signal, predicate: Callable[[Any], bool]):
        super().__init__(network, [values])
        self.values = values
        self.predicate = predicate

    def _call(self):
        if self.values.is_valid():
            next_value = self.values.get_value()
            if self.predicate(next_value):
                self._update(next_value)


class Map(Function):
    """
    Transforming function that applies a Callable to incoming values and updates the output value.

    .. seealso:: http://reactivex.io/documentation/operators/map.html
    """
    def __init__(self, network: Network, values: Signal, mapper: Callable[[Any], Any]):
        super().__init__(network, [values])
        self.values = values
        self.mapper = mapper

    def _call(self):
        if self.values.is_valid():
            next_value = self.values.get_value()
            self._update(self.mapper(next_value))


class FlatMap(MutableSignal):
    """
    Transforming function that applies a Callable to incoming values and updates the output value.

    .. seealso:: http://reactivex.io/documentation/operators/flatmap.html
    """
    def __init__(self, scheduler: NetworkScheduler, values: Signal):
        super().__init__()
        self.scheduler = scheduler
        self.values = values

        class Handler(Event):
            def __init__(self, outer):
                self.outer = outer

            def on_activate(self) -> bool:
                if values.is_valid():
                    next_values = values.get_value()
                    for next_value in next_values:
                        scheduler.schedule_update(self.outer, next_value)
                    return True
                else:
                    return False

        scheduler.get_network().connect(values, Handler(self))


class Just(MutableSignal):
    """
    Emits a single value immediately.
    .. seealso:: http://reactivex.io/documentation/operators/just.html
    """
    def __init__(self, scheduler: NetworkScheduler, value: Any):
        super().__init__()
        scheduler.schedule_update(self, value)


class From(MutableSignal):
    """
    Emits a list of values immediately, in order.

    .. seealso:: http://reactivex.io/documentation/operators/from.html
    """
    def __init__(self, scheduler: NetworkScheduler, values: List):
        super().__init__()
        for value in values:
            scheduler.schedule_update(self, value)


class Interval(MutableSignal):
    """
    Emits a monotonically increasing sequence of integers spaced out by a given interval of time.

    .. seealso:: http://reactivex.io/documentation/operators/interval.html
    """

    def on_activate(self) -> bool:
        return True

    def __init__(self, scheduler: NetworkScheduler, interval: timedelta = timedelta(seconds=1)):
        super().__init__()
        self.next_value = 0

        async def schedule_update():
            while True:
                self.next_value += 1
                scheduler.schedule_update(self, self.next_value)
                await asyncio.sleep(int(interval.total_seconds()))

        asyncio.create_task(schedule_update())


class WindowWithCount(Function):
    """
    Operator that accumulates values using a rolling window. Every tick results
    in a new batch with one more value at the end and one less value at the front.

    .. seealso:: http://reactivex.io/documentation/operators/window.html
    """

    def __init__(self, network: Network, values: Signal, count: int):
        super().__init__(network, [values])
        self.buffer = list()
        self.count = count

    def _call(self):
        if self.parameters[0].is_valid():
            if len(self.buffer) == self.count - 1:
                self.buffer.append(self.parameters[0].get_value())
                self._update(self.buffer)
            elif len(self.buffer) == self.count:
                self.buffer = self.buffer[1:]
                self.buffer.append(self.parameters[0].get_value())
                self._update(self.buffer)
            else:
                self.buffer.append(self.parameters[0].get_value())


class Scan(Function):
    """
    Operator that accumulates values in a streaming fashion.

    .. seealso:: http://reactivex.io/documentation/operators/scan.html
    """
    def __init__(self, network: Network, values: Signal):
        super().__init__(network, [values])
        self.prev_value = 0.0

    def _call(self):
        if self.parameters[0].is_valid():
            next_value = self.parameters[0].get_value()
            new_value = self.prev_value + next_value
            self._update(new_value)
            self.prev_value = new_value


class AllActivated(Event):
    """
    An event that activates when all of N input events have activated.
    """
    def __init__(self, network: Network, events: List):
        super().__init__()
        self.network = network
        self.events = events
        self.activated = dict()
        for event in events:
            self.activated[event] = False
            network.connect(event, self)

    def on_activate(self) -> bool:
        for event in self.events:
            if self.network.has_activated(event):
                self.activated[event] = True
        return all(self.activated.values())


class AnyActivated(Event):
    """
    An event that activates when any of N input events activate.
    """
    def __init__(self, network: Network, events: List):
        super().__init__()
        self.network = network
        self.events = events
        self.activated = dict()
        for event in events:
            self.activated[event] = False
            network.connect(event, self)

    def on_activate(self) -> bool:
        for event in self.events:
            if self.network.has_activated(event):
                self.activated[event] = True
        return any(self.activated.values())


class Pipe(Event):
    """
    An event that "pipes" signal values from an input signal to an output MutableSignal
    """
    def __init__(self, scheduler: NetworkScheduler, in_signal: Signal, out_signal: MutableSignal):
        self.scheduler = scheduler
        self.in_signal = in_signal
        self.out_signal = out_signal
        self.scheduler.network.connect(in_signal, self)

    def on_activate(self) -> bool:
        if self.in_signal.is_valid():
            value = self.in_signal.get_value()
            self.scheduler.schedule_update(self.out_signal, value)
            return True
        else:
            return False


class RepeatingTimer(Event):
    def __init__(self, scheduler: NetworkScheduler, interval: timedelta):
        self.scheduler = scheduler
        self.interval = interval
        self._schedule()

    def on_activate(self) -> bool:
        self._schedule()
        return True

    def _schedule(self):
        self.scheduler.get_network().attach(self)
        self.scheduler.schedule_event(self, int(self.interval.total_seconds() * 1000))


class Alarm(Event):
    def __init__(self, scheduler: NetworkScheduler, wake_up_time: datetime.time, tz: datetime.tzinfo):
        self.scheduler = scheduler
        self.wake_up_time = wake_up_time
        self.tz = tz
        self._schedule()

    def on_activate(self) -> bool:
        self._schedule()
        return True

    def _schedule(self):
        self.scheduler.get_network().attach(self)
        today = datetime.datetime.fromtimestamp(self.scheduler.get_time() / 1000.0, tz=self.tz)
        if today.time() < self.wake_up_time:
            wake_up = datetime.datetime.combine(today.date(), self.wake_up_time)
        else:
            tomorrow = datetime.datetime(today.year, today.month, today.day) + timedelta(days=1)
            wake_up = datetime.datetime.combine(tomorrow.date(), self.wake_up_time)
        self.scheduler.schedule_event_at(self, int(wake_up.timestamp() * 1000))
