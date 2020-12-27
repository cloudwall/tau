import datetime
from datetime import timedelta

from typing import Callable, Any

from tau.core import Event, Network, NetworkScheduler


class Lambda(Event):
    """
    A helper implementation of Event that binds any Python function to zero or more Event parameters.
    """
    def __init__(self, network: Network, parameters: Any, function: Callable[[Any], Any]):
        super().__init__()
        if type(parameters) is not list:
            parameters = [parameters]

        self.parameters = parameters
        for param in parameters:
            network.connect(param, self)
        self.function = function

    def on_activate(self) -> bool:
        return self.function(self.parameters)


class Do(Event):
    """
    A simple operator that executes a function whenever an Event fires; a more limited form of
    Lambda, which lets you connect to multiple upstream parameters and pass them to the function.

    .. seealso:: http://reactivex.io/documentation/operators/do.html
    """
    def __init__(self, network: Network, event: Event, function: Callable[[], Any]):
        super().__init__()
        self.event = event
        self.function = function
        network.connect(event, self)

    def on_activate(self) -> bool:
        self.function()
        return True


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