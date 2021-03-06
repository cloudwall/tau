import datetime
from datetime import timedelta

from typing import Callable, Any

from tau.core import Event, Network, NetworkScheduler, Clock


class NullEvent(Event):
    """
    A simple event that just propagates but does nothing when activated.
    """
    def on_activate(self) -> bool:
        return True


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
        self.first_time_schedule = True
        self._schedule()

    def on_activate(self) -> bool:
        self._schedule()
        return True

    def _schedule(self):
        self.scheduler.get_network().attach(self)
        today = self.scheduler.get_clock().get_time(tz=self.tz)
        if self.first_time_schedule and today.time() < self.wake_up_time:
            wake_up_dt = datetime.datetime.combine(today.date(), self.wake_up_time, tzinfo=self.tz)
        else:
            tomorrow = datetime.datetime(today.year, today.month, today.day, tzinfo=self.tz) + timedelta(days=1)
            wake_up_dt = datetime.datetime.combine(tomorrow.date(), self.wake_up_time, tzinfo=self.tz)
        self.scheduler.schedule_event_at(self, Clock.to_millis_time(wake_up_dt))
        self.first_time_schedule = False


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
        self.scheduler.schedule_event(self, Clock.to_millis_offset(self.interval))
