import asyncio
from unittest.mock import Mock

from tau.core import Network, Event, HistoricNetworkScheduler, MutableSignal, RealtimeNetworkScheduler
from tau.event import Do
from tau.signal import From


def test_event_propagation():
    network = Network()

    a = Mock(spec=Event)
    a.on_activate.return_value = True
    b = Mock(spec=Event)
    c = Mock(spec=Event)

    network.connect(a, b)
    network.connect(a, c)
    network.activate(a)
    a.on_activate.assert_called_once()
    b.on_activate.assert_called_once()
    c.on_activate.assert_called_once()


def test_event_short_circuit():
    network = Network()

    a = Mock(spec=Event)
    a.on_activate.return_value = False
    b = Mock(spec=Event)
    c = Mock(spec=Event)

    network.connect(a, b)
    network.connect(a, c)
    network.activate(a)
    a.on_activate.assert_called_once()
    b.on_activate.assert_not_called()
    c.on_activate.assert_not_called()


def test_historic_scheduler1():
    scheduler = HistoricNetworkScheduler(0, 1000)
    run_times = []

    class RecordEventTime(Event):
        def on_activate(self) -> bool:
            run_times.append(scheduler.get_time())
            return False

    evt = RecordEventTime()
    scheduler.network.attach(evt)
    scheduler.schedule_event_at(evt, -1)
    scheduler.schedule_event_at(evt, 500)
    scheduler.schedule_event_at(evt, 0)
    scheduler.schedule_event_at(evt, 1500)
    scheduler.schedule_event_at(evt, 1000)

    scheduler.run()
    assert run_times == [0, 500, 1000]


def test_historic_scheduler2():
    scheduler = HistoricNetworkScheduler(0, 1000)
    run_times = []

    class RecordEventTime(Event):
        def on_activate(self) -> bool:
            run_times.append(scheduler.get_time())
            return False

    evt = RecordEventTime()
    scheduler.network.attach(evt)
    scheduler.schedule_event(evt, -1)
    scheduler.schedule_event(evt, 500)
    scheduler.schedule_event(evt, 0)
    scheduler.schedule_event(evt, 1500)
    scheduler.schedule_event(evt, 1000)

    scheduler.run()
    assert run_times == [0, 500, 1000]


# noinspection DuplicatedCode
def test_historic_scheduler3():
    scheduler = HistoricNetworkScheduler(0, 1000)
    run_times = []

    class RecordEventTime(Event):
        def on_activate(self) -> bool:
            run_times.append(scheduler.get_time())
            return False

    signal = MutableSignal()
    evt = RecordEventTime()
    scheduler.network.connect(signal, evt)
    scheduler.schedule_update(signal, True, -1)
    scheduler.schedule_update(signal, True, 500)
    scheduler.schedule_update(signal, True, 0)
    scheduler.schedule_update(signal, True, 1500)
    scheduler.schedule_update(signal, True, 1000)

    scheduler.run()
    assert run_times == [0, 500, 1000]


# noinspection DuplicatedCode
def test_historic_scheduler4():
    scheduler = HistoricNetworkScheduler(0, 1000)
    run_times = []

    class RecordEventTime(Event):
        def on_activate(self) -> bool:
            run_times.append(scheduler.get_time())
            return False

    signal = MutableSignal()
    evt = RecordEventTime()
    scheduler.network.connect(signal, evt)
    scheduler.schedule_update_at(signal, True, -1)
    scheduler.schedule_update_at(signal, True, 500)
    scheduler.schedule_update_at(signal, True, 0)
    scheduler.schedule_update_at(signal, True, 1500)
    scheduler.schedule_update_at(signal, True, 1000)

    scheduler.run()
    assert run_times == [0, 500, 1000]


def test_raise_exception_in_exec_q():
    def fail():
        raise ValueError()

    async def main():
        scheduler = RealtimeNetworkScheduler()
        network = scheduler.get_network()
        values = From(scheduler, [0.0, 3.2, 2.1, 2.9, 8.3, 5.7])
        Do(network, values, lambda: fail())

    asyncio.run(main())

