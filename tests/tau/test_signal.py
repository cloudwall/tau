from datetime import timedelta

from tau.core import HistoricNetworkScheduler, MutableSignal
from tau.event import Do, Alarm, RepeatingTimer
from tau.signal import From, Map, Scan, Filter, BufferWithTime


def test_hello_world():
    scheduler = HistoricNetworkScheduler(0, 30 * 1000)
    signal = From(scheduler, ['world'])
    Do(scheduler.get_network(), signal, lambda: print(f'Hello, {signal.get_value()}!'))
    scheduler.run()


def test_map_reduce():
    scheduler = HistoricNetworkScheduler(0, 30 * 1000)
    network = scheduler.get_network()
    values = From(scheduler, [0.0, 3.2, 2.1, 2.9, 8.3, 5.7])
    mapper = Map(network, values, lambda x: round(x))
    accumulator = Scan(network, mapper)
    Do(network, accumulator, lambda: print(f'{accumulator.get_value()}'))
    scheduler.run()
    assert accumulator.get_value() == 22.0


def test_filter():
    scheduler = HistoricNetworkScheduler(0, 30 * 1000)
    network = scheduler.get_network()
    values = From(scheduler, [0.0, -3.2, 2.1, -2.9, 8.3, -5.7])
    filt = Filter(network, values, lambda x: x >= 0.0)
    Do(network, filt, lambda: print(f'{filt.get_value()}'))
    scheduler.run()
    assert filt.get_value() == 8.3


def test_buffer_with_time_historic():
    scheduler = HistoricNetworkScheduler(0, 30 * 1000)
    network = scheduler.get_network()
    values = MutableSignal()
    scheduler.schedule_update_at(values, 1.0, 1000)
    scheduler.schedule_update_at(values, -3.2, 2000)
    scheduler.schedule_update_at(values, 2.1, 10000)
    scheduler.schedule_update_at(values, -2.9, 15000)
    scheduler.schedule_update_at(values, 8.3, 25000)
    scheduler.schedule_update_at(values, -5.7, 30000)

    buffer = BufferWithTime(scheduler, values, timedelta(seconds=5))
    Do(network, buffer, lambda: print(f'{buffer.get_value()}'))
    scheduler.run()
