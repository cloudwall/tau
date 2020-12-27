import asyncio
from datetime import timedelta, datetime, time, tzinfo

import pytz

from tau.core import RealtimeNetworkScheduler, HistoricNetworkScheduler, MutableSignal
from tau.event import Do
from tau.signal import From, Map, Scan, Filter, BufferWithTime, Alarm, RepeatingTimer


def test_hello_world():
    async def main():
        scheduler = RealtimeNetworkScheduler()
        signal = From(scheduler, ['world'])
        Do(scheduler.get_network(), signal, lambda: print(f'Hello, {signal.get_value()}!'))

    asyncio.run(main())


def test_map_reduce():
    check_values = []

    async def main():
        scheduler = RealtimeNetworkScheduler()
        network = scheduler.get_network()
        values = From(scheduler, [0.0, 3.2, 2.1, 2.9, 8.3, 5.7])
        mapper = Map(network, values, lambda x: round(x))
        accumulator = Scan(network, mapper)
        check_values.append(accumulator)
        Do(network, accumulator, lambda: print(f'{accumulator.get_value()}'))

    asyncio.run(main())
    assert check_values[0].get_value() == 22.0


def test_filter():
    check_values = []

    async def main():
        scheduler = RealtimeNetworkScheduler()
        network = scheduler.get_network()
        values = From(scheduler, [0.0, -3.2, 2.1, -2.9, 8.3, -5.7])
        filt = Filter(network, values, lambda x: x >= 0.0)
        check_values.append(filt)
        Do(network, filt, lambda: print(f'{filt.get_value()}'))

    asyncio.run(main())
    assert check_values[0].get_value() == 8.3


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


def test_repeating_timer():
    # run for one minute
    scheduler = HistoricNetworkScheduler(0, 60 * 1000)

    tz = pytz.timezone('US/Eastern')
    timer = RepeatingTimer(scheduler, timedelta(seconds=15))
    timestamps = list()
    Do(scheduler.get_network(), timer, lambda: timestamps.append(datetime.
                                                                 fromtimestamp(scheduler.get_time() / 1000.0, tz)))
    scheduler.run()
    assert len(timestamps) == 4
    assert str(timestamps[3]) == '1969-12-31 19:01:00-05:00'


def test_alarm():
    # run for one day
    scheduler = HistoricNetworkScheduler(0, 60 * 60 * 24 * 1000)

    # schedule at 4pm US/Eastern
    tz = pytz.timezone('US/Eastern')
    alarm = Alarm(scheduler, time(16, 00, 00), tz)
    timestamps = list()
    Do(scheduler.get_network(), alarm, lambda: timestamps.append(datetime.
                                                                 fromtimestamp(scheduler.get_time() / 1000.0, tz)))
    scheduler.run()
    assert len(timestamps) == 1
    assert str(timestamps[0]) == '1970-01-01 16:00:00-05:00'
