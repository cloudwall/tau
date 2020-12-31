import datetime

import pytz

from tau.core import HistoricNetworkScheduler
from tau.event import Do, Alarm, RepeatingTimer


def test_repeating_timer():
    # run for one minute
    scheduler = HistoricNetworkScheduler(0, 60 * 1000)

    tz = pytz.timezone('US/Eastern')
    timer = RepeatingTimer(scheduler, datetime.timedelta(seconds=15))
    timestamps = list()
    Do(scheduler.get_network(), timer, lambda: timestamps.append(scheduler.get_clock().get_time(tz)))
    scheduler.run()
    assert len(timestamps) == 4
    assert str(timestamps[3]) == '1969-12-31 19:01:00-05:00'


def test_alarm():
    # run for one day
    scheduler = HistoricNetworkScheduler(0, 60 * 60 * 24 * 1000)

    # schedule at 4pm US/Eastern
    tz = pytz.timezone('US/Eastern')
    alarm = Alarm(scheduler, datetime.time(16, 00, 00), tz)
    timestamps = list()
    Do(scheduler.get_network(), alarm, lambda: timestamps.append(scheduler.get_clock().get_time(tz)))
    scheduler.run()
    assert len(timestamps) == 1
