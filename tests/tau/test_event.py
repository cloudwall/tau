from datetime import timedelta, datetime, time

import pytest
import pytz

from tau.core import HistoricNetworkScheduler
from tau.event import Do, Alarm, RepeatingTimer


@pytest.mark.skip(reason="hangs on CI build")
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


@pytest.mark.skip(reason="hangs on CI build")
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
