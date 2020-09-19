import asyncio

from tau.core import RealtimeNetworkScheduler
from tau.event import Do
from tau.signal import From


async def main():
    scheduler = RealtimeNetworkScheduler()
    signal = From(scheduler, ["world"])
    Do(scheduler.get_network(), signal, lambda: print(f"Hello, {signal.get_value()}!"))

asyncio.run(main())
