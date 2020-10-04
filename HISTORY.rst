.. :changelog:

Release History
---------------

0.9.3 (2020-10-04)
++++++++++++++++++

- Third attempt to fix thread starvation in recursive event handlers

0.9.2 (2020-10-03)
++++++++++++++++++

- Another attempt to fix thread starvation in recursive event handlers

0.9.1 (2020-10-03)
++++++++++++++++++

- Fixed critical event stall

0.9.0 (2020-10-03)
++++++++++++++++++

- Switched to producer-consumer queue model in RealtimeNetworkScheduler

0.8.0 (2020-09-26)
++++++++++++++++++

- Ensured stable insertion-based sort for events entered with same time

0.7.3 (2020-09-19)
++++++++++++++++++

- Updated examples

0.7.2 (2020-09-19)
++++++++++++++++++

- Added unit test for BufferWithTime
- Fixed bugs in BufferWithTime

0.7.1 (2020-09-19)
++++++++++++++++++

- Fixed BufferWithTime to work in historic mode

0.7.0 (2020-09-19)
++++++++++++++++++

- Moved setting of start/end time to HistoricNetworkScheduler constructor

0.6.1 (2020-09-18)
++++++++++++++++++

- Added get_start_time() and get_end_time() to HistoricNetworkScheduler

0.6.0 (2020-09-18)
++++++++++++++++++

- Split NetworkScheduler into Realtime and Historical variants

0.5.1 (2020-05-24)
++++++++++++++++++

- Added MANIFEST.in to fix broken builds

0.5.0 (2020-05-12)
++++++++++++++++++

- Added WindowWithCount operator

0.4.3 (2020-05-02)
++++++++++++++++++

- Added more unit tests
- Fixed build badge target URL

0.4.2 (2020-05-02)
++++++++++++++++++

- Added more unit tests
- Removed declared support for Python 3.6; missing asyncio functions

0.4.1 (2020-05-02)
++++++++++++++++++

- Added unit tests
- Integrated Azure DevOps build pipeline

0.4.0 (2020-04-30)
++++++++++++++++++

- Rewrote core graph functions using graph-theory; removed networkx dependency
- Properly fixed case where next sibling node activation skipped
- Added Network#attach() method to explicitly add a node to the graph without connecting it

0.3.1 (2020-04-26)
++++++++++++++++++

- Fixed case where next sibling node activation skipped

0.3.0 (2020-04-25)
++++++++++++++++++

- Added FlatMap operator

0.2.0 (2020-04-13)
++++++++++++++++++

- Switched back to Python 3.7.x

0.1.1 (2020-04-04)
+++++++++++++++++++

- Critical fix to setup.py to pick up package source
- Switch to using Do operator in hello_world.py example
- Improve the subscribe_trades.py example

0.1.0 (2020-04-04)
+++++++++++++++++++

- Remove dependency on APScheduler
- Rewrite to use asyncio internally
- Added websocket example
- Switched to require Python version >= 3.8

0.0.2 (2020-03-28)
+++++++++++++++++++

- Renamed OneShot to From and ForEach to Do
- Added BufferWithCount, BufferWithTime, Interval, Just and Scan operators
- Improved documentation

0.0.1 (2020-03-28)
+++++++++++++++++++

- Initial implementation
