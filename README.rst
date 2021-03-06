Tau: Python Reactive Programming
================================

.. image:: https://dev.azure.com/cloudwall/Tau/_apis/build/status/cloudwall.pytau?branchName=master
    :target: https://dev.azure.com/cloudwall/Tau/_build/latest?definitionId=3

.. image:: https://img.shields.io/pypi/v/pytau.svg
    :target: https://pypi.org/project/pytau/

.. image:: https://img.shields.io/pypi/l/pytau.svg
    :target: https://pypi.org/project/pytau/

.. image:: https://img.shields.io/pypi/pyversions/pytau.svg
    :target: https://pypi.org/project/pytau/

*A library for composing asynchronous and event-based programs in Python*

About Tau
---------

Tau is a `functional reactive programming <https://en.wikipedia.org/wiki/Functional_reactive_programming>`_ framework
designed for Python from the ground up.

.. code:: python

    import asyncio

    from tau.core import RealtimeNetworkScheduler
    from tau.event import Lambda
    from tau.signal import From


    async def main():
        scheduler = RealtimeNetworkScheduler()
        signal = From(scheduler, ["world"])
        Lambda(scheduler.get_network(), signal, lambda x: print(f"Hello, {x[0].get_value()}!"))

    asyncio.run(main())

Installation
------------

Tau runs on `Python <http://www.python.org/>`_ 3.6 or above. To install Tau:

.. code:: console

    pip3 install pytau

Credits
-------

Thanks to *Bjorn Madsen* for adding a feature to graph-theory to make Tau's event propagation work.