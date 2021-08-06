![badge](https://github.com/odedshimon/tornado-bunny/workflows/Tornado%20Bunny%20CI/CD/badge.svg)
[![PyPI version fury.io](https://badge.fury.io/py/tornado-bunny.svg)](https://pypi.python.org/pypi/tornado-bunny/)
[![codecov](https://codecov.io/gh/PythonCloudFrameworks/tornado-bunny/branch/master/graph/badge.svg?token=1HWGBGCV0G)](https://codecov.io/gh/PythonCloudFrameworks/tornado-bunny)
[![PyPI download month](https://img.shields.io/pypi/dm/tornado-bunny.svg)](https://pypi.python.org/pypi/tornado-bunny/)
[![PyPI pyversions](https://img.shields.io/pypi/pyversions/tornado-bunny.svg)](https://pypi.python.org/pypi/tornado-bunny/)
[![Ask Me Anything !](https://img.shields.io/badge/Ask%20me-anything-1abc9c.svg)](https://github.com/odedshimon/tornado-bunny/)

Tornado-Bunny
=============
RabbitMQ connector library for Python that is fully integrated with the [aio-pika framework](https://aio-pika.readthedocs.io/en/latest/).

Introduction
------------
Tornado-Bunny is here to simplify working with RabbitMQ while using aio-pika.
This library offers an asynchronous implementation of a RabbitMQ connector which is fully integrated with `asyncio`.
Tornado-Bunny provides an all-in-one adapter with the following functionalities:
1. publish - Publish a message.
2. receive - Consume messages from a queue. Can automatically reply to desired routes if the received message
             contains a "reply_to" property.
3. rpc - Implement RPC (Remote procedure call) logic using RabbitMQ. Publish a message with a reply_to property, wait for a reply message and return the reply's content.

Installation
------------
```bash
pip install -U tornado_bunny
```

Examples
-------
#### Simple Receiver (print messages from queue)
```python
import asyncio
from tornado_bunny import AsyncAdapter, RabbitMQConnectionData

RABBIT_URI = "amqp://guest:guest@127.0.0.1:5672/"

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    configuration = dict(
        publish=dict(
            exchange="some_ex",
            exchange_type="direct",
            routing_key="some_rk",
        ),
        receive=dict(
            exchange="some_receive_ex",
            exchange_type="direct",
            routing_key="some_rk",
            queue="some_q",
        )
    )
    connection_data = RabbitMQConnectionData(username="guest", password="guest", connection_name="example")
    adapter = AsyncAdapter(rabbitmq_connection_data=connection_data, configuration=configuration, loop=loop)
    loop.create_task(adapter.receive(handler=lambda msg: print(msg.body), queue="some_q"))
    loop.run_forever()
```

#### Full Microservices Using RPC pattern
Example of 2 Microservices implementing a fully scalable application that calculates a number in the Fibonacci series while implementing [RabbitMQ Remote procedure call (RPC)](https://www.rabbitmq.com/tutorials/tutorial-six-python.html) pattern, can be found at the examples directory.

Class Diagram
------------
![Class Diagram](class_diagram.jpeg)

Architecture
------------
1. `RabbitMQConnectionData` -
   A simple dataclass which contains all the relevant credentials and parameters necessary for opening a connection
   to a RabbitMQ server.
2. `AsyncConnection` -
   A class responsible for the management of a single connection to a RabbitMQ server.
   The class connects to a server whose credentials are specified in a `RabbitMQConnectionData` object passed to it.
   The main function of this class is **get_connection** which uses aio-pika to open a robust connection to the server.
3. `ChannelConfiguration` -
   A class which manages a single channel within a given RabbitMQ connection.
   This class encapsulates an AsyncConnection object, and exposes functionality to declare exchanges and queues.
   This class receives a connection (AsyncConnection from the previous paragraph) and parameters relevant to the creation of the channel.
4. `Publisher` -
   A class which creates and uses a `ChannelConfiguration` object to publish messages to a given exchange.
   Automatically declares the desired exchange with various configurable parameters, such as exchange type.
   The main function of this class is **publish**, which ensures that the instance's channel is open and that
   the relevant exchange has been declared, following which, it publishes a message to the exchange.
5. `Consumer` -
   A class which creates and uses a `ChannelConfiguration` object to consume messages from a given queue.
   Automatically declares the desired queue, and optionally an exchange as well, with various configurable parameters.
   The main functionality of this class is **consume**, which ensures that the instance's channel is open and that
   the relevant queue and exchange have been declared and bound as desired, following which, it consumes messages from
   the queue.
6. `AsyncAdapter` -
   A class which exposes all the desired functionality for this framework:
   1. **publish**: Publish a message to a given exchange.
   2. **receive**: Receive messages from a given queue. Messages received which have their `reply_to`
      parameter set will automatically have a response sent to them containing the message handler's result.
   3. **rpc**: Perform an RPC by publishing a message with its `reply_to` parameter set to the relevant value.

   To perform these operations, each adapter instance receives a `RabbitMQConnectionData` instance,
   which is used to create a `AsyncConnection` instance. This is in turn used to create the
   `Publisher` and `Consumer` instances necessary to work with the queues and exchanges specified
   in the configurations given to the `AsyncAdapter` in its constructor.
   Each instance of the class also maintains a dictionary of correlation IDs relevant to messages
   we are waiting on a response for, namely RPC requests.


Todo
----
* Implement Prometheus metrics support.
* Server example - refactor it to render real HTML

Notes
-----
This package is inspired by various implementations that I have encountered over the years.
The current version includes improvements and adjustments designed to improve integration
with technologies and frameworks developed over the last few years:
* Python 3.9
* aio-pika 6.8.0
* RabbitMQ Server 3.8.3 on Ubuntu 18
