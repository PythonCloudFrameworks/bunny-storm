![badge](https://github.com/odedshimon/tornado-bunny/workflows/Tornado%20Bunny%20CI/CD/badge.svg)
[![PyPI version fury.io](https://badge.fury.io/py/tornado-bunny.svg)](https://pypi.python.org/pypi/tornado-bunny/)
[![PyPI download month](https://img.shields.io/pypi/dm/tornado-bunny.svg)](https://pypi.python.org/pypi/tornado-bunny/)
[![PyPI pyversions](https://img.shields.io/pypi/pyversions/tornado-bunny.svg)](https://pypi.python.org/pypi/tornado-bunny/)
[![Ask Me Anything !](https://img.shields.io/badge/Ask%20me-anything-1abc9c.svg)](https://github.com/odedshimon/tornado-bunny/)

Tornado-Bunny
=============
RabbitMQ connector library for Python that is fully integrated with [Tornado Framework](http://www.tornadoweb.org).

Introduction
------------
Tornado-Bunny is here to simplify working with RabbitMQ while using Tornado framework.
This library offers asynchronous implementation of RabbitMQ connector that fully integrated with `tornado.ioloop`.
Tornado-Bunny connector is all-in-one connector that support number of functionalities:
1. publish - publish a message.
2. receive - consume messages from a queue. If received properties is not none, it publishes result back to "reply_to" queue.
3. rpc - publish a message with replay_to properties (correlation_id and queue name), waits for an answer message and returns value.

Installation
------------
```bash
pip install -U tornado_bunny
```

Examples
-------
#### Simple Reciever (print messages from queue)
```python
import tornado.ioloop
from tornado_bunny import TornadoAdapter

RABBIT_URI = "amqp://guest:guest@127.0.0.1:5672/"

if __name__ == "__main__":
    io_loop = tornado.ioloop.IOLoop.instance()
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
    rabbit_connection = TornadoAdapter(rabbitmq_url=RABBIT_URI, configuration=configuration, io_loop=io_loop)
    rabbit_connection.receive(handler=lambda msg: print(msg))
    io_loop.start()
```

#### Full Microservices Using RPC pattern
Example of 2 Microservices implementing a fully scalable application that calculates a number in the Fibonacci series while implementing [RabbitMQ Remote procedure call (RPC)](https://www.rabbitmq.com/tutorials/tutorial-six-python.html) pattern, can be found at the examples directory.

Architecture
------------
1. `AsyncConnection` -
    A class that handles a single connection to a RabbitMQ server. The class Constructor gets a parameter called “io_loop” assuming that the user who creates the connection will want to supply the io_loop by himself. This class supports both Tornado ioloop as well as asyncio ioloop. The main function of this class is get_connection() Which begins a series of asynchronous calls until an connection object is received using pika package.
2. `ChannelConfiguration` -
    A class that handles a single channel within a RabbitMQ connection. This class encapsulates an AsyncConnection object and it provides both publish and consume capabilities. This class gets a connection (AsyncConnection from the previous paragraph), an exchange, a routing key for publishing messages to and a queue to consume.
3. `TornadoBunny` -
    This class actually does the (all the) magic required for the RPC by using the classes we have built so far along with a few other little tricks.
The class encapsulating two async channels (and two connections, respectively for each channel). The first channel is used for publishing messages while the other one is used for consuming messages.
The class also has two dictionaries for storing the RPC related exchanges and correlation id’s state.
The receive() function is responsible for consuming messages. If received properties of consumed message is not none, it publishes result back to `reply_to` queue.
The publish() function is responsible for publishing a message to the given exchange.
The rpc() function is the function that implements the RPC logic. First, it consumes the receiving queue, then it generates a unique uuid that will be used as the correlation id and therefore it stored as a key at the dictionary which linking between a correlation id and the caller. Afterwards the correlation id is attached along with the reply queue to a message properties object that attached to the message which then will finally be sent. At this stage we are yielding on the _wait_result() function that will be called only when we will get the response back.


Todo
----
* Implement Prometheus metrics support.
* Enable passing an existing channel.
* Support asyncio ioloop.
* Server example - refactor it to render real HTML
* Write Tests.

Notes
-----
This package was inspired by various implementations that I have encountered over the years, especially on Python 2.7 versions.
The current version including improvements and adjustments that enables to integrate with the most updated frameworks at the time that this package was developed:
* Python 3.8
* pika 1.1.0
* tornado 6.0.4
* RabbitMQ Server 3.8.3 on Ubuntu 18
