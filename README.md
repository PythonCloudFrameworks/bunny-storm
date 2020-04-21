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
pip install -U git+https://github.com/odedshimon/tornado-bunny
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
    A class that handles a single connection to a RabbitMQ server.
    Supports Tornado ioloop.
2. `TornadoAdapter` -
    A class encapsulating two async connections, one for publishing messages and one for consuming messages.
    Holds two dictionaries that stores the rpc related exchanges and corelation_id's.
3. `ChannelConfiguration` -
    A class wrapping a single RabbitMQ channel, using an AsyncConnection object.
    It provides both publish and consume capabilities.

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
