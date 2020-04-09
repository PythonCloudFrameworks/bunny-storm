Tornado-Bunny
=============
Tornado-Bunny is a RabbitMQ connector library for Python that is fully integrated with [Tornado Framework](http://www.tornadoweb.org).

Introduction
------------
Tornado-Bunny is here to simplify working with RabbitMQ while using Tornado framework.
This library offers asynchronous implementation of RabbitMQ connector that fully integrated with `tornado.ioloop`.
Tornado-Bunny connector is all-in-one connector that support number of functionalities:
1. publish - publish message.
2. receive - consume messages from a queue. If received properties is not none, it publishes result back to "reply_to" queue.
3. rpc - publish a message with replay_to properties (correlation_id and queue name), waits for an answer message and returns value. 

Examples
-------
Example of 2 Microservices implementing a fully scalable application that calculates a number in the Fibonacci series while implementing [RabbitMQ Remote procedure call (RPC)](https://www.rabbitmq.com/tutorials/tutorial-six-python.html) pattern, can be found at the examples directory.

Architecture
------------
1. `AsyncConnection` - 
    A class that handles a single connection to a RabbitMQ server.
    Supports Tornado ioloop.
2. `TornadoAdapter` - 
    A class encapsulating two async connections, one for publishing messages and one for consuming messages.
    Holds two dictionaries that stores the rpc related exchanges and corelation_id's.

Todo
----
* Implement Prometheus metrics support.
* Enable passing an existing channel.
* Support asyncio ioloop.
* Create setup.py and configure as a Python package.
* Write Tests.

Notes
-----
This package was inspired by various implementations that I have encountered over the years, especially on Python 2.7 versions.
The current version including improvements and adjustments that enables to integrate with the most updated frameworks at the time that this package was developed:
* Python 3.8
* pika 1.1.0
* tornado 6.0.4
* RabbitMQ Server 3.8.3 on Ubuntu 18
 