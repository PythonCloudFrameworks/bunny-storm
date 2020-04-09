Tornado-Bunny
=============
Tornado-Bunny is a RabbitMQ connector library for Python that is fully integrated with [Tornado Framework](http://www.tornadoweb.org).

Introduction
------------
Tornado-Bunny is here to simplify working with RabbitMQ while using Tornado framework.
This library offers asynchronous implementation of RabbitMQ connector that supports `tornado.ioloop`.
Tornado-Bunny connector is all-in-one connector that support number of functionalities:
1. publish - publish message.
2. receive - consume messages from a queue. If received properties is not none, it publishes result back to "reply_to" queue.
3. rpc - publish a message with replay_to properties (coorlation_id and queue name), wait for answer message and return value. 

Examples
-------
Example of 2 Microservices implementing a fully scalable application that calculate a number in the Fibonacci series while implementing [RabbitMQ Remote procedure call (RPC)](https://www.rabbitmq.com/tutorials/tutorial-six-python.html) pattern, can be found at the examples directory.

Architecture
------------
1. `AsyncConnection` - 
    a class that handles a single connection to a RabbitMQ server.
    Supports Tornado ioloop.
2. `TornadoAdapter` - 
    a class encapsulate two async connections, one for publishing messages and one for consuming messages.
    also holds two dictionaries that stores the rpc related exchanges and corelation_id's

Todo
----
* Implement Prometheus metrics support.
* Enable passing an existing channel
* Write Tests
* Support asyncio ioloop
