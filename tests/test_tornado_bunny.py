#!/usr/bin/env python

"""Tests for `tornado_bunny` package."""


import functools
import asyncio
import logging
import sys
import os

import pytest
from tornado import gen
from tornado.ioloop import IOLoop
from tornado.queues import Queue
from pika.adapters.tornado_connection import TornadoConnection

from tornado_bunny import AsyncConnection, ChannelConfiguration

if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


@pytest.fixture(scope="session")
def rabbitmq_host():
    return os.getenv("RABBITMQ_HOST", "localhost")


@pytest.fixture(scope="session")
def rabbitmq_user():
    return os.getenv("RABBITMQ_USER", "test_user")


@pytest.fixture(scope="session")
def rabbitmq_password():
    return os.getenv("RABBITMQ_PASSWORD", "pass123")


@pytest.fixture(scope="session")
def rabbitmq_url(rabbitmq_user, rabbitmq_password, rabbitmq_host):
    return f"amqp://{rabbitmq_user}:{rabbitmq_password}@{rabbitmq_host}:5672/"


@pytest.fixture(scope="session")
def configuration():
    return dict(
        publish=dict(
            exchange="test_pub",
            exchange_type="direct",
            routing_key="test",
            queue="test",
            durable=False,
            auto_delete=True,
            prefetch_count=1
        ),
        receive=dict(
            routing_key="test",
            queue="test",
            durable=False,
            auto_delete=True,
            prefetch_count=1
        )
    )


def test_async_connection(rabbitmq_url):
    io_loop = IOLoop.current()
    async_connection = AsyncConnection(rabbitmq_url, io_loop, logging.getLogger(__name__))

    # Test first connection to RabbitMQ
    connection = io_loop.run_sync(async_connection.get_connection)
    assert isinstance(connection, TornadoConnection) and connection.is_open

    # Test asynchronous connection queue method
    connection_top = io_loop.run_sync(async_connection._top)
    assert connection_top == connection

    # Test that closing the connection results in reconnecting automatically without needing to manually reconnect
    async_connection._connection_queue.get_nowait()
    connection.close()
    connection = io_loop.run_sync(async_connection._top)
    assert connection.is_open and connection != connection_top


class TestChannelConfiguration:
    MESSAGE = "test_message"

    @gen.coroutine
    def _top(self):
        message = yield self._message_queue.get()
        self._message_queue.put(message)
        return message

    def test_channel_configuration(self, rabbitmq_url, configuration):
        self._message_queue = Queue(maxsize=1)
        io_loop = IOLoop.current()
        self.io_loop = io_loop
        async_connection = AsyncConnection(rabbitmq_url, io_loop, logging.getLogger(__name__))

        publish_channel = ChannelConfiguration(
            async_connection, async_connection.logger, io_loop, **configuration["publish"])
        receive_channel = ChannelConfiguration(
            async_connection, async_connection.logger, io_loop, **configuration["receive"])

        # Test channel creation and getter from channel async queue
        channel = io_loop.run_sync(publish_channel._get_channel)
        assert channel.is_open

        # Publish message and check that it uses the same channel
        io_loop.run_sync(functools.partial(publish_channel.publish, self.MESSAGE))
        assert io_loop.run_sync(publish_channel._get_channel) == channel

        # Start consuming and wait for message
        io_loop.spawn_callback(receive_channel.consume, self.callback)
        message = io_loop.run_sync(self._top, 10)
        # Stop the loop, in order to stop consuming
        io_loop.stop()
        assert message == self.MESSAGE

    def callback(self, channel, method, properties, body):
        body = body.decode()
        print(f"consumed: {body}")
        self._message_queue.put(body)
