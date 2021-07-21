#!/usr/bin/env python

"""Tests for `tornado_bunny` package."""


import asyncio
import logging
import sys
import os
from typing import Any

import pytest
from aio_pika import RobustConnection, Message, IncomingMessage

from tornado_bunny import AsyncConnection, ChannelConfiguration, RabbitMQConnectionData

if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


@pytest.fixture(scope="session")
def rabbitmq_user() -> str:
    return os.getenv("RABBITMQ_USER", "test_user")


@pytest.fixture(scope="session")
def rabbitmq_password() -> str:
    return os.getenv("RABBITMQ_PASSWORD", "pass123")


@pytest.fixture(scope="session")
def rabbitmq_host() -> str:
    return os.getenv("RABBITMQ_HOST", "localhost")


@pytest.fixture(scope="session")
def rabbitmq_port() -> int:
    return int(os.getenv("RABBITMQ_PORT", "5672"))


@pytest.fixture(scope="session")
def rabbitmq_virtual_host() -> str:
    return os.getenv("RABBITMQ_VIRTUAL_HOST", "vhost")


@pytest.fixture(scope="session")
def rabbitmq_connection_data(rabbitmq_user: str, rabbitmq_password: str, rabbitmq_host: str, rabbitmq_port: int,
                             rabbitmq_virtual_host: str) -> RabbitMQConnectionData:
    connection_data = RabbitMQConnectionData(username=rabbitmq_user,
                                             password=rabbitmq_password,
                                             host=rabbitmq_host,
                                             port=rabbitmq_port,
                                             virtual_host=rabbitmq_virtual_host)
    return connection_data


@pytest.fixture(scope="session")
def configuration() -> dict:
    return dict(
        publish=dict(
            exchange_name="test_pub",
            exchange_type="direct",
            routing_key="test",
            queue_name="test",
            durable=False,
            auto_delete=True,
            prefetch_count=1
        ),
        receive=dict(
            routing_key="test",
            queue_name="test",
            durable=False,
            auto_delete=True,
            prefetch_count=1
        )
    )


def run_coroutine_to_completion(loop: asyncio.AbstractEventLoop, coroutine, *args, **kwargs) -> Any:
    return loop.run_until_complete(asyncio.gather(coroutine(*args, **kwargs)))[0]


def test_async_connection(rabbitmq_connection_data: RabbitMQConnectionData) -> None:
    loop = asyncio.get_event_loop()
    async_connection = AsyncConnection(rabbitmq_connection_data, logging.getLogger(__name__), loop)

    # Test first connection to RabbitMQ
    connection = run_coroutine_to_completion(loop, async_connection.get_connection)
    assert isinstance(connection, RobustConnection) and async_connection.is_connected()

    # Test that closing the connection results in reconnecting automatically without needing to manually reconnect
    run_coroutine_to_completion(loop, async_connection.close)
    new_connection = run_coroutine_to_completion(loop, async_connection.get_connection)
    assert async_connection.is_connected() and connection != new_connection


class TestChannelConfiguration:
    MESSAGE_BODY: bytes = b"test_message"
    _test_future: asyncio.Future = None

    def test_channel_configuration(self, rabbitmq_connection_data, configuration):
        loop = asyncio.get_event_loop()
        self.loop = loop
        async_connection = AsyncConnection(rabbitmq_connection_data, logging.getLogger(__name__), loop)

        publish_channel = ChannelConfiguration(
            async_connection, async_connection.logger, loop, **configuration["publish"])
        receive_channel = ChannelConfiguration(
            async_connection, async_connection.logger, loop, **configuration["receive"])

        # Test channel creation and getter from channel async queue
        channel = run_coroutine_to_completion(loop, publish_channel._get_channel)
        assert not channel.is_closed

        # Publish message and check that it uses the same channel
        message = Message(self.MESSAGE_BODY)
        run_coroutine_to_completion(loop, publish_channel.publish, message=message)
        assert run_coroutine_to_completion(loop, publish_channel._get_channel) == channel

        # Start consuming and wait for message
        message_body = run_coroutine_to_completion(loop, self.check_consume, loop, receive_channel)
        # Stop the loop, in order to stop consuming
        loop.stop()
        assert message_body == self.MESSAGE_BODY

    async def check_consume(self, loop: asyncio.AbstractEventLoop, receive_channel: ChannelConfiguration) -> None:
        self._test_future = loop.create_future()
        await receive_channel.consume(self.message_handler)
        try:
            await asyncio.wait_for(self._test_future, timeout=10)
            return self._test_future.result()
        except asyncio.exceptions.TimeoutError:
            return

    async def message_handler(self, message: IncomingMessage) -> None:
        self._test_future.set_result(message.body)
