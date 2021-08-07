import asyncio
import logging
import os
import sys
from typing import Union

import pytest

from tornado_bunny import RabbitMQConnectionData, AsyncConnection, ChannelConfiguration, Consumer, Publisher, \
    IntentionalCloseChannelError

if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


@pytest.fixture(scope="session")
def rabbitmq_user() -> str:
    return os.getenv("RABBITMQ_USER", "guest")


@pytest.fixture(scope="session")
def rabbitmq_password() -> str:
    return os.getenv("RABBITMQ_PASSWORD", "guest")


@pytest.fixture(scope="session")
def rabbitmq_host() -> str:
    return os.getenv("RABBITMQ_HOST", "localhost")


@pytest.fixture(scope="session")
def rabbitmq_port() -> int:
    return int(os.getenv("RABBITMQ_PORT", "5672"))


@pytest.fixture(scope="session")
def rabbitmq_virtual_host() -> str:
    return os.getenv("RABBITMQ_VIRTUAL_HOST", "/")


@pytest.fixture(scope="function")
def rabbitmq_connection_data(rabbitmq_user: str, rabbitmq_password: str, rabbitmq_host: str, rabbitmq_port: int,
                             rabbitmq_virtual_host: str) -> RabbitMQConnectionData:
    connection_data = RabbitMQConnectionData(username=rabbitmq_user,
                                             password=rabbitmq_password,
                                             host=rabbitmq_host,
                                             port=rabbitmq_port,
                                             virtual_host=rabbitmq_virtual_host,
                                             connection_name="test_runner")
    return connection_data


@pytest.fixture(scope="session")
def configuration() -> dict:
    return dict(
        publish=dict(
            exchange_name="test_pub",
            exchange_type="direct",
            routing_key="unit_test",
            durable=False,
            auto_delete=True,
            prefetch_count=1
        ),
        receive=dict(
            exchange_name="test_pub",
            exchange_type="direct",
            routing_key="unit_test",
            queue_name="unit_test",
            durable=False,
            auto_delete=True,
            prefetch_count=1
        )
    )


@pytest.fixture(scope="session")
def logger() -> logging.Logger:
    return logging.getLogger(__name__)


@pytest.fixture(scope="function")
def async_connection(rabbitmq_connection_data: RabbitMQConnectionData, event_loop: asyncio.AbstractEventLoop,
                     logger: logging.Logger) -> AsyncConnection:
    return AsyncConnection(rabbitmq_connection_data, logger, event_loop)


@pytest.fixture(scope="function")
async def channel_config(async_connection: AsyncConnection, logger: logging.Logger,
                         event_loop: asyncio.AbstractEventLoop) -> ChannelConfiguration:
    channel_config = ChannelConfiguration(async_connection, logger, event_loop)
    yield channel_config
    # Teardown
    await channel_configuration_teardown(channel_config)


@pytest.fixture(scope="function")
async def publisher(async_connection: AsyncConnection, logger: logging.Logger, configuration: dict) -> Publisher:
    publisher = Publisher(async_connection, logger, **configuration["publish"])
    yield publisher
    # Teardown
    await publisher_teardown(publisher)


@pytest.fixture(scope="function")
async def consumer(async_connection: AsyncConnection, logger: logging.Logger, configuration: dict) -> Consumer:
    consumer = Consumer(async_connection, logger, **configuration["receive"])
    yield consumer
    # Teardown
    await consumer_teardown(consumer)


async def channel_configuration_teardown(channel_configuration: ChannelConfiguration) -> None:
    if channel_configuration._channel:
        await channel_configuration._channel.close(exc=IntentionalCloseChannelError("Teardown"))
        channel_configuration._channel = None


async def consumer_teardown(consumer: Consumer) -> None:
    if consumer._queue:
        await consumer._queue.delete(if_unused=False, if_empty=False)
        consumer._queue = None
    if consumer._exchange:
        await consumer._exchange.delete(if_unused=False)
        consumer._exchange = None
    await channel_configuration_teardown(consumer.channel_config)


async def publisher_teardown(publisher: Publisher) -> None:
    if publisher._exchange:
        await publisher._exchange.delete(if_unused=False)
        publisher._exchange = None
    await channel_configuration_teardown(publisher.channel_config)


async def collect_future(future: asyncio.Future, timeout: Union[int, float]) -> Union[bytes, None]:
    try:
        await asyncio.wait_for(future, timeout=timeout)
        return future.result()
    except asyncio.exceptions.TimeoutError:
        return
