import asyncio
import logging
import os
import sys
from typing import Any, Coroutine, Union

import pytest

from tornado_bunny import RabbitMQConnectionData, AsyncConnection, ChannelConfiguration, Consumer, Publisher, \
    IntentionalCloseChannelError

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
def loop() -> asyncio.AbstractEventLoop:
    return asyncio.get_event_loop()


@pytest.fixture(scope="function")
def async_connection(rabbitmq_connection_data: RabbitMQConnectionData, loop: asyncio.AbstractEventLoop,
                     logger: logging.Logger) -> AsyncConnection:
    return AsyncConnection(rabbitmq_connection_data, logger, loop)


@pytest.fixture(scope="function")
def channel_config(async_connection: AsyncConnection, logger: logging.Logger,
                   loop: asyncio.AbstractEventLoop) -> ChannelConfiguration:
    channel_config = ChannelConfiguration(async_connection, logger, loop)
    yield channel_config
    # Teardown
    channel_configuration_teardown(channel_config)


@pytest.fixture(scope="function")
def publisher(async_connection: AsyncConnection, loop: asyncio.AbstractEventLoop, logger: logging.Logger,
              configuration: dict) -> Publisher:
    publisher = Publisher(async_connection, logger, **configuration["publish"])
    yield publisher
    # Teardown
    publisher_teardown(publisher)


@pytest.fixture(scope="function")
def consumer(async_connection: AsyncConnection, loop: asyncio.AbstractEventLoop, logger: logging.Logger,
             configuration: dict) -> ChannelConfiguration:
    consumer = Consumer(async_connection, logger, loop, **configuration["receive"])
    yield consumer
    # Teardown
    consumer_teardown(consumer)


def channel_configuration_teardown(channel_configuration: ChannelConfiguration) -> None:
    if channel_configuration._channel:
        run_coroutine_to_completion(channel_configuration._channel.close(exc=IntentionalCloseChannelError("Teardown")))
        channel_configuration._channel = None


def consumer_teardown(consumer: Consumer) -> None:
    if consumer._queue:
        run_coroutine_to_completion(consumer._queue.delete(if_unused=False, if_empty=False))
        consumer._queue = None
    if consumer._exchange:
        run_coroutine_to_completion(consumer._exchange.delete(if_unused=False))
        consumer._exchange = None
    channel_configuration_teardown(consumer.channel_config)


def publisher_teardown(publisher: Publisher) -> None:
    if publisher._exchange:
        run_coroutine_to_completion(publisher._exchange.delete(if_unused=False))
        publisher._exchange = None
    channel_configuration_teardown(publisher.channel_config)


def run_coroutine_to_completion(coroutine: Coroutine) -> Any:
    loop = asyncio.get_event_loop()
    return loop.run_until_complete(asyncio.gather(coroutine))[0]


async def collect_future(future: asyncio.Future, timeout: Union[int, float]) -> Union[bytes, None]:
    try:
        await asyncio.wait_for(future, timeout=timeout)
        return future.result()
    except asyncio.exceptions.TimeoutError:
        return
