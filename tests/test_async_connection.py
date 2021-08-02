import asyncio
import logging

import pytest
from aio_pika import RobustConnection

from tests.conftest import run_coroutine_to_completion
from tornado_bunny import RabbitMQConnectionData, AsyncConnection


def test_async_connection_get_connection(loop: asyncio.AbstractEventLoop,
                                         logger: logging.Logger,
                                         rabbitmq_connection_data: RabbitMQConnectionData) -> None:
    # Arrange
    async_connection = AsyncConnection(rabbitmq_connection_data, logger, loop)

    # Act
    connection = run_coroutine_to_completion(async_connection.get_connection())

    # Assert
    assert isinstance(connection, RobustConnection) and async_connection.is_connected()


def test_async_connection_connection_failure(loop: asyncio.AbstractEventLoop,
                                             logger: logging.Logger,
                                             rabbitmq_connection_data: RabbitMQConnectionData) -> None:
    # Arrange
    rabbitmq_connection_data.host = "1.1.1.1"
    async_connection = AsyncConnection(rabbitmq_connection_data, logger, loop, connection_attempts=1, timeout=0.1)

    # Act
    with pytest.raises(RuntimeError):
        run_coroutine_to_completion(async_connection.get_connection())


def test_async_connection_close_connection(loop: asyncio.AbstractEventLoop,
                                           logger: logging.Logger,
                                           rabbitmq_connection_data: RabbitMQConnectionData) -> None:
    # Arrange
    async_connection = AsyncConnection(rabbitmq_connection_data, logger, loop)
    connection = run_coroutine_to_completion(async_connection.get_connection())

    # Act
    run_coroutine_to_completion(async_connection.close())

    # Assert
    assert not async_connection.is_connected()

    # Act
    new_connection = run_coroutine_to_completion(async_connection.get_connection())

    # Assert
    assert isinstance(new_connection, RobustConnection) and async_connection.is_connected()
    assert async_connection.is_connected() and connection != new_connection
