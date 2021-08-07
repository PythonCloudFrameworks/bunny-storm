import asyncio
import logging

import pytest
from aio_pika import RobustConnection

from tornado_bunny import RabbitMQConnectionData, AsyncConnection


@pytest.mark.asyncio
async def test_async_connection_get_connection(event_loop: asyncio.AbstractEventLoop,
                                               logger: logging.Logger,
                                               rabbitmq_connection_data: RabbitMQConnectionData) -> None:
    # Arrange
    async_connection = AsyncConnection(rabbitmq_connection_data, logger, event_loop)

    # Act
    connection = await async_connection.get_connection()

    # Assert
    assert isinstance(connection, RobustConnection) and async_connection.is_connected()


@pytest.mark.asyncio
async def test_async_connection_connection_failure(event_loop: asyncio.AbstractEventLoop,
                                                   logger: logging.Logger,
                                                   rabbitmq_connection_data: RabbitMQConnectionData) -> None:
    # Arrange
    rabbitmq_connection_data.host = "1.1.1.1"
    async_connection = AsyncConnection(rabbitmq_connection_data,
                                       logger,
                                       event_loop,
                                       connection_attempts=2,
                                       timeout=0.1,
                                       attempt_backoff=1)

    # Act
    with pytest.raises(ConnectionError):
        await async_connection.get_connection()


@pytest.mark.asyncio
async def test_async_connection_close_connection(event_loop: asyncio.AbstractEventLoop,
                                                 logger: logging.Logger,
                                                 rabbitmq_connection_data: RabbitMQConnectionData) -> None:
    # Arrange
    async_connection = AsyncConnection(rabbitmq_connection_data, logger, event_loop)
    connection = await async_connection.get_connection()

    # Act
    await async_connection.close()

    # Assert
    assert not async_connection.is_connected()

    # Act
    await async_connection.close()

    # Assert
    assert not async_connection.is_connected()

    # Act
    new_connection = await async_connection.get_connection()

    # Assert
    assert isinstance(new_connection, RobustConnection) and async_connection.is_connected()
    assert async_connection.is_connected() and connection != new_connection
