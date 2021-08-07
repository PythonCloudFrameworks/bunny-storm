import asyncio

import pytest
from aio_pika import RobustExchange, RobustQueue

from tests.conftest import channel_configuration_teardown
from tornado_bunny import AsyncConnection, ChannelConfiguration


@pytest.mark.asyncio
async def test_channel_configuration_creation(async_connection: AsyncConnection, event_loop: asyncio.AbstractEventLoop):
    # Arrange
    channel_config = ChannelConfiguration(
        async_connection,
        async_connection.logger,
        loop=event_loop
    )

    # Act
    channel = await channel_config.ensure_channel()
    channel_copy = await channel_config.ensure_channel()

    # Assert
    assert channel is channel_copy
    assert not channel.is_closed

    # Teardown
    await channel_configuration_teardown(channel_config)


@pytest.mark.asyncio
async def test_channel_configuration_declare_exchange(channel_config: ChannelConfiguration) -> None:
    # Arrange
    expected_exchange_name = "creation_test"
    expected_exchange_type = "direct"
    expected_durable = False
    expected_auto_delete = True

    # Act
    exchange = await channel_config.declare_exchange(expected_exchange_name,
                                                     expected_exchange_type,
                                                     expected_durable,
                                                     expected_auto_delete)
    exchange_copy = await channel_config.declare_exchange(expected_exchange_name,
                                                          expected_exchange_type,
                                                          expected_durable,
                                                          expected_auto_delete)

    # Assert
    assert isinstance(exchange, RobustExchange)
    assert exchange is exchange_copy
    assert exchange.name == expected_exchange_name
    assert exchange._Exchange__type == expected_exchange_type  # Private variable testing is weird
    assert exchange.durable == expected_durable
    assert exchange.auto_delete == expected_auto_delete


@pytest.mark.asyncio
async def test_channel_configuration_declare_exchange_queue(channel_config: ChannelConfiguration) -> None:
    # Arrange
    expected_exchange_name = "creation_test"
    expected_exchange_type = "direct"
    expected_queue_name = "creation_test_q"
    expected_routing_key = "#"
    expected_durable = False
    expected_auto_delete = True

    # Act
    exchange = await channel_config.declare_exchange(expected_exchange_name,
                                                     expected_exchange_type,
                                                     expected_durable,
                                                     expected_auto_delete)
    queue = await channel_config.declare_queue(expected_queue_name,
                                               exchange,
                                               expected_routing_key,
                                               expected_durable,
                                               expected_auto_delete)
    queue_copy = await channel_config.declare_queue(expected_queue_name,
                                                    exchange,
                                                    expected_routing_key,
                                                    expected_durable,
                                                    expected_auto_delete)

    # Assert
    assert isinstance(queue, RobustQueue)
    assert queue is queue_copy
    assert queue.durable == expected_durable
    assert queue.auto_delete == expected_auto_delete
    assert list(queue._bindings) == [(exchange, expected_routing_key)]


@pytest.mark.asyncio
async def test_channel_configuration_close_reopen(channel_config: ChannelConfiguration):
    # Act
    await channel_config.ensure_channel()
    await channel_config._channel.close()

    # Assert
    assert channel_config._channel.is_closed

    # Act
    await channel_config.ensure_channel()

    # Assert
    assert not channel_config._channel.is_closed
