import asyncio
from aio_pika import RobustExchange, RobustQueue

from tests.conftest import run_coroutine_to_completion, channel_configuration_teardown
from tornado_bunny import AsyncConnection, ChannelConfiguration


def test_channel_configuration_creation(async_connection: AsyncConnection, loop: asyncio.AbstractEventLoop) -> None:
    # Arrange
    channel_config = ChannelConfiguration(
        async_connection,
        async_connection.logger,
        loop=loop
    )

    # Act
    channel = run_coroutine_to_completion(channel_config.ensure_channel())
    channel_copy = run_coroutine_to_completion(channel_config.ensure_channel())

    # Assert
    assert channel == channel_copy
    assert not channel.is_closed

    # Teardown
    channel_configuration_teardown(channel_config)


def test_channel_configuration_declare_exchange(channel_config: ChannelConfiguration) -> None:
    # Arrange
    expected_exchange_name = "creation_test"
    expected_exchange_type = "direct"
    expected_durable = False
    expected_auto_delete = True

    # Act
    exchange = run_coroutine_to_completion(channel_config.declare_exchange(expected_exchange_name,
                                                                           expected_exchange_type,
                                                                           expected_durable,
                                                                           expected_auto_delete))

    # Assert
    assert isinstance(exchange, RobustExchange)
    assert exchange.name == expected_exchange_name
    assert exchange._Exchange__type == expected_exchange_type  # Private variable testing is weird
    assert exchange.durable == expected_durable
    assert exchange.auto_delete == expected_auto_delete


def test_channel_configuration_declare_exchange_queue(channel_config: ChannelConfiguration) -> None:
    # Arrange
    expected_exchange_name = "creation_test"
    expected_exchange_type = "direct"
    expected_queue_name = "creation_test_q"
    expected_routing_key = "#"
    expected_durable = False
    expected_auto_delete = True

    # Act
    exchange = run_coroutine_to_completion(channel_config.declare_exchange(expected_exchange_name,
                                                                           expected_exchange_type,
                                                                           expected_durable,
                                                                           expected_auto_delete))
    queue = run_coroutine_to_completion(channel_config.declare_queue(expected_queue_name,
                                                                     exchange,
                                                                     expected_routing_key,
                                                                     expected_durable,
                                                                     expected_auto_delete))

    # Assert
    assert isinstance(queue, RobustQueue)
    assert queue.durable == expected_durable
    assert queue.auto_delete == expected_auto_delete
    assert list(queue._bindings) == [(exchange, expected_routing_key)]


def test_channel_configuration_close_reopen(channel_config: ChannelConfiguration):
    # Act
    run_coroutine_to_completion(channel_config.ensure_channel())
    run_coroutine_to_completion(channel_config._channel.close())

    # Assert
    assert channel_config._channel.is_closed

    # Act
    run_coroutine_to_completion(channel_config.ensure_channel())

    # Assert
    assert not channel_config._channel.is_closed
