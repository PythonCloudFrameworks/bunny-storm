import asyncio
from typing import Union

import pytest
from aio_pika import IncomingMessage, RobustExchange, Message, RobustQueue

from tests.conftest import run_coroutine_to_completion, channel_configuration_teardown
from tornado_bunny import AsyncConnection, ChannelConfiguration


class TestChannelConfiguration:
    _test_future: asyncio.Future = None

    @pytest.fixture(scope="function")
    def message(self) -> Message:
        return Message(b"test message")

    def test_channel_configuration_creation(self,
                                            async_connection: AsyncConnection,
                                            loop: asyncio.AbstractEventLoop) -> None:

        # Arrange
        expected_exchange_name = "creation_test"
        expected_exchange_type = "direct"
        expected_queue_name = "creation_test_q"
        expected_routing_key = "#"
        expected_durable = False
        expected_auto_delete = True

        channel_config = ChannelConfiguration(
            async_connection,
            async_connection.logger,
            loop=loop,
            exchange_name=expected_exchange_name,
            exchange_type=expected_exchange_type,
            queue_name=expected_queue_name,
            routing_key=expected_routing_key,
            durable=expected_durable,
            auto_delete=expected_auto_delete,
        )

        # Act
        channel = run_coroutine_to_completion(channel_config._get_channel())
        channel_copy = run_coroutine_to_completion(channel_config._get_channel())

        # Assert
        assert channel == channel_copy
        assert not channel.is_closed

        assert isinstance(channel_config._exchange, RobustExchange)
        assert channel_config._exchange.name == expected_exchange_name
        assert channel_config._exchange._Exchange__type == expected_exchange_type  # Private variable testing is weird
        assert channel_config._exchange.durable == expected_durable
        assert channel_config._exchange.auto_delete == expected_auto_delete

        assert isinstance(channel_config._queue, RobustQueue)
        assert channel_config._queue.durable == expected_durable
        assert channel_config._queue.auto_delete == expected_auto_delete
        assert list(channel_config._queue._bindings) == [(channel_config._exchange, channel_config._routing_key)]

        # Teardown
        channel_configuration_teardown(channel_config)

    def test_channel_configuration_close_reopen(self, message: Message, publish_channel: ChannelConfiguration):
        # Act
        run_coroutine_to_completion(publish_channel._get_channel())
        run_coroutine_to_completion(publish_channel._channel.close())

        # Assert
        assert publish_channel._channel.is_closed

        # Act
        run_coroutine_to_completion(publish_channel._get_channel())

        # Assert
        assert not publish_channel._channel.is_closed

    def test_channel_configuration_publish(self, message: Message, publish_channel: ChannelConfiguration) -> None:
        # Act
        run_coroutine_to_completion(publish_channel.start_channel())
        message_count_pre = publish_channel._queue.declaration_result.message_count
        run_coroutine_to_completion(publish_channel.publish(message=message))
        run_coroutine_to_completion(publish_channel._queue.declare())
        message_count_post = publish_channel._queue.declaration_result.message_count

        # Assert
        assert message_count_post == message_count_pre + 1

    def test_channel_configuration_consume_publish(self,
                                                   loop: asyncio.AbstractEventLoop,
                                                   message: Message,
                                                   publish_channel: ChannelConfiguration,
                                                   receive_channel: ChannelConfiguration):
        # Act
        message_body = run_coroutine_to_completion(self.check_consume_publish(loop,
                                                                              message,
                                                                              publish_channel,
                                                                              receive_channel))
        # Assert
        assert receive_channel.started
        assert message_body == message.body

    def test_channel_configuration_consume_publish_close_channel(self,
                                                                 loop: asyncio.AbstractEventLoop,
                                                                 message: Message,
                                                                 publish_channel: ChannelConfiguration,
                                                                 receive_channel: ChannelConfiguration):
        # Act
        message_body = run_coroutine_to_completion(self.check_consume_publish_close_channel(loop,
                                                                                            message,
                                                                                            publish_channel,
                                                                                            receive_channel))
        # Assert
        assert receive_channel.started
        assert message_body == message.body

    async def check_consume_publish(self,
                                    loop: asyncio.AbstractEventLoop,
                                    message: Message,
                                    publish_channel: ChannelConfiguration,
                                    receive_channel: ChannelConfiguration) -> Union[bytes, None]:
        self._test_future = loop.create_future()
        await receive_channel.consume(self.message_handler)
        await publish_channel.publish(message=message)
        return await self.collect_future()

    async def check_consume_publish_close_channel(self,
                                                  loop: asyncio.AbstractEventLoop,
                                                  message: Message,
                                                  publish_channel: ChannelConfiguration,
                                                  receive_channel: ChannelConfiguration) -> Union[bytes, None]:
        self._test_future = loop.create_future()
        await receive_channel.consume(self.message_handler)
        await receive_channel._channel.close()
        await publish_channel.publish(message=message)
        return await self.collect_future()

    async def message_handler(self, message: IncomingMessage) -> None:
        self._test_future.set_result(message.body)

    async def collect_future(self) -> Union[bytes, None]:
        try:
            await asyncio.wait_for(self._test_future, timeout=10)
            return self._test_future.result()
        except asyncio.exceptions.TimeoutError:
            return
