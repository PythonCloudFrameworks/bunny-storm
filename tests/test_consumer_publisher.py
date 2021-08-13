import asyncio

import pytest
from aio_pika import Message, IncomingMessage

from tests.conftest import collect_future
from tornado_bunny import Consumer, Publisher


class TestConsumerPublisher:
    _test_future: asyncio.Future = None

    @pytest.fixture(scope="function")
    def message(self) -> Message:
        return Message(b"test message")

    @pytest.mark.asyncio
    async def test_publish(self, message: Message, publisher: Publisher, consumer: Consumer) -> None:
        # Act
        await publisher._prepare_publish()
        queue = await publisher.channel_config.declare_queue("publish_test",
                                                             publisher._exchange,
                                                             publisher._routing_key,
                                                             publisher._durable,
                                                             publisher._auto_delete)
        message_count_pre = queue.declaration_result.message_count
        await publisher.publish(message=message)
        await queue.declare()
        message_count_post = queue.declaration_result.message_count

        # Assert
        assert message_count_post == message_count_pre + 1

        # Teardown
        await queue.delete(if_unused=False, if_empty=False)

    @pytest.mark.asyncio
    async def test_consume_publish(self,
                                   event_loop: asyncio.AbstractEventLoop,
                                   message: Message,
                                   publisher: Publisher,
                                   consumer: Consumer):
        # Arrange
        self._test_future = event_loop.create_future()

        # Act
        await consumer.consume(self.message_handler)
        await publisher.publish(message=message)
        message_body = await collect_future(self._test_future, timeout=10)

        # Assert
        assert consumer.channel_config.started
        assert message_body == message.body

    @pytest.mark.asyncio
    async def test_consume_publish_close_channel(self,
                                                 event_loop: asyncio.AbstractEventLoop,
                                                 message: Message,
                                                 publisher: Publisher,
                                                 consumer: Consumer):
        # Arrange
        self._test_future = event_loop.create_future()

        # Act
        await consumer.consume(self.message_handler)
        await consumer.channel_config.close_channel(None)
        await asyncio.sleep(0.1)  # Allow consumer to recover
        await publisher.publish(message=message)
        message_body = await collect_future(self._test_future, timeout=10)

        # Assert
        assert consumer.channel_config.started
        assert message_body == message.body

    async def message_handler(self, message: IncomingMessage) -> None:
        self._test_future.set_result(message.body)
