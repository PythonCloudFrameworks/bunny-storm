import asyncio
from typing import Union

import pytest
from aio_pika import Message, IncomingMessage

from tests.conftest import run_coroutine_to_completion, collect_future
from tornado_bunny import Consumer, Publisher


class TestConsumerPublisher:
    _test_future: asyncio.Future = None

    @pytest.fixture(scope="function")
    def message(self) -> Message:
        return Message(b"test message")

    def test_publish(self, message: Message, publisher: Publisher, consumer: Consumer) -> None:
        # Act
        run_coroutine_to_completion(publisher._prepare_publish())
        queue = run_coroutine_to_completion(publisher.channel_config.declare_queue("publish_test",
                                                                                   publisher._exchange,
                                                                                   publisher._routing_key,
                                                                                   publisher._durable,
                                                                                   publisher._auto_delete))
        message_count_pre = queue.declaration_result.message_count
        run_coroutine_to_completion(publisher.publish(message=message))
        run_coroutine_to_completion(queue.declare())
        message_count_post = queue.declaration_result.message_count

        # Assert
        assert message_count_post == message_count_pre + 1

        # Teardown
        run_coroutine_to_completion(queue.delete(if_unused=False, if_empty=False))

    def test_consume_publish(self,
                             loop: asyncio.AbstractEventLoop,
                             message: Message,
                             publisher: Publisher,
                             consumer: Consumer):
        # Act
        message_body = run_coroutine_to_completion(self.check_consume_publish(loop, message, publisher, consumer))
        # Assert
        assert consumer.channel_config.started
        assert message_body == message.body

    def test_consume_publish_close_channel(self,
                                           loop: asyncio.AbstractEventLoop,
                                           message: Message,
                                           publisher: Publisher,
                                           consumer: Consumer):
        # Act
        message_body = run_coroutine_to_completion(self.check_consume_publish_close_channel(loop,
                                                                                            message,
                                                                                            publisher,
                                                                                            consumer))
        # Assert
        assert consumer.channel_config.started
        assert message_body == message.body

    async def check_consume_publish(self,
                                    loop: asyncio.AbstractEventLoop,
                                    message: Message,
                                    publisher: Publisher,
                                    consumer: Consumer) -> Union[bytes, None]:
        self._test_future = loop.create_future()
        await consumer.consume(self.message_handler)
        await publisher.publish(message=message)
        return await collect_future(self._test_future, timeout=10)

    async def check_consume_publish_close_channel(self,
                                                  loop: asyncio.AbstractEventLoop,
                                                  message: Message,
                                                  publisher: Publisher,
                                                  consumer: Consumer) -> Union[bytes, None]:
        self._test_future = loop.create_future()
        await consumer.consume(self.message_handler)
        await consumer.channel_config.close_channel(None)
        await asyncio.sleep(0.1)  # Allow consumer to recover
        await publisher.publish(message=message)
        return await collect_future(self._test_future, timeout=10)

    async def message_handler(self, message: IncomingMessage) -> None:
        self._test_future.set_result(message.body)
