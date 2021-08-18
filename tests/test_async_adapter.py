#!/usr/bin/env python

"""Tests for `tornado_bunny` package."""

import asyncio
from logging import Logger

import pytest
from aio_pika import IncomingMessage

from tests.conftest import collect_future, consumer_teardown, publisher_teardown
from tornado_bunny import AsyncAdapter, RabbitMQConnectionData, Publisher, Consumer


@pytest.fixture(scope="function")
def rpc_executor_config() -> dict:
    return dict(
        receive=dict(
            incoming=dict(
                exchange_name="echo_requests",
                exchange_type="topic",
                routing_key="#",
                queue_name="echo_request_queue",
                durable=True,
                auto_delete=False,
                prefetch_count=10
            )
        )
    )


@pytest.fixture(scope="function")
def rpc_server_config() -> dict:
    return dict(
        receive=dict(
            incoming=dict(
                exchange_name="echo_responses",
                exchange_type="direct",
                routing_key="echo_response",
                queue_name="echo_response_queue",
                durable=True,
                auto_delete=False,
                prefetch_count=1
            )
        ),
        publish=dict(
            outgoing=dict(
                exchange_name="echo_requests",
                exchange_type="topic",
                routing_key="echo_request",
                durable=True,
                auto_delete=False,
                prefetch_count=1
            )
        )
    )


async def async_adapter_teardown(adapter: AsyncAdapter) -> None:
    for consumer in adapter._consumers.values():
        await consumer_teardown(consumer)
    for publisher in adapter._publishers.values():
        await publisher_teardown(publisher)


@pytest.fixture(scope="function")
async def executor_adapter(rabbitmq_connection_data: RabbitMQConnectionData, rpc_executor_config: dict) -> AsyncAdapter:
    adapter = AsyncAdapter(rabbitmq_connection_data, rpc_executor_config)
    yield adapter
    # Teardown
    await async_adapter_teardown(adapter)


@pytest.fixture(scope="function")
async def server_adapter(rabbitmq_connection_data: RabbitMQConnectionData, rpc_server_config: dict) -> AsyncAdapter:
    adapter = AsyncAdapter(rabbitmq_connection_data, rpc_server_config)
    yield adapter
    # Teardown
    await async_adapter_teardown(adapter)


class TestAsyncAdapter:
    _test_future: asyncio.Future = None
    _received_rpc_body: bytes = None

    def test_async_adapter_creation(self, rabbitmq_connection_data: RabbitMQConnectionData) -> None:
        # Arrange
        test_config = dict(
            receive=dict(
                incoming=dict(
                    exchange_name="echo_responses",
                    exchange_type="direct",
                    routing_key="echo_response",
                    queue_name="echo_response_queue",
                    durable=True,
                    auto_delete=False,
                    prefetch_count=1
                ),
                incoming2=dict(
                    exchange_name="echo_responses_2",
                    exchange_type="topic",
                    routing_key="echo_response_2",
                    queue_name="echo_response_queue_2",
                    durable=True,
                    auto_delete=False,
                    prefetch_count=10,
                )
            ),
            publish=dict(
                outgoing=dict(
                    exchange_name="echo_requests",
                    exchange_type="topic",
                    routing_key="echo_request",
                    durable=True,
                    auto_delete=False,
                    prefetch_count=1
                )
            )
        )

        rabbit_adapter = AsyncAdapter(rabbitmq_connection_data=rabbitmq_connection_data, configuration=test_config)

        # Assert
        for publish_config in test_config["publish"].values():
            publisher = rabbit_adapter._publishers[publish_config["exchange_name"]]
            assert isinstance(publisher, Publisher)
            assert publisher._exchange_name == publish_config["exchange_name"]
            assert publisher._exchange_type == publish_config["exchange_type"]
            assert publisher._routing_key == publish_config["routing_key"]
            assert publisher._durable == publish_config["durable"]
            assert publisher._auto_delete == publish_config["auto_delete"]
            assert publisher.channel_config._prefetch_count == publish_config["prefetch_count"]

        for consume_config in test_config["receive"].values():
            consumer = rabbit_adapter._consumers[consume_config["queue_name"]]
            assert isinstance(consumer, Consumer)
            assert consumer._exchange_name == consume_config["exchange_name"]
            assert consumer._exchange_type == consume_config["exchange_type"]
            assert consumer._queue_name == consume_config["queue_name"]
            assert consumer._routing_key == consume_config["routing_key"]
            assert consumer._durable == consume_config["durable"]
            assert consumer._auto_delete == consume_config["auto_delete"]
            assert consumer.channel_config._prefetch_count == consume_config["prefetch_count"]

    @pytest.mark.asyncio
    async def test_async_adapter_consume_publish(self,
                                                 event_loop: asyncio.AbstractEventLoop,
                                                 executor_adapter: AsyncAdapter,
                                                 server_adapter: AsyncAdapter):
        # Arrange
        exchange_name = "echo_requests"
        queue_name = "echo_request_queue"
        expected_message_body = b"test message"

        # Act
        self._test_future = event_loop.create_future()
        await executor_adapter.receive(self.base_message_handler, queue_name)
        await server_adapter.publish(body=expected_message_body, exchange=exchange_name)
        message_body = await collect_future(self._test_future, timeout=10)

        # Assert
        assert executor_adapter.status_check()
        assert server_adapter.status_check()

        assert message_body == expected_message_body

    @pytest.mark.asyncio
    async def test_async_adapter_rpc(self, event_loop, executor_adapter: AsyncAdapter, server_adapter: AsyncAdapter):
        # Arrange
        message_body = b"Double this!"
        expected_result = b"Double this! Double this!"

        # Act
        await executor_adapter.receive(handler=self.rpc_executor_message_handler, queue="echo_request_queue")
        result = await server_adapter.rpc(body=message_body,
                                          receive_queue="echo_response_queue",
                                          publish_exchange="echo_requests",
                                          timeout=10,
                                          ttl=10)

        # Assert
        assert executor_adapter.status_check()
        assert server_adapter.status_check()

        assert self._received_rpc_body == message_body
        assert result == expected_result

    async def base_message_handler(self, logger: Logger, message: IncomingMessage) -> None:
        self._test_future.set_result(message.body)

    async def rpc_executor_message_handler(self, logger: Logger, message: IncomingMessage) -> bytes:
        self._received_rpc_body = message.body
        return message.body + b" " + message.body

    # Error handling tests
    @pytest.mark.asyncio
    async def test_async_adapter_publish_error_handling(self, executor_adapter: AsyncAdapter):
        # Arrange
        bad_message_content = "This should be a bytes object"
        bad_exchange_name = "This exchange doesn't exist"

        # Act/Assert
        with pytest.raises(KeyError):
            await executor_adapter.publish(b"", bad_exchange_name)

        with pytest.raises(Exception):
            await executor_adapter.publish(bad_message_content, "echo_responses")

    @pytest.mark.asyncio
    async def test_async_adapter_receive_error_handling(self, executor_adapter: AsyncAdapter):
        # Arrange
        bad_queue_name = "This queue doesn't exist"

        # Act/Assert
        with pytest.raises(KeyError):
            await executor_adapter.receive(self.base_message_handler, bad_queue_name)

    @pytest.mark.asyncio
    async def test_async_adapter_rpc_bad_queue(self, event_loop, server_adapter: AsyncAdapter):
        # Arrange
        message_body = b"Handle this!"

        # Assert
        with pytest.raises(KeyError):
            await server_adapter.rpc(body=message_body,
                                     receive_queue="Nonexistent queue",
                                     publish_exchange="echo_requests",
                                     timeout=10,
                                     ttl=10)

    @pytest.mark.asyncio
    async def test_async_adapter_rpc_timeout(self,
                                             event_loop: asyncio.AbstractEventLoop,
                                             executor_adapter: AsyncAdapter,
                                             server_adapter: AsyncAdapter):
        # Arrange
        message_body = b"Handle this!"

        # Assert
        await executor_adapter.receive(handler=self.rpc_executor_message_handler, queue="echo_request_queue")
        with pytest.raises(TimeoutError):
            await server_adapter.rpc(body=message_body,
                                     receive_queue="echo_response_queue",
                                     publish_exchange="echo_requests",
                                     timeout=0,
                                     ttl=10)
