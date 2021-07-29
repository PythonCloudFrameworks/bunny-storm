#!/usr/bin/env python

"""Tests for `tornado_bunny` package."""

import asyncio
from logging import Logger
from typing import Union

import pytest
from aio_pika import IncomingMessage

from tests.conftest import run_coroutine_to_completion, collect_future, consumer_teardown, publisher_teardown
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
        ),
        publish=dict(
            outgoing=dict(
                exchange_name="echo_responses",
                exchange_type="direct",
                routing_key="echo_response",
                durable=True,
                auto_delete=False,
                prefetch_count=1
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


def async_adapter_teardown(adapter: AsyncAdapter) -> None:
    for consumer in adapter._consumers.values():
        consumer_teardown(consumer)
    for publisher in adapter._publishers.values():
        publisher_teardown(publisher)


@pytest.fixture(scope="function")
def executor_adapter(rabbitmq_connection_data: RabbitMQConnectionData, rpc_executor_config: dict) -> AsyncAdapter:
    adapter = AsyncAdapter(rabbitmq_connection_data, rpc_executor_config)
    yield adapter
    # Teardown
    async_adapter_teardown(adapter)


@pytest.fixture(scope="function")
def server_adapter(rabbitmq_connection_data: RabbitMQConnectionData, rpc_server_config: dict) -> AsyncAdapter:
    adapter = AsyncAdapter(rabbitmq_connection_data, rpc_server_config)
    yield adapter
    # Teardown
    async_adapter_teardown(adapter)


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

    def test_async_adapter_consume_publish(self, loop, executor_adapter: AsyncAdapter, server_adapter: AsyncAdapter):
        # Arrange
        expected_message_body = b"test message"
        # Act
        message_body = run_coroutine_to_completion(self.check_async_adapter_consume_publish(loop,
                                                                                            expected_message_body,
                                                                                            "echo_requests",
                                                                                            "echo_request_queue",
                                                                                            server_adapter,
                                                                                            executor_adapter))
        # Assert
        assert executor_adapter.status_check()
        assert server_adapter.status_check()

        assert message_body == expected_message_body

    def test_async_adapter_rpc(self, loop, executor_adapter: AsyncAdapter, server_adapter: AsyncAdapter):
        # Arrange
        message_body = b"Double this!"
        expected_result = b"Double this! Double this!"

        # Act
        run_coroutine_to_completion(executor_adapter.receive(handler=self.rpc_executor_message_handler,
                                                             queue="echo_request_queue"))
        result = run_coroutine_to_completion(server_adapter.rpc(body=message_body,
                                                                receive_queue="echo_response_queue",
                                                                publish_exchange="echo_requests",
                                                                timeout=10,
                                                                ttl=10))

        # Assert
        assert executor_adapter.status_check()
        assert server_adapter.status_check()

        assert self._received_rpc_body == message_body
        assert result == expected_result

    async def check_async_adapter_consume_publish(self,
                                                  loop: asyncio.AbstractEventLoop,
                                                  body: bytes,
                                                  exchange_name: str,
                                                  queue_name: str,
                                                  publish_adapter: AsyncAdapter,
                                                  receive_adapter: AsyncAdapter) -> Union[bytes, None]:
        self._test_future = loop.create_future()
        await receive_adapter.receive(self.base_message_handler, queue_name)
        await publish_adapter.publish(body=body, exchange=exchange_name)
        return await collect_future(self._test_future, timeout=10)

    async def base_message_handler(self, logger: Logger, message: IncomingMessage) -> None:
        self._test_future.set_result(message.body)

    async def rpc_executor_message_handler(self, logger: Logger, message: IncomingMessage) -> bytes:
        self._received_rpc_body = message.body
        return message.body + b" " + message.body
