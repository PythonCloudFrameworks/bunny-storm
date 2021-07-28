#!/usr/bin/env python

"""Tests for `tornado_bunny` package."""

import asyncio

import pytest

from tornado_bunny import AsyncAdapter, RabbitMQConnectionData


@pytest.fixture(scope="function")
def rpc_executor_config() -> dict:
    return dict(
        receive=dict(
            incoming_1=dict(
                exchange_name="executor_ex",
                exchange_type="direct",
                routing_key="fib_calc",
                queue_name="executor_q",
                durable=True,
                auto_delete=False,
                prefetch_count=1
            ),
            incoming_2=dict(
                exchange_name="executor_ex_2",
                exchange_type="topic",
                routing_key="#",
                queue_name="executor_q_2",
                durable=True,
                auto_delete=False,
                prefetch_count=10
            )
        ),
        publish=dict(
            outgoing=dict(
                exchange_name="test_server",
                exchange_type="direct",
                routing_key="fib_server",
                queue_name="fib_server_q",
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
                exchange_name="executor_ex",
                exchange_type="direct",
                routing_key="fib_calc",
                queue_name="executor_q",
                durable=True,
                auto_delete=False,
                prefetch_count=1
            )
        ),
        publish=dict(
            outgoing=dict(
                exchange_name="test_server",
                exchange_type="direct",
                routing_key="fib_server",
                queue_name="fib_server_q",
                durable=True,
                auto_delete=False,
                prefetch_count=1
            )
        )
    )


def test_async_adapter_creation(rabbitmq_connection_data: RabbitMQConnectionData, configuration: dict):
    # Arrange
    test_configuration = None
    rabbit_adapter = AsyncAdapter(rabbitmq_connection_data=rabbitmq_connection_data, configuration=configuration)
