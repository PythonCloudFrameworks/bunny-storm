import asyncio
from logging import Logger
from typing import Union

import aio_pika
import aiormq
from aio_pika import Message

from . import ChannelConfiguration, AsyncConnection


class Publisher:
    channel_config: ChannelConfiguration

    _exchange_name: str
    _exchange_type: str
    _routing_key: str
    _durable: bool
    _auto_delete: bool

    def __init__(self, connection: AsyncConnection, logger: Logger, exchange_name: str,
                 loop: asyncio.AbstractEventLoop = None, exchange_type: str = "topic", routing_key: str = None,
                 durable: bool = False, auto_delete: bool = False, prefetch_count: int = 1,
                 channel_number: int = None, publisher_confirms: bool = True, on_return_raises: bool = False):
        self.channel_config = ChannelConfiguration(
            connection,
            logger,
            loop,
            prefetch_count,
            channel_number,
            publisher_confirms,
            on_return_raises
        )

        self._logger = logger
        self._exchange_name = exchange_name
        self._exchange_type = exchange_type
        self._routing_key = routing_key
        self._durable = durable
        self._auto_delete = auto_delete

    @property
    def logger(self) -> Logger:
        return self._logger

    async def publish(self,
                      message: Message,
                      mandatory: bool = True,
                      immediate: bool = False,
                      timeout: Union[int, float, None] = None) -> None:
        await self.channel_config.ensure_channel()
        exchange = await self.channel_config.declare_exchange(
            self._exchange_name,
            exchange_type=self._exchange_type,
            durable=self._durable,
            auto_delete=self._auto_delete
        )
        self.logger.info(f"Publishing message. exchange: {exchange}; routing_key: {self._routing_key}; "
                          f"message: {message}")
        try:
            await exchange.publish(
                message=message,
                routing_key=self._routing_key,
                mandatory=mandatory,
                immediate=immediate,
                timeout=timeout
            )
        except aiormq.exceptions.ChannelNotFoundEntity as exc:
            self.logger.error(f"Exchange {exchange} was not found, resetting channel")
            await self.channel_config.reset_channel(exc)
        except aio_pika.exceptions.DeliveryError as exc:
            self.logger.error(f"Message {message} was returned, resetting channel")
            await self.channel_config.reset_channel(exc)
