import asyncio
import functools
from logging import Logger
from types import FunctionType
from typing import Union, Tuple

import aiormq
from aio_pika.types import Sender

from . import ChannelConfiguration, IntentionalCloseChannelError, AsyncConnection


class Consumer:
    channel_config: ChannelConfiguration

    _exchange_name: str
    _exchange_type: str
    _queue_name: str
    _routing_key: str
    _durable: bool
    _auto_delete: bool

    _should_consume: bool
    _consume_params: Union[Tuple[FunctionType, FunctionType, bool], None]

    def __init__(self, connection: AsyncConnection, logger: Logger, loop: asyncio.AbstractEventLoop = None,
                 exchange_name: str = None, exchange_type: str = "topic", queue_name: str = "", routing_key: str = None,
                 durable: bool = False, auto_delete: bool = False, prefetch_count: int = 1, channel_number: int = None,
                 publisher_confirms: bool = True, on_return_raises: bool = False):
        self._loop = loop or asyncio.get_event_loop()
        self.channel_config = ChannelConfiguration(
            connection,
            logger,
            self._loop,
            prefetch_count,
            channel_number,
            publisher_confirms,
            on_return_raises
        )
        self.channel_config.add_close_callback(self._on_channel_close)

        self._logger = logger
        self._exchange_name = exchange_name
        self._exchange_type = exchange_type
        self._queue_name = queue_name
        self._routing_key = routing_key
        self._durable = durable
        self._auto_delete = auto_delete

        self._should_consume = False
        self._consume_params = None

    @property
    def logger(self) -> Logger:
        return self._logger

    def _on_channel_close(self, sender: Sender, exc: BaseException):
        if self._should_consume and not isinstance(exc, IntentionalCloseChannelError):
            self._loop.create_task(self.consume(*self._consume_params))

    async def consume(self, on_message_callback: FunctionType, handler: FunctionType = None, no_ack: bool = False):
        self.logger.info(f"[start consuming] routing key: {self._routing_key}; queue name: {self._queue_name}")
        await self.channel_config.ensure_channel()
        if self._exchange_name:
            exchange = await self.channel_config.declare_exchange(
                self._exchange_name,
                exchange_type=self._exchange_type,
                durable=self._durable,
                auto_delete=self._auto_delete
            )
        else:
            exchange = None
        queue = await self.channel_config.declare_queue(
            queue_name=self._queue_name,
            exchange=exchange,
            routing_key=self._routing_key,
            durable=self._durable,
            auto_delete=self._auto_delete
        )

        self._should_consume = True
        self._consume_params = (on_message_callback, handler, no_ack)
        callback = on_message_callback if handler is None else functools.partial(on_message_callback, handler=handler)

        try:
            await queue.consume(callback=callback, no_ack=no_ack)
        except aiormq.exceptions.ChannelNotFoundEntity as exc:
            self.logger.error(f"Queue {queue} was not found, resetting channel")
            self._on_channel_close(None, exc)
