import asyncio
from logging import Logger
from typing import Union, List

from aio_pika import RobustChannel, RobustExchange, RobustQueue
from aio_pika.types import CloseCallbackType, Sender

from . import AsyncConnection


class ChannelConfiguration:
    _logger: Logger

    _connection: AsyncConnection
    _loop: asyncio.AbstractEventLoop

    _prefetch_count: int
    _channel_number: int
    _publisher_confirms: bool
    _on_return_raises: bool

    _channel_lock: asyncio.Lock
    _channel: Union[RobustChannel, None]
    _started: bool

    _channel_close_callbacks: List[CloseCallbackType]

    def __init__(self, connection: AsyncConnection, logger: Logger, loop: asyncio.AbstractEventLoop = None,
                 prefetch_count: int = 1, channel_number: int = None, publisher_confirms: bool = True,
                 on_return_raises: bool = False):
        self._logger = logger

        self._connection = connection
        self._loop = loop or asyncio.get_event_loop()

        self._prefetch_count = prefetch_count
        self._channel_number = channel_number
        self._publisher_confirms = publisher_confirms
        self._on_return_raises = on_return_raises

        self._channel_lock = asyncio.Lock()
        self._channel = None
        self._started = False

        self._channel_close_callbacks = []

    @property
    def started(self) -> bool:
        return self._started

    @property
    def logger(self) -> Logger:
        return self._logger

    @property
    def loop(self) -> asyncio.AbstractEventLoop:
        return self._loop

    def add_close_callback(self, callback: CloseCallbackType):
        self._channel_close_callbacks.append(callback)

    async def ensure_channel(self) -> RobustChannel:
        await self._channel_lock.acquire()
        if not self._started:
            await self._start_channel()

        if self._channel.is_closed:
            await self._channel.reopen()
        self._channel_lock.release()

        return self._channel

    async def reset_channel(self, exc: BaseException) -> None:
        if self._channel.is_closed:
            self._on_channel_close(None, exc)
        else:
            await self._channel.close(exc)

    def _on_channel_close(self, sender: Sender, exc: BaseException) -> None:
        self.logger.error("Channel closed. Exception info: ")
        self.logger.error(exc, exc_info=True)
        self._started = False
        for callback in self._channel_close_callbacks:
            callback(sender, exc)

    async def _start_channel(self) -> RobustChannel:
        self.logger.info("Creating channel")
        if self._started:
            return self._channel

        connection = await self._connection.get_connection()
        self._channel = await connection.channel(channel_number=self._channel_number,
                                                 publisher_confirms=self._publisher_confirms,
                                                 on_return_raises=self._on_return_raises)
        self._channel.add_close_callback(self._on_channel_close)
        await self._channel.set_qos(prefetch_count=self._prefetch_count)

        self._started = True
        return self._channel

    async def declare_exchange(self,
                               exchange_name: str,
                               exchange_type: str = "direct",
                               durable: bool = None,
                               auto_delete: bool = False) -> RobustExchange:
        await self.ensure_channel()
        self.logger.info(f"Declaring exchange: {exchange_name}")
        if exchange_name not in self._channel._exchanges:
            exchange = await self._channel.declare_exchange(
                name=exchange_name,
                type=exchange_type,
                durable=durable,
                auto_delete=auto_delete,
            )
            self.logger.info(f"Declared exchange: {exchange_name}")
        else:
            exchange = list(self._channel._exchanges[exchange_name])[0]
        return exchange

    async def declare_queue(self,
                            queue_name: str,
                            exchange: RobustExchange = None,
                            routing_key: str = None,
                            durable: bool = None,
                            auto_delete: bool = False) -> RobustQueue:
        await self.ensure_channel()
        self.logger.info(f"Declaring queue: {queue_name}")
        if queue_name not in self._channel._queues:
            queue = await self._channel.declare_queue(
                name=queue_name,
                durable=durable,
                auto_delete=auto_delete,
            )
            self.logger.info(f"Declared queue: {queue_name}")
        else:
            queue = list(self._channel._queues[queue_name])[0]

        if exchange and routing_key:
            self.logger.info(f"Binding queue: {queue_name} to exchange: {exchange}, route: {routing_key}")
            await queue.bind(exchange=exchange, routing_key=routing_key)

        return queue
