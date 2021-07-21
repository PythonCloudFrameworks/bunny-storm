import asyncio
import functools
from logging import Logger
from typing import Union

from aio_pika import RobustChannel, RobustExchange, RobustQueue, Message

from . import AsyncConnection


class ChannelConfiguration:
    _logger: Logger

    _connection: AsyncConnection
    _loop: asyncio.AbstractEventLoop

    _exchange_name: str
    _exchange_type: str
    _queue_name: str
    _routing_key: str
    _durable: bool
    _auto_delete: bool
    _prefetch_count: int
    _should_consume: bool
    _consume_params: list

    _channel_lock: asyncio.Lock
    _exchange: RobustExchange
    _queue: RobustQueue
    _channel: RobustChannel
    _started: bool

    def __init__(self, connection: AsyncConnection, logger: Logger, loop: asyncio.AbstractEventLoop = None,
                 exchange_name: str = None, exchange_type: str = "topic", queue_name: str = "", routing_key: str = None,
                 durable: bool = False, auto_delete: bool = False, prefetch_count: int = 1):
        self._logger = logger

        self._connection = connection
        self._loop = loop or asyncio.get_event_loop()

        self._exchange_name = exchange_name
        self._exchange_type = exchange_type
        self._queue_name = queue_name
        self._routing_key = routing_key
        self._durable = durable
        self._auto_delete = auto_delete
        self._prefetch_count = prefetch_count

        self._should_consume = False
        self._consume_params = list()

        self._channel_lock = asyncio.Lock(loop=self._loop)
        self._exchange = None
        self._queue = None
        self._channel = None
        self._started = False

    @property
    def started(self) -> bool:
        return self._started

    async def consume(self, on_message_callback, handler=None, no_ack=False):
        self._logger.info(f"[start consuming] routing key: {self._routing_key}; queue name: {self._queue_name}")
        await self._get_channel()

        self._should_consume = True
        self._consume_params = [on_message_callback, handler, no_ack]
        if handler is not None:
            await self._queue.consume(
                callback=functools.partial(on_message_callback, handler=handler),
                no_ack=no_ack,
            )
        else:
            await self._queue.consume(callback=on_message_callback, no_ack=no_ack)

    async def publish(self, message: Message, mandatory: bool = True, immediate: bool = False,
                      timeout: Union[int, float, None] = None) -> None:
        await self._get_channel()
        exchange = await self._declare_exchange()
        routing_key = self._routing_key

        self._logger.info(f"Publishing message. exchange: {exchange}; routing_key: {routing_key}; message: {message}")
        await exchange.publish(
            message=message,
            routing_key=routing_key,
            mandatory=mandatory,
            immediate=immediate,
            timeout=timeout
        )

    async def _get_channel(self) -> RobustChannel:
        await self._channel_lock.acquire()
        if not self._started:
            await self.start_channel()

        if self._channel.is_closed:
            await self._channel.reopen()
        self._channel_lock.release()

        return self._channel

    def on_channel_close(self, sender, exc):
        self._logger.warning("Channel closed. Exception info: ")
        self._logger.error(exc, exc_info=True)
        self._started = False
        self._loop.create_task(self.start_channel())

    def on_channel_return(self, sender, message):
        raise Exception(f"Channel returned. Message: {message}")

    async def start_channel(self) -> RobustChannel:
        self._logger.info("Creating channel")
        if self._started:
            return self._channel

        connection = await self._connection.get_connection()
        self._channel = await connection.channel()
        self._channel.add_close_callback(self.on_channel_close)
        self._channel.add_on_return_callback(self.on_channel_return)
        await self._channel.set_qos(prefetch_count=self._prefetch_count)

        self._queue = await self._declare_queue()
        self._exchange = await self._ensure_exchange()
        self._started = True
        return self._channel

    async def _ensure_exchange(self) -> Union[RobustExchange, None]:
        exchange = None
        if self._exchange_name:
            exchange = await self._declare_exchange()
            self._logger.info(f"Binding queue: {self._queue_name} to exchange: {self._exchange_name}")
            await self._queue.bind(exchange=exchange, routing_key=self._routing_key)
        return exchange

    async def _declare_exchange(self) -> RobustExchange:
        self._logger.info(f"Declaring exchange: {self._exchange_name}")
        if self._exchange_name not in self._channel._exchanges:
            exchange = await self._channel.declare_exchange(
                name=self._exchange_name,
                type=self._exchange_type,
                durable=self._durable,
                auto_delete=self._auto_delete,
            )
            self._logger.info(f"Declared exchange: {self._exchange_name}")
        else:
            exchange = list(self._channel._exchanges[self._exchange_name])[0]
        return exchange

    async def _declare_queue(self) -> RobustQueue:
        self._logger.info(f"Declaring queue: {self._exchange_name}")
        if self._queue_name not in self._channel._queues:
            queue = await self._channel.declare_queue(
                name=self._queue_name,
                durable=self._durable,
                auto_delete=self._auto_delete,
            )
            self._logger.info(f"Declared queue: {self._exchange_name}")
        else:
            queue = list(self._channel._queues[self._queue_name])[0]
        return queue
