import asyncio
from logging import Logger
from typing import Union, List

from aio_pika import RobustChannel, RobustExchange, RobustQueue
from aio_pika.types import CloseCallbackType, Sender

from . import AsyncConnection


class ChannelConfiguration:
    """
    Responsible for the management of a single channel in a given connection.
    Exposes APIs for the declaration of exchanges and queues.
    Automatically recovers from being closed.
    """
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
        """
        :param connection: AsyncConnection instance to create a channel with
        :param logger: Logger
        :param loop: Event loop
        :param prefetch_count: Prefetch count of channel
        :param channel_number: Index of channel (set automatically if None)
        :param publisher_confirms: Whether or not to use publisher confirms
        :param on_return_raises: Whether or not to raise an error in case publishing a message causes a return
        """
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
        """
        Whether or not the channel has been started
        :return: self._started
        """
        return self._started

    @property
    def publisher_confirms(self) -> bool:
        """
        :return: self._publisher_confirms
        """
        return self._publisher_confirms

    @property
    def logger(self) -> Logger:
        """
        :return: self._logger
        """
        return self._logger

    @property
    def loop(self) -> asyncio.AbstractEventLoop:
        """
        :return: self._loop
        """
        return self._loop

    async def get_default_exchange(self) -> RobustExchange:
        await self.ensure_channel()
        return self._channel.default_exchange

    def add_close_callback(self, callback: CloseCallbackType) -> None:
        """
        Adds a callback which will be called in case the channel is closed. Callbacks are called sequentially.
        :param callback: Callback to add
        """
        self._channel_close_callbacks.append(callback)

    async def ensure_channel(self) -> RobustChannel:
        """
        Ensures that the channel has been started and is open, then returns it.
        If the channel has not been started, starts the channel.
        If the channel is closed, reopens it.
        Uses a Lock to ensure that no race conditions can occur.
        :return: Robust channel held by this instance
        """
        async with self._channel_lock:
            if not self._started:
                await self._start_channel()

            if self._channel.is_closed:
                await self._channel.reopen()

        return self._channel

    async def close_channel(self, exc: BaseException) -> None:
        """
        Resets the channel.
        If the channel is already closed, calls the callback for the channel being closed.
        If the channel is open, closes the channel.
        :param exc: Exception to pass for the channel's closing
        """
        if self._channel.is_closed:
            self._on_channel_close(None, exc)
        else:
            await self._channel.close(exc)

    def _on_channel_close(self, sender: Sender, exc: BaseException) -> None:
        """
        Handles the channel getting closed.
        Prints indicative logs, sets self._started to False, and calls all channel close callbacks.
        :param sender: Closer
        :param exc: Exception which caused the closing
        """
        self.logger.error("Channel closed. Exception info: ")
        self.logger.error(exc, exc_info=True)
        self._started = False
        for callback in self._channel_close_callbacks:
            callback(sender, exc)

    async def _start_channel(self) -> RobustChannel:
        """
        Creates a new channel using the AsyncConnection given to this instance, and sets the relevant parameters.
        :return: Channel created
        """
        self.logger.info("Creating channel")
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
        """
        Declares an exchange with the given parameters.
        If an exchange with the given name already exists in the channel, returns it instead of creating a new one.
        :param exchange_name: Exchange name
        :param exchange_type: Exchange type
        :param durable: Exchange durability
        :param auto_delete: Whether or not the exchange is auto deleted
        :return: Exchange which was declared or gotten
        """
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
        """
        Declares a queue with the given parameters.
        If a queue with the given name already exists in the channel, gets it instead of creating a new one.
        If an exchange and valid routing key are passed, the queue is bound to the exchange with the routing key.
        :param queue_name: Queue name
        :param exchange: Exchange to bind queue to
        :param routing_key: Routing key to bind queue with
        :param durable: Queue durability
        :param auto_delete: Whether or not the queue auto deletes
        :return: Declared or gotten queue
        """
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
