import asyncio
import functools
from logging import Logger
from types import FunctionType
from typing import Union, Tuple

import aiormq
from aio_pika.types import Sender

from . import ChannelConfiguration, IntentionalCloseChannelError, AsyncConnection


class Consumer:
    """
    Responsible for consuming messages from a given queue (or queue and exchange) and running the given message handler
    """
    _channel_config: ChannelConfiguration

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
        """
        :param connection: AsyncConnection to pass to ChannelConfiguration
        :param logger: Logger
        :param loop: Loop
        :param exchange_name: Exchange name to bind queue to
        :param exchange_type: Exchange type
        :param queue_name: Queue name
        :param routing_key: Routing key for queue in exchange
        :param durable: Queue/exchange durability
        :param auto_delete: Whether or not queue/exchange auto delete
        :param prefetch_count: Prefetch count for ChannelConfiguration
        :param channel_number: Channel number for ChannelConfiguration
        :param publisher_confirms: Publisher confirms for ChannelConfiguration
        :param on_return_raises: On return raises for ChannelConfiguration
        """
        self._channel_config = ChannelConfiguration(
            connection,
            logger,
            loop,
            prefetch_count,
            channel_number,
            publisher_confirms,
            on_return_raises
        )
        self._channel_config.add_close_callback(self._on_channel_close)

        self._loop = self._channel_config.loop
        self._logger = logger
        self._exchange_name = exchange_name
        self._exchange_type = exchange_type
        self._queue_name = queue_name
        self._routing_key = routing_key
        self._durable = durable
        self._auto_delete = auto_delete

        self._exchange = None
        self._queue = None
        self._should_consume = False
        self._consume_params = None

    @property
    def logger(self) -> Logger:
        """
        :return: self._logger
        """
        return self._logger

    @property
    def channel_config(self) -> ChannelConfiguration:
        """
        :return: self._channel_config
        """
        return self._channel_config

    def _on_channel_close(self, sender: Sender, exc: BaseException) -> None:
        """
        Channel close callback which checks if we want to resume message consumption after the channel has been closed.
        :param sender: Closer
        :param exc: Close exception
        """
        if self._should_consume and not isinstance(exc, IntentionalCloseChannelError):
            self._loop.create_task(self.consume(*self._consume_params))

    async def _prepare_consume(self) -> None:
        """
        Prepares channel, queue, and exchange (if necessary) so the instance can begin consuming messages.
        """
        await self.channel_config.ensure_channel()
        if self._exchange_name:
            self._exchange = await self.channel_config.declare_exchange(
                self._exchange_name,
                exchange_type=self._exchange_type,
                durable=self._durable,
                auto_delete=self._auto_delete
            )
        else:
            self._exchange = None
        self._queue = await self.channel_config.declare_queue(
            queue_name=self._queue_name,
            exchange=self._exchange,
            routing_key=self._routing_key,
            durable=self._durable,
            auto_delete=self._auto_delete
        )

    async def consume(self, on_message_callback, handler=None, no_ack: bool = False) -> None:
        """
        Begins consuming messages and triggering the given callback for each message consumed.
        :param on_message_callback: Callback to consume with
        :param handler: Handler to pass on_message_callback
        :param no_ack: Whether or not we want to skip ACKing the messages
        """
        self.logger.info(f"[start consuming] routing key: {self._routing_key}; queue name: {self._queue_name}")
        await self._prepare_consume()
        self._should_consume = True
        self._consume_params = (on_message_callback, handler, no_ack)
        callback = on_message_callback if handler is None else functools.partial(on_message_callback, handler=handler)

        try:
            await self._queue.consume(callback=callback, no_ack=no_ack)
        except aiormq.exceptions.ChannelNotFoundEntity as exc:
            self.logger.error(f"Queue {self._queue} was not found, resetting channel")
            self._on_channel_close(None, exc)
