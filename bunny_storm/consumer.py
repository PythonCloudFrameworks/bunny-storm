import asyncio
import functools
from logging import Logger
from types import FunctionType
from typing import Tuple, Optional

import aiormq
from aio_pika.queue import ConsumerTag
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
    _exclusive_queue: bool
    _auto_delete: bool
    _exchange_kwargs: dict
    _queue_kwargs: dict

    _should_consume: bool
    _consume_params: Optional[Tuple[FunctionType, FunctionType, bool]]
    _consumer_tag: Optional[ConsumerTag]
    _consumer_lock: asyncio.Lock

    def __init__(self, connection: AsyncConnection, logger: Logger, loop: asyncio.AbstractEventLoop = None,
                 exchange_name: str = None, exchange_type: str = "topic", queue_name: str = "", routing_key: str = None,
                 durable: bool = False, exclusive_queue: bool = False, auto_delete: bool = False,
                 prefetch_count: int = 1, channel_number: int = None, publisher_confirms: bool = True,
                 on_return_raises: bool = False, exchange_kwargs: dict = None, queue_kwargs: dict = None, **kwargs):
        """
        :param connection: AsyncConnection to pass to ChannelConfiguration
        :param logger: Logger
        :param loop: Loop
        :param exchange_name: Exchange name to bind queue to
        :param exchange_type: Exchange type
        :param queue_name: Queue name
        :param routing_key: Routing key for queue in exchange
        :param durable: Queue/exchange durability
        :param exclusive_queue: Queue exclusivity
        :param auto_delete: Whether or not queue/exchange auto delete
        :param prefetch_count: Prefetch count for ChannelConfiguration
        :param channel_number: Channel number for ChannelConfiguration
        :param publisher_confirms: Publisher confirms for ChannelConfiguration
        :param on_return_raises: On return raises for ChannelConfiguration
        :param exchange_kwargs: Kwargs for exchange declaration
        :param queue_kwargs: Kwargs for queue declaration
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
        self._exclusive_queue = exclusive_queue
        self._auto_delete = auto_delete
        self._exchange_kwargs = exchange_kwargs or dict()
        self._queue_kwargs = queue_kwargs or dict()

        self._exchange = None
        self._queue = None
        self._should_consume = False
        self._consume_params = None
        self._consumer_tag = None
        self._consumer_lock = asyncio.Lock()

        for key, value in kwargs.items():
            self._logger.warning(f"Consumer received unexpected keyword argument. Key: {key} Value: {value}")

    @property
    def logger(self) -> Logger:
        return self._logger

    @property
    def channel_config(self) -> ChannelConfiguration:
        return self._channel_config

    @property
    def consumer_tag(self) -> ConsumerTag:
        return self._consumer_tag

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
                auto_delete=self._auto_delete,
                **self._exchange_kwargs
            )
        else:
            self._exchange = None
        self._queue = await self.channel_config.declare_queue(
            queue_name=self._queue_name,
            exchange=self._exchange,
            routing_key=self._routing_key,
            durable=self._durable,
            exclusive=self._exclusive_queue,
            auto_delete=self._auto_delete,
            **self._queue_kwargs
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
            async with self._consumer_lock:
                self._consumer_tag = await self._queue.consume(callback=callback, no_ack=no_ack,
                                                               consumer_tag=self._consumer_tag)
        except aiormq.exceptions.ChannelNotFoundEntity as exc:
            self.logger.error(f"Queue {self._queue} was not found, resetting channel")
            self._on_channel_close(None, exc)
        except aiormq.exceptions.DuplicateConsumerTag:
            # In case this consumer is already consuming the given queue, do not call consume again. Otherwise a
            # consumer leak will occur.
            self.logger.warning(f"Attempted to consume already consumed queue {self._queue} with tag "
                                f"`{self._consumer_tag}")

    async def close(self) -> None:
        """
        Close channel intentionally
        """
        await self.channel_config.close_channel(IntentionalCloseChannelError("Close channel"))
