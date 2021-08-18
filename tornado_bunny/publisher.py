import asyncio
from logging import Logger
from typing import Union

import aio_pika
import aiormq
from aio_pika import Message, RobustExchange

from . import ChannelConfiguration, AsyncConnection


class Publisher:
    """
    Responsible for publishing messages to a given exchange with a given routing key
    """
    channel_config: ChannelConfiguration

    _exchange_name: str
    _exchange_type: str
    _routing_key: str
    _durable: bool
    _auto_delete: bool

    def __init__(self, connection: AsyncConnection, logger: Logger, exchange_name: str,
                 loop: asyncio.AbstractEventLoop = None, exchange_type: str = "topic", routing_key: str = None,
                 durable: bool = False, auto_delete: bool = False, prefetch_count: int = 1,
                 channel_number: int = None, publisher_confirms: bool = True, on_return_raises: bool = True):
        """
        :param connection: AsyncConnection to pass to ChannelConfiguration
        :param logger: Logger
        :param loop: Loop
        :param exchange_name: Exchange name to send messages to
        :param exchange_type: Exchange type
        :param routing_key: Routing key to send to
        :param durable: Exchange durability
        :param auto_delete: Whether or not exchange auto deletes
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

        self._logger = logger
        self._exchange_name = exchange_name
        self._exchange_type = exchange_type
        self._routing_key = routing_key
        self._durable = durable
        self._auto_delete = auto_delete
        self._exchange = None

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

    async def _prepare_publish(self) -> None:
        """
        Ensures the channel is started and declares the desired exchange
        """
        await self.channel_config.ensure_channel()
        self._exchange = await self.channel_config.declare_exchange(
            self._exchange_name,
            exchange_type=self._exchange_type,
            durable=self._durable,
            auto_delete=self._auto_delete
        )

    async def _publish_message(self,
                               exchange: RobustExchange,
                               message: Message,
                               routing_key: str,
                               mandatory: bool = True,
                               immediate: bool = False,
                               timeout: Union[int, float, None] = None) -> Union[Exception, None]:
        self.logger.info(f"Publishing message. exchange: {exchange}; routing_key: {routing_key}; message: {message}")
        try:
            result = await exchange.publish(
                message=message,
                routing_key=routing_key,
                mandatory=mandatory,
                immediate=immediate,
                timeout=timeout
            )
            if self.channel_config.publisher_confirms and not result:
                raise ValueError("Publisher confirm failed")
        except aiormq.exceptions.ChannelNotFoundEntity as exc:
            self.logger.error(f"Exchange {self._exchange} was not found, resetting channel")
            return exc
        except aio_pika.exceptions.DeliveryError as exc:
            self.logger.error(f"Message {message} was returned, resetting channel")
            return exc
        except ValueError as exc:
            self.logger.error("Publisher confirm failed")
            return exc

    async def publish(self,
                      message: Message,
                      mandatory: bool = True,
                      immediate: bool = False,
                      timeout: Union[int, float, None] = None) -> None:
        """
        Publishes the given message to the desired exchange with the desired routing key
        :param message: Message to publish
        :param mandatory: Whether or not the message is mandatory
        :param immediate: Whether or not the message should be immediate
        :param timeout: Publish timeout
        """
        await self._prepare_publish()
        publish_exception = await self._publish_message(
            exchange=self._exchange,
            message=message,
            routing_key=self._routing_key,
            mandatory=mandatory,
            immediate=immediate,
            timeout=timeout
        )
        if publish_exception:
            await self.channel_config.close_channel(publish_exception)
            await self.publish(message, mandatory, immediate, timeout)

    async def default_exchange_publish(self,
                                       message: Message,
                                       routing_key: str,
                                       mandatory: bool = True,
                                       immediate: bool = False,
                                       timeout: Union[int, float, None] = None) -> None:
        exchange = await self.channel_config.get_default_exchange()
        publish_exception = await self._publish_message(
            exchange=exchange,
            message=message,
            routing_key=routing_key,
            mandatory=mandatory,
            immediate=immediate,
            timeout=timeout
        )
        if publish_exception:
            await self.channel_config.close_channel(publish_exception)
            await self.default_exchange_publish(message, routing_key, mandatory, immediate, timeout)
