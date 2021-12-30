from __future__ import annotations

import asyncio
import functools
from logging import Logger
from typing import Union, Optional

import aio_pika
import aiormq
from aio_pika import Message, RobustExchange

from . import ChannelConfiguration, AsyncConnection, IntentionalCloseChannelError


def _retry_publish_exception(coroutine_function):
    """
    A decorator that wraps a publish function in order to retry it in case of an exception, with dedicated logic
    """

    @functools.wraps(coroutine_function)
    async def publish(self: Publisher,
                      message: Message,
                      routing_key: str = None,
                      mandatory: bool = True,
                      immediate: bool = False,
                      timeout: Union[int, float, None] = None,
                      max_retry_count: int = 5,
                      retry_count: int = 0) -> None:
        publish_exception = None

        try:
            await coroutine_function(self, message, routing_key, mandatory, immediate, timeout)
        except aiormq.exceptions.ChannelNotFoundEntity as exc:
            self.logger.error(f"Could not send message, Exchange {self._exchange} was not found, resetting channel")
            publish_exception = exc
        except aio_pika.exceptions.DeliveryError as exc:
            self.logger.error(f"Could not send message, Message {message} was returned, resetting channel")
            publish_exception = exc
        except ValueError as exc:
            self.logger.error("Could not send message, Publisher confirm failed")
            publish_exception = exc
        except (asyncio.exceptions.CancelledError, ConnectionError):
            self.logger.error("Could not send message, Publisher was cancelled")
            publish_exception = self.CLOSE_CONNECTION
        except BaseException as exc:
            self.logger.exception(f"Could not send message, Unknown publisher error: {exc}")
            publish_exception = self.CLOSE_CONNECTION

        if publish_exception:
            if publish_exception == self.CLOSE_CONNECTION:
                # Let robust connection handle the reconnection and wait until it's done
                await self.channel_config.connection.wait_connected()
            else:
                await self.channel_config.close_channel(publish_exception)

            if retry_count < max_retry_count:
                retry_count = retry_count + 1
                await publish(self, message, routing_key, mandatory, immediate, timeout, max_retry_count, retry_count)

    return publish


class Publisher:
    """
    Responsible for publishing messages to a given exchange with a given routing key
    """
    CLOSE_CONNECTION = "Connection close is needed"

    channel_config: ChannelConfiguration

    _exchange_name: str
    _exchange_type: str
    _routing_key: str
    _durable: bool
    _auto_delete: bool
    _exchange_kwargs: dict
    _exchange: Optional[RobustExchange]

    def __init__(self, connection: AsyncConnection, logger: Logger, exchange_name: str,
                 loop: asyncio.AbstractEventLoop = None, exchange_type: str = "topic", routing_key: str = None,
                 durable: bool = False, auto_delete: bool = False, prefetch_count: int = 1,
                 channel_number: int = None, publisher_confirms: bool = True, on_return_raises: bool = True,
                 exchange_kwargs: dict = None, **kwargs):
        """
        :param connection: AsyncConnection to pass to ChannelConfiguration
        :param logger: Logger
        :param loop: Loop
        :param exchange_name: Exchange name to send messages to
        :param exchange_type: Exchange type
        :param routing_key: Default routing key to send the message with
        :param durable: Exchange durability
        :param auto_delete: Whether or not exchange auto deletes
        :param prefetch_count: Prefetch count for ChannelConfiguration
        :param channel_number: Channel number for ChannelConfiguration
        :param publisher_confirms: Publisher confirms for ChannelConfiguration
        :param on_return_raises: On return raises for ChannelConfiguration
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

        self._logger = logger
        self._exchange_name = exchange_name
        self._exchange_type = exchange_type
        self._routing_key = routing_key
        self._durable = durable
        self._auto_delete = auto_delete
        self._exchange_kwargs = exchange_kwargs or dict()
        self._exchange = None

        for key, value in kwargs.items():
            self._logger.warning(f"Publisher received unexpected keyword argument. Key: {key} Value: {value}")

    @property
    def logger(self) -> Logger:
        return self._logger

    @property
    def channel_config(self) -> ChannelConfiguration:
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
            auto_delete=self._auto_delete,
            **self._exchange_kwargs
        )

    async def _publish_message(self,
                               exchange: RobustExchange,
                               message: Message,
                               routing_key: str,
                               mandatory: bool = True,
                               immediate: bool = False,
                               timeout: Union[int, float, None] = None) -> None:
        self.logger.info(
            f"Publishing message. exchange: {exchange}; routing_key: {routing_key}; message: {message.body[:100]}")
        result = await exchange.publish(
            message=message,
            routing_key=routing_key,
            mandatory=mandatory,
            immediate=immediate,
            timeout=timeout
        )
        if self.channel_config.publisher_confirms and not result:
            raise ValueError("Publisher confirm failed")

    @_retry_publish_exception
    async def publish(self,
                      message: Message,
                      routing_key: str = None,
                      mandatory: bool = True,
                      immediate: bool = False,
                      timeout: Union[int, float, None] = None,
                      max_retry_count: int = 5) -> None:
        """
        Publishes the given message to the desired exchange with the desired routing key
        :param message: Message to publish
        :param routing_key: Routing key to send the message with, defaults to the publisher's
        :param mandatory: Whether or not the message is mandatory
        :param immediate: Whether or not the message should be immediate
        :param timeout: Publish timeout
        :param max_retry_count: Publish max retry count
        """
        await self._prepare_publish()
        await self._publish_message(
            exchange=self._exchange,
            message=message,
            routing_key=routing_key or self._routing_key,
            mandatory=mandatory,
            immediate=immediate,
            timeout=timeout
        )

    @_retry_publish_exception
    async def default_exchange_publish(self,
                                       message: Message,
                                       routing_key: str,
                                       mandatory: bool = True,
                                       immediate: bool = False,
                                       timeout: Union[int, float, None] = None,
                                       max_retry_count: int = 5
                                       ) -> None:
        exchange = await self.channel_config.get_default_exchange()
        await self._publish_message(
            exchange=exchange,
            message=message,
            routing_key=routing_key,
            mandatory=mandatory,
            immediate=immediate,
            timeout=timeout
        )

    async def close(self) -> None:
        """
        Close channel intentionally
        """
        await self.channel_config.close_channel(IntentionalCloseChannelError("Close channel"))
