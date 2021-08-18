import asyncio
import logging
from logging import Logger
import uuid
import sys
from types import FunctionType
from typing import Union

from aio_pika import Message, DeliveryMode, IncomingMessage

from . import RabbitMQConnectionData, AsyncConnection, Consumer, Publisher


class AsyncAdapter:
    """
    An asynchronous RabbitMQ client which is fully integrated with aio-pika.
    All-in-one adapter which provides the following interfaces:
    1. publish - Publish a message.
    2. receive - Consume messages from a queue. Can automatically reply to desired routes if the received message
                 contains a "reply_to" property.
    3. rpc - Publish a message with a reply_to property, wait for a reply message and return the reply's value.

    Architecture:
    This class uses a single connection, and creates a Consumer for each receive channel passed in the configurations,
    and a Publisher for each publish channel passed in the configuration.
    """

    _rabbitmq_connection_data: RabbitMQConnectionData
    _logger: Logger
    _loop: asyncio.AbstractEventLoop
    _configuration: dict

    def __init__(self, rabbitmq_connection_data: RabbitMQConnectionData, configuration: dict, logger: Logger = None,
                 loop: asyncio.AbstractEventLoop = None, connection_properties: dict = None,
                 connection_timeout: Union[int, float] = 10, connection_attempts: int = 5, attempt_backoff: int = 5):
        """
        :param rabbitmq_connection_data: Connection data for RabbitMQ server. Used to open AsyncConnection.
        :param configuration: RabbitMQ configuration for both consuming and publishing.
                              It is separated by the keys `receive` and `publish`
        :param logger: Logger to use. If None, creates a default Logger.
        :param loop: Async event loop. If it is None, defaults to asyncio.get_event_loop()
        :param connection_properties: Connection properties
        :param connection_timeout: Timeout of each connection attempt
        :param connection_attempts: Number of attempts for connection
        :param attempt_backoff: Seconds waited between connection attempts
        """
        self._rabbitmq_connection_data = rabbitmq_connection_data
        self._logger = logger or self._create_logger()
        self._loop = loop or asyncio.get_event_loop()
        self._configuration = configuration

        self._connection = AsyncConnection(
            rabbitmq_connection_data,
            self.logger,
            self._loop,
            connection_properties,
            connection_timeout,
            connection_attempts,
            attempt_backoff
        )

        self._rpc_corr_id_dict = dict()

        self._default_publisher = Publisher(connection=self._connection,
                                            logger=self.logger,
                                            exchange_name="",
                                            loop=self._loop)

        self._publishers = dict()
        for publish_configuration in self._configuration.get("publish", dict()).values():
            self.add_publisher(publish_configuration)

        self._consumers = dict()
        for receive_configuration in self._configuration.get("receive", dict()).values():
            self.add_consumer(receive_configuration)

    @staticmethod
    def _create_logger() -> Logger:
        """
        Creates logger with hardcoded handlers and formatters
        :return: Created logger
        """
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.DEBUG)
        sh = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter('[%(asctime)s] [%(filename)-25s] [%(levelname)-8s] %(message)s')
        sh.setFormatter(formatter)
        sh.setLevel(logging.DEBUG)
        logger.addHandler(sh)
        logger.propagate = False
        return logger

    @property
    def logger(self) -> Logger:
        """
        :return: self._logger
        """
        return self._logger

    def add_publisher(self, configuration: dict) -> Publisher:
        """
        Creates a Publisher with the given configurations and add to self._publishers
        :param configuration: Configuration to pass to publisher
        :return: Created publisher
        """
        publisher = Publisher(connection=self._connection, logger=self.logger, loop=self._loop, **configuration)
        self._publishers[configuration["exchange_name"]] = publisher
        return publisher

    def add_consumer(self, configuration: dict) -> Consumer:
        """
        Creates a Consumer with the given configurations and add to self._consumers
        :param configuration: Configurations to pass to Consumer
        :return: Created consumer
        """
        consumer = Consumer(connection=self._connection, logger=self.logger, loop=self._loop, **configuration)
        self._consumers[configuration["queue_name"]] = consumer
        return consumer

    async def publish(self, body: bytes, exchange: str, properties: dict = None, mandatory: bool = True,
                      immediate: bool = False, timeout: Union[int, float, None] = None):
        """
        Publish a message using the publisher relevant to the exchange given.
        :param body: message
        :param exchange: The exchange to publish to
        :param properties: RabbitMQ message properties
        :param mandatory: RabbitMQ publish mandatory param
        :param immediate: Whether or not message should be immediate
        :param timeout: Publish timeout
        """
        self.logger.info("Trying to publish message")
        if properties is None:
            properties = dict(delivery_mode=DeliveryMode.PERSISTENT)

        publisher = self._publishers.get(exchange)
        if publisher is None:
            raise KeyError(f"There is no publisher for the given exchange: {exchange}")

        try:
            message = Message(body, **properties)
            await publisher.publish(message, mandatory=mandatory, immediate=immediate, timeout=timeout)
        except Exception:
            self.logger.exception("Failed to publish message")
            raise

    async def receive(self, handler, queue: str, no_ack: bool = False) -> None:
        """
        Consume messages with the consumer relevant to the queue given.
        If the received message contains a "reply_to" parameter, reply to the route with the message handler's result.
        :param handler: Message handler
        :type handler: async def fn(logger, incoming_message)
        :param queue: The queue to consume from
        :param no_ack: Whether or not to ack incoming messages
        """
        consumer = self._consumers.get(queue)
        if consumer is None:
            raise KeyError(f"There is no consumer for the given queue: {queue}")

        try:
            await consumer.consume(self._on_message, handler=handler, no_ack=no_ack)
        except Exception:
            self.logger.exception("Failed to receive message.")
            raise

    async def _on_message(self, message: IncomingMessage, handler: FunctionType) -> None:
        """
        Base message handler. Runs the given message handler on the received message, and if we want to reply to a
        given route, sends the handler's result as a reply.
        :param message: Consumed message to handle
        :param handler: Message handler
        """
        self.logger.info("Received a new message")
        try:
            result = await handler(self.logger, message)
            self.logger.info("Message has been processed successfully")
            if message.reply_to is not None:
                self.logger.info(f"Sending result back to "
                                 f"queue: {message.reply_to}, correlation id: {message.correlation_id}")
                response_message = Message(body=result,
                                           correlation_id=message.correlation_id,
                                           reply_to=message.reply_to)
                await self._default_publisher.default_exchange_publish(message=response_message,
                                                                       routing_key=message.reply_to,
                                                                       mandatory=False)
                self.logger.info(f"Sent result back to caller. "
                                 f"Queue: {message.reply_to}, correlation id: {message.correlation_id}")
        except Exception:
            self.logger.exception("Failed to handle received message.")
            raise
        finally:
            message.ack()

    async def rpc(self, body: bytes, receive_queue: str, publish_exchange: str, timeout: Union[int, float],
                  ttl: int) -> bytes:
        """
        RPC call. Consumes from the given receive_queue to wait for the result of the RPC call.
        Then publishes a message to the given publish_exchange with correlation_id and reply_to properties.
        Then waits for the RPC result until it is received or a timeout occurs.
        If a result arrives, returns it.
        :param body: Message body
        :param receive_queue: The queue to consume from
        :param publish_exchange: The exchange to publish to
        :param timeout: RPC timeout (seconds)
        :param ttl: Message's time to live in the RabbitMQ queue (seconds)
        :return: RPC call result
        :raises: Exception contained in future if any
        """
        consumer = self._consumers.get(receive_queue)
        if consumer is None:
            raise KeyError(f"There is no consumer for the given queue: {receive_queue}")

        self.logger.info(f"Preparing to rpc call. Publish exchange: {publish_exchange}; Receive queue: {receive_queue}")
        await consumer.consume(self._rpc_response_callback)

        correlation_id = self._prepare_rpc_correlation_id()
        properties = dict(correlation_id=correlation_id, reply_to=receive_queue, expiration=ttl * 1000)
        await self.publish(body, publish_exchange, properties=properties, mandatory=True)
        self.logger.info(f"RPC message has been sent. {correlation_id}")

        future = await self._wait_result(correlation_id, timeout)
        self.logger.info(f"RPC message gets response. {correlation_id}")
        if future.exception():
            self.logger.error(f"RPC future returned exception: {future.exception()}")
            raise future.exception()
        if correlation_id in self._rpc_corr_id_dict:
            del self._rpc_corr_id_dict[correlation_id]
        return future.result()

    def _prepare_rpc_correlation_id(self) -> str:
        """
        Prepares a valid correlation_id for an RPC call, ensuring that it doesn't already exist in
        self._rpc_corr_id_dict.
        :return: Correlation ID
        """
        correlation_id = str(uuid.uuid1())
        while correlation_id in self._rpc_corr_id_dict:
            correlation_id = str(uuid.uuid1())
        self.logger.info(f"Starting rpc calling correlation id: {correlation_id}")

        future = self._loop.create_future()
        self._rpc_corr_id_dict[correlation_id] = future
        return correlation_id

    def _rpc_response_callback(self, message: IncomingMessage) -> None:
        """
        Handles RPC response message, setting Future result to message body
        :param message: RPC response message
        """
        self.logger.info(f"Received RPC response. Correlation id: {message.correlation_id}")
        if message.correlation_id in self._rpc_corr_id_dict:
            self.logger.info(f"Received RPC response. Correlation id: {message.correlation_id} was found")
            self._rpc_corr_id_dict[message.correlation_id].set_result(message.body)
        else:
            self.logger.warning(f"Received unexpected RPC response. Correlation id: {message.correlation_id}")
        message.ack()

    async def _wait_result(self, corr_id: str, timeout: Union[int, float, None] = None) -> asyncio.Future:
        """
        Wait for RPC result for given correlation ID until the given timeout fires.
        :param corr_id: Correlation ID to wait for
        :param timeout: Timeout in seconds
        :return: Future whose result we set
        """
        self.logger.info(f"Starting to wait for result. {corr_id}")
        try:
            future = self._rpc_corr_id_dict[corr_id]
        except KeyError:
            future = asyncio.Future()
            future.set_exception(KeyError(f"No future exists for correlation ID {corr_id}"))
            return future

        try:
            await asyncio.wait_for(future, timeout=timeout)
        except asyncio.TimeoutError:
            self.logger.error(f"RPC timeout. Correlation id: {corr_id}")
            del self._rpc_corr_id_dict[corr_id]
            future = asyncio.Future()
            future.set_exception(TimeoutError(f'RPC timeout. Correlation id: {corr_id}'))

        return future

    def status_check(self) -> bool:
        """
        :return: Whether or not the RabbitMQ connection being used is connected
        """
        return self._connection.is_connected()
