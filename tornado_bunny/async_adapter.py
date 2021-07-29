import asyncio
import logging
import uuid
import sys
from types import FunctionType
from typing import Union

from aio_pika import Message, DeliveryMode, IncomingMessage
from cached_property import cached_property

from . import RabbitMQConnectionData, AsyncConnection, Consumer, Publisher


class AsyncAdapter:
    def __init__(self, rabbitmq_connection_data: RabbitMQConnectionData, configuration: dict,
                 loop: asyncio.AbstractEventLoop = None):
        """
        An asynchronous RabbitMQ client, that use tornado to complete invoking.
        It is an `all-in-one` RabbitMQ client, including following interfaces:
        1. publish - publish message.
        2. receive - consume messages from a queue. If received properties is not none, it publishes result back
           to `reply_to` queue.
        3. rpc - publish a message with replay_to properties, wait for answer message and return value.

        Architecture:
        This class encapsulate two async connections, one for publishing messages and one for consuming messages.

        :param rabbitmq_url: url for RabbitMQ. It can be either '127.0.0.1' ("localhost") or
                             'amqp://dev:aispeech2018@10.12.7.22:5672/'
        :param configuration: RabbitMQ configuration for both receiving and publishing.
                              It is separated by the keys `receive` and `publish`
        :param loop: io loop. If it is none, using asyncio.get_event_loop() instead
        """
        self._rabbitmq_connection_data = rabbitmq_connection_data
        self._loop = loop or asyncio.get_event_loop()
        self.configuration = configuration

        properties = dict(connection_name="aaaa")
        self._connection = AsyncConnection(rabbitmq_connection_data, self.logger, self._loop, properties)

        self._publishers = {
            publish_configuration["exchange_name"]: self.create_publisher(publish_configuration)
            for publish_configuration in self.configuration["publish"].values()
        }
        self._consumers = {
            receive_configuration["queue_name"]: self.create_consumer(receive_configuration)
            for receive_configuration in self.configuration["receive"].values()
        }
        self._rpc_corr_id_dict = dict()

    @cached_property
    def logger(self):
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.DEBUG)
        sh = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter('[%(asctime)s] [%(filename)-25s] [%(levelname)-8s] %(message)s')
        sh.setFormatter(formatter)
        sh.setLevel(logging.DEBUG)
        logger.addHandler(sh)
        logger.propagate = False
        return logger

    def create_publisher(self, configuration: dict) -> Publisher:
        return Publisher(connection=self._connection, logger=self.logger, loop=self._loop, **configuration)

    def create_consumer(self, configuration: dict) -> Consumer:
        return Consumer(connection=self._connection, logger=self.logger, loop=self._loop, **configuration)

    async def publish(self, body: bytes, exchange: str, properties: dict = None, mandatory: bool = True,
                      immediate: bool = False, timeout: Union[int, float, None] = None):
        """
        Publish a message. Creates a brand new channel in the first time, then uses the existing channel onwards.
        :param body: message
        :param exchange: The exchange to publish to
        :param properties: RabbitMQ message properties
        :param mandatory: RabbitMQ publish mandatory param
        """
        self.logger.info("Trying to publish message")
        if properties is None:
            properties = dict(delivery_mode=DeliveryMode.PERSISTENT)

        publisher = self._publishers.get(exchange)
        if publisher is None:
            self.logger.error("There is not publisher for the given exchange")

        try:
            message = Message(body, **properties)
            await publisher.publish(message, mandatory=mandatory, immediate=immediate, timeout=timeout)
        except Exception as e:
            self.logger.exception(f"Failed to publish message")
            raise Exception("Failed to publish message")

    async def receive(self, handler, queue: str, no_ack: bool = False) -> None:
        """
        Receive messages. Creates a brand new channel in the first time, then uses the existing channel onwards.
        The first time it declares exchange and queue, then bind the queue to the particular exchange with routing key.
        If received properties is not none, it publishes result back to `reply_to` queue.
        :param handler: message handler
        :type handler gen.coroutine def fn(logger, body)
        :param queue: The queue to consume from
        :param no_ack: whether to ack
        """
        consumer = self._consumers.get(queue)
        if consumer is None:
            self.logger.error("There is not consumer for the given queue")

        try:
            await consumer.consume(self._on_message, handler=handler, no_ack=no_ack)
        except Exception as e:
            self.logger.exception(f"Failed to receive message. {str(e)}")
            raise Exception("Failed to receive message")

    async def _on_message(self, message: IncomingMessage, handler: FunctionType):
        self.logger.info("Received a new message")
        try:
            result = await handler(self.logger, message)
            self.logger.info("Message has been processed successfully")
            if message.reply_to is not None:
                self.logger.info(f"Sending result back to "
                                 f"queue: {message.reply_to}, correlation id: {message.correlation_id}")
                publisher = list(self._publishers.values())[0]
                response_message = Message(body=result,
                                           correlation_id=message.correlation_id,
                                           reply_to=message.reply_to)
                await publisher.publish(message=response_message, mandatory=False)
                self.logger.info(f"Sent result back to caller. "
                                 f"Queue: {message.reply_to}, correlation id: {message.correlation_id}")
        except Exception as e:
            self.logger.exception("Failed to handle received message.")
            raise Exception("Failed to handle received message.")
        finally:
            message.ack()

    async def rpc(self, body: bytes, receive_queue: str, publish_exchange: str, timeout: Union[int, float], ttl: int):
        """
        RPC call. It consumes the receiving queue (waiting result).
        It then publishes message to RabbitMQ with properties that has correlation_id and reply_to.
        It will start a coroutine to wait a timeout and raise an `Exception("timeout")` if met.
        If server has been sent result, it returns it asynchronously.
        :param body: message
        :param receive_queue: The queue to consume
        :param publish_exchange: The exchange to publish to
        :param timeout: rpc timeout (seconds)
        :param ttl:  message's time to live in the RabbitMQ queue (seconds)
        :type ttl: int
        :return: result or Exception("timeout")
        """
        consumer = self._consumers.get(receive_queue)
        if consumer is None:
            self.logger.error("There is not receiver for the given queue")

        self.logger.info(f"Preparing to rpc call. Publish exchange: {publish_exchange}; Receive queue: {receive_queue}")
        await consumer.consume(self._rpc_callback_process)

        correlation_id = str(uuid.uuid1())
        self.logger.info(f"Starting rpc calling correlation id: {correlation_id}")
        if correlation_id in self._rpc_corr_id_dict:
            self.logger.warning(f"Correlation id exists before calling. {correlation_id}")
            del self._rpc_corr_id_dict[correlation_id]

        future = self._loop.create_future()
        self._rpc_corr_id_dict[correlation_id] = future
        properties = dict(correlation_id=correlation_id, reply_to=receive_queue, expiration=ttl*1000)
        await self.publish(body, publish_exchange, properties=properties, mandatory=True)
        self.logger.info(f"RPC message has been sent. {correlation_id}")

        await self._wait_result(correlation_id, timeout)
        self.logger.info(f"RPC message gets response. {correlation_id}")
        if future.exception():
            self.logger.error(f"RPC future returned exception: {future.exception()}")
        if correlation_id in self._rpc_corr_id_dict:
            del self._rpc_corr_id_dict[correlation_id]
        return future.result()

    def _rpc_callback_process(self, message: IncomingMessage):
        self.logger.info(f"RPC get response, correlation id: {message.correlation_id}")
        if message.correlation_id in self._rpc_corr_id_dict:
            self.logger.info(f"RPC get response, correlation id: {message.correlation_id} was found in state dict")
            self._rpc_corr_id_dict[message.correlation_id].set_result(message.body)
        else:
            self.logger.warning(f"RPC get non exist response. Correlation id: {message.correlation_id}")
        message.ack()

    async def _wait_result(self, corr_id: str, timeout: Union[int, float, None] = None):
        self.logger.info(f"Beginning waiting for result. {corr_id}")
        future = self._rpc_corr_id_dict[corr_id]
        try:
            await asyncio.wait_for(future, timeout=timeout)
        except asyncio.TimeoutError:
            if corr_id in self._rpc_corr_id_dict:
                self.logger.error(f"RPC timeout. Correlation id: {corr_id}")
                del self._rpc_corr_id_dict[corr_id]
                future.set_exception(Exception(f'RPC timeout. Correlation id: {corr_id}'))

        return future

    def status_check(self):
        return self._connection.is_connected()
