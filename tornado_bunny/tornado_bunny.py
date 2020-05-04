import logging
import uuid
import sys

from cached_property import cached_property
from pika import BasicProperties
from pika import URLParameters, ConnectionParameters
from tornado import gen
from tornado.ioloop import IOLoop
from tornado.concurrent import Future

from .async_connection import AsyncConnection
from .channel_configuration import ChannelConfiguration


class TornadoAdapter:
    def __init__(self, rabbitmq_url, configuration, io_loop=None):
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
        :param io_loop: io loop. If it is none, using IOLoop.current() instead
        """
        self._parameter = ConnectionParameters("127.0.0.1") \
            if rabbitmq_url in ["localhost", "127.0.0.1"] else URLParameters(rabbitmq_url)
        if io_loop is None:
            io_loop = IOLoop.current()
        self._io_loop = io_loop
        self.configuration = configuration
        self._publish_connection = AsyncConnection(rabbitmq_url, io_loop, self.logger)
        self._publish_channels = {
            publish_configuration["exchange"]: ChannelConfiguration(
                self._publish_connection, self.logger, io_loop, **publish_configuration)
            for publish_configuration in configuration["publish"].values()
        }
        self._receive_connection = AsyncConnection(rabbitmq_url, io_loop, self.logger)
        self._receive_channels = {
            receive_configuration["queue"]: ChannelConfiguration(
                self._receive_connection, self.logger, io_loop, **receive_configuration)
            for receive_configuration in configuration["receive"].values()
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

    @gen.coroutine
    def publish(self, body, exchange, properties=None,  mandatory=True):
        """
        Publish a message. Creates a brand new channel in the first time, then uses the existing channel onwards.
        :param body: message
        :param exchange: The exchange to publish to
        :param properties: RabbitMQ message properties
        :param mandatory: RabbitMQ publish mandatory param
        """
        self.logger.info("Trying to publish message")
        if properties is None:
            properties = BasicProperties(delivery_mode=2)

        publish_channel = self._publish_channels.get(exchange)
        if publish_channel is None:
            self.logger.error("There is not publisher for the given exchange")

        try:
            yield publish_channel.publish(body, properties=properties, mandatory=mandatory)
        except Exception as e:
            self.logger.exception(f"Failed to publish message")
            raise Exception("Failed to publish message")

    @gen.coroutine
    def receive(self, handler, queue, no_ack=False):
        """
        Receive messages. Creates a brand new channel in the first time, then uses the existing channel onwards.
        The first time it declares exchange and queue, then bind the queue to the particular exchange with routing key.
        If received properties is not none, it publishes result back to `reply_to` queue.
        :param handler: message handler
        :type handler gen.coroutine def fn(logger, body)
        :param queue: The queue to consume from
        :param no_ack: whether to ack
        """
        receive_channel = self._receive_channels.get(queue)
        if receive_channel is None:
            self.logger.error("There is not receiver for the given queue")

        try:
            yield receive_channel.consume(self._on_message, handler=handler, no_ack=no_ack)
        except Exception as e:
            self.logger.exception(f"Failed to receive message. {str(e)}")
            raise Exception("Failed to receive message")

    def _on_message(self, unused_channel, basic_deliver, properties, body, handler=None):
        self.logger.info("Received a new message")
        self._io_loop.call_later(0.01, self._process_message, unused_channel, basic_deliver, properties, body, handler)

    @gen.coroutine
    def _process_message(self, unused_channel, basic_deliver, properties, body, handler=None):
        try:
            result = yield handler(self.logger, body)
            self.logger.info("Message has been processed successfully")
            if properties is not None and properties.reply_to is not None:
                self.logger.info(f"Sending result back to "
                                 f"queue: {properties.reply_to}, correlation id: {properties.correlation_id}")
                publish_channel = list(self._publish_channels.values())[0]
                yield publish_channel.publish(properties=BasicProperties(correlation_id=properties.correlation_id),
                                              body=str(result),
                                              mandatory=False,
                                              reply_to=properties.reply_to
                                              )
                self.logger.info(f"Sent result back to caller. "
                                 f"Queue: {properties.reply_to}, correlation id: {properties.correlation_id}")
        except Exception as e:
            self.logger.exception("Failed to handle received message.")
            raise Exception("Failed to handle received message.")
        finally:
            unused_channel.basic_ack(basic_deliver.delivery_tag)

    @gen.coroutine
    def rpc(self, body, receive_queue, publish_exchange, timeout, ttl):
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
        receive_channel = self._receive_channels.get(receive_queue)
        if receive_channel is None:
            self.logger.error("There is not receiver for the given queue")

        self.logger.info(f"Preparing to rpc call. Publish exchange: {publish_exchange}; Receive queue: {receive_queue}")
        yield receive_channel.consume(self._rpc_callback_process)

        correlation_id = str(uuid.uuid1())
        self.logger.info(f"Starting rpc calling correlation id: {correlation_id}")
        if correlation_id in self._rpc_corr_id_dict:
            self.logger.warning(f"Correlation id exists before calling. {correlation_id}")
            del self._rpc_corr_id_dict[correlation_id]

        self._rpc_corr_id_dict[correlation_id] = Future()
        properties = BasicProperties(
            correlation_id=correlation_id, reply_to=receive_queue, expiration=str(ttl*1000))
        yield self.publish(body, publish_exchange, properties=properties, mandatory=True)
        self.logger.info(f"RPC message has been sent. {correlation_id}")
        result = yield self._wait_result(correlation_id, timeout)
        if correlation_id in self._rpc_corr_id_dict:
            del self._rpc_corr_id_dict[correlation_id]
        self.logger.info(f"RPC message gets response. {correlation_id}")
        return result

    def _rpc_callback_process(self, unused_channel, basic_deliver, properties, body):
        self.logger.info(f"RPC get response, correlation id: {properties.correlation_id}")
        if properties.correlation_id in self._rpc_corr_id_dict:
            self.logger.info(f"RPC get response, correlation id: {properties.correlation_id} was found in state dict")
            self._rpc_corr_id_dict[properties.correlation_id].set_result(body)
        else:
            self.logger.warning(f"RPC get non exist response. Correlation id: {properties.correlation_id}")
        unused_channel.basic_ack(basic_deliver.delivery_tag)

    def _wait_result(self, corr_id, timeout=None):
        self.logger.info(f"Beginning waiting for result. {corr_id}")
        future = self._rpc_corr_id_dict[corr_id]

        def on_timeout():
            if corr_id in self._rpc_corr_id_dict:
                self.logger.error(f"RPC timeout. Correlation id: {corr_id}")
                del self._rpc_corr_id_dict[corr_id]
                future.set_exception(Exception(f'RPC timeout. Correlation id: {corr_id}'))

        if timeout is not None:
            self._io_loop.call_later(timeout, on_timeout)
        return future

    def status_check(self):
        return self._receive_connection.status_ok and self._publish_connection.status_ok
