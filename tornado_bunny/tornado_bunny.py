import datetime
import functools
import logging
import uuid
import sys
import asyncio

from cached_property import cached_property
from pika import BasicProperties
from pika import URLParameters, ConnectionParameters
from tornado import gen
from tornado.concurrent import Future
from tornado.ioloop import IOLoop
from tornado.queues import Queue

from .async_connection import AsyncConnection


class TornadoAdapter(object):

    _USER_CLOSE_CODE = 0
    _NORMAL_CLOSE_CODE = 200
    _NO_ROUTE_CODE = 312

    def __init__(self, rabbitmq_url, io_loop=None):
        """
        An asynchronous RabbitMQ client, that use tornado to complete invoking.
        It is an `everything-in-one` RabbitMQ client, including following interfaces:
        1. publish - publish message.
        2. receive - consume messages from a queue. if received properties is not none, it publishes result back
           to `reply_to` queue.
        3. rpc - publish a message with replay_to properties, wait for answer message and return value.

        Architecture:
        This class encapsulate two async connections, one for publishing messages and one for consuming messages.

        :param rabbitmq_url: url for rabbitmq. it can be either '127.0.0.1' ("localhost") or 'amqp://dev:aispeech2018@10.12.7.22:5672/'
        :param io_loop: io loop. if it is none, using IOLoop.current() instead.
        """
        self._parameter = ConnectionParameters("127.0.0.1") if rabbitmq_url in ["localhost", "127.0.0.1"] else URLParameters(rabbitmq_url)
        if io_loop is None:
            io_loop = IOLoop.current()
        self._io_loop = io_loop
        self._publish_conn = AsyncConnection(rabbitmq_url, io_loop)
        self._receive_conn = AsyncConnection(rabbitmq_url, io_loop)
        self._rpc_exchange_dict = dict()
        self._rpc_corr_id_dict = dict()

    @cached_property
    def logger(self):
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.DEBUG)
        sh = logging.StreamHandler(sys.stdout)
        sh.setLevel(logging.DEBUG)
        logger.addHandler(sh)
        return logger

    def _create_channel(self, connection, return_callback=None,
                        cancel_callback=None, close_callback=None):
        self.logger.info("creating channel")
        future = Future()

        def on_channel_flow(*args, **kwargs):
            pass

        def on_channel_cancel(frame):
            self.logger.error("Channel was canceled")
            if cancel_callback is not None:
                if gen.is_coroutine_function(cancel_callback):
                    self._io_loop.spawn_callback(cancel_callback)
                else:
                    cancel_callback()

        def on_channel_closed(channel, reason):
            reply_code, reply_txt = reason.args
            self.logger.info(f'Channel {channel} was closed: {reason}')
            if reply_code not in [self._NORMAL_CLOSE_CODE, self._USER_CLOSE_CODE]:
                self.logger.error(f"Channel closed. reply code: {reply_code}; reply text: {reply_txt}. "
                                  f"System will exist")
                if close_callback is not None:
                    if gen.is_coroutine_function(close_callback):
                        self._io_loop.spawn_callback(close_callback)
                    else:
                        close_callback()
                else:
                    raise Exception(f"Channel was close unexpected. reply code: {reply_code}, reply text: {reply_txt}")
            else:
                self.logger.info(f"Reply code: {reply_code}, reply text: {reply_txt}")

        # if publish message failed, it will invoke this method.
        def on_channel_return(channel, method, property, body):
            self.logger.error(f"Rejected from server. reply code: {method.reply_code}, reply text: {method.reply_txt}")
            if return_callback is not None:
                if gen.is_coroutine_function(return_callback):
                    self._io_loop.spawn_callback(return_callback)
                else:
                    return_callback()
            else:
                raise Exception("Failed to publish message.")

        def open_callback(channel):
            self.logger.info("Created channel")
            channel.add_on_close_callback(on_channel_closed)
            channel.add_on_return_callback(on_channel_return)
            channel.add_on_flow_callback(on_channel_flow)
            channel.add_on_cancel_callback(on_channel_cancel)
            future.set_result(channel)

        connection.channel(on_open_callback=open_callback)
        return future

    def _exchange_declare(self, channel, exchange=None, exchange_type='topic', **kwargs):
        self.logger.info(f"Declaring exchange: {exchange}")
        future = Future()

        def on_exchange_declared(unframe):
            self.logger.info(f"Declared exchange: {exchange}")
            future.set_result(unframe)

        channel.exchange_declare(callback=on_exchange_declared, exchange=exchange, exchange_type=exchange_type, **kwargs)
        return future

    def _queue_declare(self, channel, queue='', **kwargs):
        self.logger.info(f"Declaring queue: {queue}")
        future = Future()

        def on_queue_declared(method_frame):
            self.logger.info(f"Declared queue: {method_frame.method.queue}")
            future.set_result(method_frame.method.queue)

        channel.queue_declare(callback=on_queue_declared, queue=queue, **kwargs)
        return future

    def _queue_bind(self, channel, queue, exchange, routing_key=None, **kwargs):
        self.logger.info(f"Binding queue: {queue} to exchange: {exchange}")
        future = Future()

        def callback(unframe):
            self.logger.info(f"bound queue: {queue} to exchange: {exchange}")
            future.set_result(unframe)

        channel.queue_bind(callback=callback, queue=queue, exchange=exchange, routing_key=routing_key, **kwargs)
        return future

    @gen.coroutine
    def publish(self, exchange, routing_key, body,
                properties=None,  mandatory=True, return_callback=None, close_callback=None):
        """
        publish message. creating a brand new channel once invoke this method. After publishing, it closes the
        channel.
        :param exchange: exchange name
        :type exchange; str or unicode
        :param routing_key: routing key (e.g. dog.yellow, cat.big)
        :param body: message
        :param properties: properties
        :param mandatory: whether
        :param return_callback: failed callback
        :param close_callback: channel close callback
        :return: None
        """
        try:
            self.logger.info("Try to publish message")
            conn = yield self._publish_conn.get_connection()
            self.logger.info(f"Preparing to publish. exchange: {exchange}; routing_key: {routing_key}")
            channel = yield self._create_channel(conn, return_callback=return_callback, close_callback=close_callback)
            try:
                if properties is None:
                    properties = BasicProperties(delivery_mode=2)
                self._exchange_declare(channel, exchange)
                channel.basic_publish(exchange=exchange, routing_key=routing_key, body=body,
                                      mandatory=mandatory, properties=properties)
            except Exception as e:
                self.logger.error(f"Failed to publish message. {str(e)}")
                raise Exception("Failed to publish message")
            finally:
                self.logger.info("Closing channel")
                channel.close()
        except Exception:
            raise Exception("Failed to publish message")

    @gen.coroutine
    def receive(self, exchange, routing_key, queue_name, handler, no_ack=False, prefetch_count=0,
                return_callback=None, close_callback=None, cancel_callback=None):
        """
        receive message. creating a brand new channel to consume message. Before consuming, it have to declaring
        exchange and queue. And bind queue to particular exchange with routing key. if received properties is not
        none, it publishes result back to `reply_to` queue.
        :param exchange: exchange name
        :param routing_key: routing key (e.g. dog.*, *.big)
        :param queue_name: queue name
        :param handler: message handler,
        :type handler def fn(body)
        :param no_ack: ack
        :param prefetch_count: prefetch count
        :param return_callback: channel return callback.
        :param close_callback: channel close callback
        :param cancel_callback: channel cancel callback
        :return: None
        """
        try:
            conn = yield self._receive_conn.get_connection()
            self.logger.info(f"[receive] exchange: {exchange}; routing key: {routing_key}; queue name: {queue_name}")
            channel = yield self._create_channel(conn, close_callback=close_callback, cancel_callback=cancel_callback)
            yield self._queue_declare(channel, queue=queue_name, auto_delete=False, durable=True)
            if routing_key != "":
                yield self._exchange_declare(channel, exchange=exchange)
                yield self._queue_bind(channel, exchange=exchange, queue=queue_name, routing_key=routing_key)
            self.logger.info(f"[start consuming] exchange: {exchange}; "
                             f"routing key: {routing_key}; queue name: {queue_name}")
            channel.basic_qos(prefetch_count=prefetch_count)
            channel.basic_consume(
                on_message_callback=functools.partial(
                    self._on_message,
                    exchange=exchange,
                    handler=handler,
                    return_callback=return_callback,
                    close_callback=close_callback),
                queue=queue_name,
                auto_ack=no_ack
            )
        except Exception as e:
            self.logger.error(f"Failed to receive message. {str(e)}")
            raise Exception("Failed to receive message")

    def _on_message(self, unused_channel, basic_deliver, properties, body, exchange, handler=None,
                    return_callback=None, close_callback=None):
        self.logger.info("Consuming message")
        self._io_loop.spawn_callback(self._process_message, unused_channel, basic_deliver, properties, body,
                                     exchange, handler, return_callback, close_callback)

    @gen.coroutine
    def _process_message(self, unused_channel, basic_deliver, properties, body, exchange, handler=None,
                         return_callback=None, close_callback=None):
        try:
            result = yield handler(body)
            self.logger.info("Message has been processed successfully")
            if properties is not None \
                    and properties.reply_to is not None:
                self.logger.info(f"Sending result back to "
                                 f"queue: {properties.reply_to}, correlation id: {properties.correlation_id}")
                yield self.publish(exchange=exchange,
                                   routing_key=properties.reply_to,
                                   properties=BasicProperties(correlation_id=properties.correlation_id),
                                   body=str(result), mandatory=False,
                                   return_callback=return_callback, close_callback=close_callback)
                self.logger.info(f"Sent result back to caller. "
                                 f"Queue: {properties.reply_to}, correlation id: {properties.correlation_id}")
        except Exception as e:
            import traceback
            self.logger.error(traceback.format_exc())
            raise Exception("Failed to handle received message.")
        finally:
            unused_channel.basic_ack(basic_deliver.delivery_tag)

    @gen.coroutine
    def rpc(self, exchange, routing_key, body, timeout, ttl,
            close_callback=None, return_callback=None, cancel_callback=None):
        """
        rpc call. It create a queue randomly when encounters first call with the same exchange name. Then, it starts
        consuming the created queue(waiting result). It publishes message to rabbitmq with properties that has correlation_id
        and reply_to. It will starts a coroutine to wait timeout and raises an `Exception("timeout")`.
        ttl is used to message's time to live in rabbitmq queue. If server has been sent result, it return it asynchronously.
        :param exchange: exchange name
        :param routing_key: routing key(e.g. dog.Yellow, cat.big)
        :param body: message
        :param timeout: rpc timeout (second)
        :param ttl: message's ttl (second)
        :type ttl: int
        :param close_callback: channel close callback
        :param return_callback: channel close callback
        :param cancel_callback: channel cancel callback
        :return: result or Exception("timeout")
        """
        conn = yield self._receive_conn.get_connection()
        self.logger.info(f"Preparing to rpc call. exchange: {exchange}; routing key: {routing_key}")
        if exchange not in self._rpc_exchange_dict:
            self._rpc_exchange_dict[exchange] = Queue(maxsize=1)
            callback_queue = yield self._initialize_rpc_callback(exchange, conn, cancel_callback=cancel_callback,
                                                                 close_callback=close_callback)
            yield self._rpc_exchange_dict[exchange].put(callback_queue)
        callback_queue = yield self._rpc_exchange_dict[exchange].get()
        yield self._rpc_exchange_dict[exchange].put(callback_queue)
        corr_id = str(uuid.uuid1())
        self.logger.info(f"Starting rpc calling correlation id: {corr_id}")
        if corr_id in self._rpc_corr_id_dict:
            self.logger.warning(f"Correlation id exists before calling. {corr_id}")
            del self._rpc_corr_id_dict[corr_id]
        self._rpc_corr_id_dict[corr_id] = Future()
        properties = BasicProperties(correlation_id=corr_id, reply_to=callback_queue, expiration=str(ttl*1000))
        yield self.publish(exchange, routing_key, body, properties=properties, mandatory=True,
                           close_callback=close_callback, return_callback=return_callback)
        self.logger.info(f"RPC message has been sent. {corr_id}")
        result = yield self._wait_result(corr_id, timeout)
        if corr_id in self._rpc_corr_id_dict:
            del self._rpc_corr_id_dict[corr_id]
        self.logger.info(f"RPC message gets response. {corr_id}")
        raise gen.Return(result)

    @gen.coroutine
    def _initialize_rpc_callback(self, exchange, conn, close_callback=None, cancel_callback=None):
        self.logger.info("Initialize rpc callback queue")
        rpc_channel = yield self._create_channel(conn, close_callback=close_callback, cancel_callback=cancel_callback)
        callback_queue = yield self._queue_declare(rpc_channel, auto_delete=True)
        self.logger.info(f"Callback queue: {callback_queue}")
        if exchange != "":
            yield self._exchange_declare(rpc_channel, exchange)
            yield self._queue_bind(rpc_channel, exchange=exchange, queue=callback_queue, routing_key=callback_queue)
        rpc_channel.basic_consume(on_message_callback=self._rpc_callback_process, queue=callback_queue)
        raise gen.Return(callback_queue)

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
            self._io_loop.add_timeout(datetime.timedelta(seconds=timeout), on_timeout)
        return future

    def status_check(self):
        return self._receive_conn.status_ok and self._publish_conn.status_ok

