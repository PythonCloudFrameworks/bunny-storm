import datetime
import functools
import logging
import uuid

from pika import BasicProperties
from pika import URLParameters, ConnectionParameters
from tornado import gen
from tornado.concurrent import Future
from tornado.ioloop import IOLoop
from tornado.queues import Queue

from AsyncConnection import AsyncConnection


class TornadoAdapter(object):

    _USER_CLOSE_CODE = 0
    _NORMAL_CLOSE_CODE = 200
    _NO_ROUTE_CODE = 312

    @property
    def logger(self):
        return self._logger

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
        self._logger = logging.getLogger(__name__)
        self._publish_conn = AsyncConnection(rabbitmq_url, io_loop)
        self._receive_conn = AsyncConnection(rabbitmq_url, io_loop)
        self._rpc_exchange_dict = dict()
        self._rpc_corr_id_dict = dict()

    def _create_channel(self, connection, return_callback=None,
                        cancel_callback=None, close_callback=None):
        self.logger.info("creating channel")
        future = Future()

        def on_channel_flow(*args, **kwargs):
            pass

        def on_channel_cancel(frame):
            self.logger.error("channel was canceled")
            if cancel_callback is not None:
                if hasattr(cancel_callback, "__tornado_coroutine__"):
                    self._io_loop.spawn_callback(cancel_callback)
                else:
                    cancel_callback()

        def on_channel_closed(channel, reason):
            reply_code, reply_txt = reason.args
            self.logger.info('Channel %i was closed: %s', channel, reason)
            if reply_code not in [self._NORMAL_CLOSE_CODE, self._USER_CLOSE_CODE]:
                self.logger.error("channel closed. reply code: %d; reply text: %s. system will exist"
                                  , reply_code, reply_txt)
                if close_callback is not None:
                    if hasattr(close_callback, "__tornado_coroutine__"):
                        self._io_loop.spawn_callback(close_callback)
                    else:
                        close_callback()
                else:
                    raise Exception("channel was close unexpected. reply code: %d, reply text: %s"
                                    % (reply_code, reply_txt,))
            else:
                self.logger.info("reply code %s, reply txt: %s", reply_code, reply_txt)

        # if publish message failed, it will invoke this method.
        def on_channel_return(channel, method, property, body):
            self.logger.error("reject from server. reply code: %d, reply text: %s.",
                              method.reply_code, method.reply_text)
            if return_callback is not None:
                if hasattr(return_callback, "__tornado_coroutine__"):
                    self._io_loop.spawn_callback(return_callback)
                else:
                    return_callback()
            else:
                raise Exception("failed to publish message.")

        def open_callback(channel):
            self.logger.info("created channel")
            channel.add_on_close_callback(on_channel_closed)
            channel.add_on_return_callback(on_channel_return)
            channel.add_on_flow_callback(on_channel_flow)
            channel.add_on_cancel_callback(on_channel_cancel)
            future.set_result(channel)

        connection.channel(on_open_callback=open_callback)
        return future

    def _exchange_declare(self, channel, exchange=None, exchange_type='topic', **kwargs):
        self.logger.info("declaring exchange: %s " % exchange)
        future = Future()

        def on_exchange_declared(unframe):
            self.logger.info("declared exchange: %s", exchange)
            future.set_result(unframe)

        channel.exchange_declare(callback=on_exchange_declared, exchange=exchange, exchange_type=exchange_type, **kwargs)
        return future

    def _queue_declare(self, channel, queue='', **kwargs):
        self.logger.info("declaring queue: %s" % queue)
        future = Future()

        def on_queue_declared(method_frame):
            self.logger.info("declared queue: %s", method_frame.method.queue)
            future.set_result(method_frame.method.queue)

        channel.queue_declare(callback=on_queue_declared, queue=queue, **kwargs)
        return future

    def _queue_bind(self, channel, queue, exchange, routing_key=None, **kwargs):
        self.logger.info("binding queue: %s to exchange: %s", queue, exchange)
        future = Future()

        def callback(unframe):
            self.logger.info("bound queue: %s to exchange: %s", queue, exchange)
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
            self.logger.info("preparing to publish. exchange: %s; routing_key: %s", exchange, routing_key)
            channel = yield self._create_channel(conn, return_callback=return_callback, close_callback=close_callback)
            try:
                if properties is None:
                    properties = BasicProperties(delivery_mode=2)
                channel.basic_publish(exchange=exchange, routing_key=routing_key, body=body,
                                      mandatory=mandatory, properties=properties)
            except Exception as e:
                self.logger.error("failed to publish message. %s", str(e))
                raise Exception("failed to publish message")
            finally:
                self.logger.info("closing channel")
                channel.close()
        except Exception:
            raise Exception("failed to publish message")

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
            self.logger.info("[receive] exchange: %s; routing key: %s; queue name: %s", exchange, routing_key, queue_name)
            channel = yield self._create_channel(conn, close_callback=close_callback, cancel_callback=cancel_callback)
            yield self._queue_declare(channel, queue=queue_name, auto_delete=False, durable=True)
            if routing_key != "":
                yield self._exchange_declare(channel, exchange=exchange)
                yield self._queue_bind(channel, exchange=exchange, queue=queue_name, routing_key=routing_key)
            self.logger.info("[start consuming] exchange: %s; routing key: %s; queue name: %s",
                             exchange, routing_key, queue_name)
            channel.basic_qos(prefetch_count=prefetch_count)
            channel.basic_consume(on_message_callback=functools.partial(self._on_message, exchange=exchange, handler=handler,
                                                    return_callback=return_callback, close_callback=close_callback),
                                  queue=queue_name, auto_ack=no_ack)
        except Exception as e:
            self.logger.error("failed to receive message. %s", str(e))
            raise Exception("failed to receive message")

    def _on_message(self, unused_channel, basic_deliver, properties, body, exchange, handler=None,
                    return_callback=None, close_callback=None):
        self.logger.info("consuming message")
        self._io_loop.spawn_callback(self._process_message, unused_channel, basic_deliver, properties, body,
                                     exchange, handler, return_callback, close_callback)

    @gen.coroutine
    def _process_message(self, unused_channel, basic_deliver, properties, body, exchange, handler=None,
                         return_callback=None, close_callback=None):
        try:
            result = yield handler(body)
            self.logger.info("message has been processed successfully")
            if properties is not None \
                    and properties.reply_to is not None:
                self.logger.info("sending result back to queue: %s, correlation id: %s",
                                 properties.reply_to, properties.correlation_id)
                yield self.publish(exchange=exchange,
                                   routing_key=properties.reply_to,
                                   properties=BasicProperties(correlation_id=properties.correlation_id),
                                   body=str(result), mandatory=False,
                                   return_callback=return_callback, close_callback=close_callback)
                self.logger.info("sent result back to caller. queue : %s, correlation id: %s",
                                 properties.reply_to, properties.correlation_id)
        except Exception as e:
            import traceback
            self.logger.error(traceback.format_exc())
            raise Exception("failed to handle received message.")
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
        self.logger.info("preparing to rpc call. exchange: %s; routing key: %s", exchange, routing_key)
        if exchange not in self._rpc_exchange_dict:
            self._rpc_exchange_dict[exchange] = Queue(maxsize=1)
            callback_queue = yield self._initialize_rpc_callback(exchange, conn, cancel_callback=cancel_callback,
                                                                 close_callback=close_callback)
            yield self._rpc_exchange_dict[exchange].put(callback_queue)
        callback_queue = yield self._rpc_exchange_dict[exchange].get()
        yield self._rpc_exchange_dict[exchange].put(callback_queue)
        corr_id = str(uuid.uuid1())
        self.logger.info("starting rpc calling correlation id: %s", corr_id)
        if corr_id in self._rpc_corr_id_dict:
            self.logger.warning("correlation id exists before calling. %s", corr_id)
            del self._rpc_corr_id_dict[corr_id]
        self._rpc_corr_id_dict[corr_id] = Future()
        properties = BasicProperties(correlation_id=corr_id, reply_to=callback_queue, expiration=str(ttl*1000))
        yield self.publish(exchange, routing_key, body,
                           properties=properties, mandatory=True,
                           close_callback=close_callback, return_callback=return_callback)
        self.logger.info("rpc message has been sent. %s", corr_id)
        result = yield self._wait_result(corr_id, timeout)
        if corr_id in self._rpc_corr_id_dict:
            del self._rpc_corr_id_dict[corr_id]
        self.logger.info("rpc message gets response. %s", corr_id)
        raise gen.Return(result)

    @gen.coroutine
    def _initialize_rpc_callback(self, exchange, conn, close_callback=None, cancel_callback=None):
        self.logger.info("initialize rpc callback queue")
        rpc_channel = yield self._create_channel(conn, close_callback=close_callback, cancel_callback=cancel_callback)
        callback_queue = yield self._queue_declare(rpc_channel, auto_delete=True)
        self.logger.info("callback queue: %s", callback_queue)
        if exchange != "":
            yield self._exchange_declare(rpc_channel, exchange)
            yield self._queue_bind(rpc_channel, exchange=exchange, queue=callback_queue, routing_key=callback_queue)
        rpc_channel.basic_consume(on_message_callback=self._rpc_callback_process, queue=callback_queue)
        raise gen.Return(callback_queue)

    def _rpc_callback_process(self, unused_channel, basic_deliver, properties, body):
        self.logger.info("rpc get response, correlation id: %s", properties.correlation_id)
        if properties.correlation_id in self._rpc_corr_id_dict:
            self.logger.info("rpc get response, correlation id: %s was found in state dict", properties.correlation_id)
            self._rpc_corr_id_dict[properties.correlation_id].set_result(body)
        else:
            self.logger.warning("rpc get non exist response. correlation id: %s", properties.correlation_id)
        unused_channel.basic_ack(basic_deliver.delivery_tag)

    def _wait_result(self, corr_id, timeout=None):
        self.logger.info("begin waiting result. %s", corr_id)
        future = self._rpc_corr_id_dict[corr_id]

        def on_timeout():
            if corr_id in self._rpc_corr_id_dict:
                self.logger.error("rpc timeout. corr id : %s", corr_id)
                del self._rpc_corr_id_dict[corr_id]
                future.set_exception(Exception('rpc timeout. corr id: %s' % corr_id))

        if timeout is not None:
            self._io_loop.add_timeout(datetime.timedelta(seconds=timeout), on_timeout)
        return future

    def status_check(self):
        return self._receive_conn.status_ok and self._publish_conn.status_ok
