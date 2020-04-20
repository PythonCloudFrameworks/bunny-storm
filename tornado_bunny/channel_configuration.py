import functools

from tornado import gen
from tornado.ioloop import IOLoop
from tornado.queues import Queue
from pika import BasicProperties


class ChannelConfiguration:
    _USER_CLOSE_CODE = 0
    _NORMAL_CLOSE_CODE = 200
    _NO_ROUTE_CODE = 312

    def __init__(self, connection, logger, io_loop=None, exchange=None, exchange_type=None, queue=None,
                 routing_key=None, durable=None, auto_delete=None, prefetch_count=None):
        self.logger = logger
        self._connection = connection
        if io_loop is None:
            io_loop = IOLoop.current()
        self._io_loop = io_loop
        self._channel_queue = Queue(maxsize=1)
        self._queue = queue if queue is not None else ""
        self._exchange = exchange
        if exchange_type is None:
            exchange_type = 'topic'
        self._exchange_type = exchange_type
        self._routing_key = routing_key
        self._durable = durable
        self._auto_delete = auto_delete
        if prefetch_count is None:
            prefetch_count = 1
        self._prefetch_count = prefetch_count

    def consume(self, on_message_callback, handler=None, no_ack=False):
        self.logger.info(f"[start consuming] exchange: {self._exchange}; "
                         f"routing key: {self._routing_key}; queue name: {self._queue}")
        channel = yield self._get_channel()

        if handler is not None:
            channel.basic_consume(
                queue=self._queue,
                on_message_callback=functools.partial(
                    on_message_callback,
                    exchange=self._exchange,
                    handler=handler,
                    queue=self._queue,
                    auto_ack=no_ack
                )
            )
        else:
            channel.basic_consume(queue=self._queue, on_message_callback=on_message_callback)

    def publish(self, body, mandatory=None, properties=None):
        if properties is None:
            properties = BasicProperties(delivery_mode=2)

        channel = yield self._get_channel()
        self.logger.info(f"Publishing message. exchange: {self._exchange}; routing_key: {self._routing_key}")
        channel.basic_publish(exchange=self._exchange, routing_key=self._routing_key, body=body,
                              mandatory=mandatory, properties=properties)

    def _get_channel(self):
        if self._channel_queue.empty():
            channel = self._create_channel()
            self._channel_queue.put(channel)

        channel = yield self._top()
        return channel

    @gen.coroutine
    def _top(self):
        conn = yield self._channel_queue.get()
        self._channel_queue.put(conn)
        return conn

    def _create_channel(self):
        self.logger.info("creating channel")

        def on_channel_flow(*args, **kwargs):
            pass

        def on_channel_cancel(frame):
            self.logger.error("Channel was canceled")
            if not self._channel_queue.empty():
                channel = yield self._channel_queue.get()
                if channel and not channel.is_close or channel.is_closing:
                    channel.close()

        def on_channel_closed(channel, reason):
            reply_code, reply_txt = reason.args
            self.logger.info(f'Channel {channel} was closed: {reason}')
            if reply_code not in [self._NORMAL_CLOSE_CODE, self._USER_CLOSE_CODE]:
                self.logger.error(f"Channel closed. reply code: {reply_code}; reply text: {reply_txt}. "
                                  f"System will exist")
                raise Exception(f"Channel was close unexpected. reply code: {reply_code}, reply text: {reply_txt}")
            else:
                self.logger.info(f"Reply code: {reply_code}, reply text: {reply_txt}")

        def on_channel_return(channel, method, property, body):
            """"If publish message has failed, this method will be invoked."""
            self.logger.error(f"Rejected from server. reply code: {method.reply_code}, reply text: {method.reply_txt}")
            raise Exception("Failed to publish message.")

        def open_callback(channel):
            self.logger.info("Created channel")
            channel.add_on_close_callback(on_channel_closed)
            channel.add_on_return_callback(on_channel_return)
            channel.add_on_flow_callback(on_channel_flow)
            channel.add_on_cancel_callback(on_channel_cancel)
            self._channel_queue.put(channel)
            self._exchange_declare()

        connection = self._connection.get_connection()
        connection.channel(on_open_callback=open_callback)

    def _exchange_declare(self):
        self.logger.info(f"Declaring exchange: {self._exchange}")

        channel = yield self._get_channel()
        channel.exchange_declare(
            callback=self._on_exchange_declared,
            exchange=self._exchange,
            exchange_type=self._exchange_type,
            durable=self._durable,
            auto_delete=self._auto_delete)

    def _on_exchange_declared(self, unframe):
        self.logger.info(f"Declared exchange: {self._exchange}")
        self._queue_declare()

    def _queue_declare(self):
        self.logger.info(f"Declaring queue: {self._queue}")

        channel = yield self._get_channel()
        channel.queue_declare(
            callback=self._on_queue_declared, queue=self._queue, durable=self._durable, auto_delete=self._auto_delete)

    def _on_queue_declared(self, method_frame):
        self.logger.info(f"Declared queue: {method_frame.method.queue}")
        self._queue = method_frame.method.queue
        self._queue_bind()

    def _queue_bind(self):
        self.logger.info(f"Binding queue: {self._queue} to exchange: {self._exchange}")

        channel = yield self._get_channel()
        channel.queue_bind(
            callback=self._on_queue_bind_ok, queue=self._queue, exchange=self._exchange, routing_key=self._routing_key)

    def _on_queue_bind_ok(self, unframe):
        self.logger.info(f"bound queue: {self._queue} to exchange: {self._exchange}")
        channel = yield self._get_channel()
        channel.basic_qos(prefetch_count=self._prefetch_count)
