import datetime
import functools
import logging

from pika import URLParameters, ConnectionParameters
from pika.adapters.tornado_connection import TornadoConnection
from tornado import gen
from tornado.concurrent import Future
from tornado.queues import Queue


class AsyncConnection(object):
    INIT_STATUS = "init"
    CONNECTING_STATUS = "connecting"
    OPEN_STATUS = "open"
    CLOSE_STATUS = "close"
    TIMEOUT_STATUS = 'timeout'

    def __init__(self, rabbitmq_url, io_loop, timeout=10):
        self._parameter = ConnectionParameters("127.0.0.1") if rabbitmq_url in ["localhost", "127.0.0.1"] else \
            URLParameters(rabbitmq_url)
        self._io_loop = io_loop
        self._timeout = timeout
        self._logger = logging.getLogger(__name__)
        self._queue = Queue(maxsize=1)
        self._current_status = self.INIT_STATUS

    @property
    def logger(self):
        return self._logger

    @property
    def status_ok(self):
        return self._current_status != self.CLOSE_STATUS \
               and self._current_status != self.TIMEOUT_STATUS

    @gen.coroutine
    def get_connection(self):
        if self._current_status == self.INIT_STATUS:
            self._current_status = self.CONNECTING_STATUS
            yield self._connect()
        conn = yield self._top()
        raise gen.Return(conn)

    @gen.coroutine
    def _connect(self):
        try:
            connection = yield self._try_connect()
            if connection is not None:
                self._queue.put(connection)
        except Exception as e:
            self.logger.error("failed to connect rabbitmq. %s", e)

    @gen.coroutine
    def _top(self):
        conn = yield self._queue.get()
        self._queue.put(conn)
        raise gen.Return(conn)

    def _on_timeout(self, future):
        if self._current_status == self.CONNECTING_STATUS:
            self.logger.error("creating connection time out")
            self._current_status = self.TIMEOUT_STATUS
            future.set_exception(Exception("connection Timeout"))

    def _try_connect(self):
        self.logger.info("creating connection")
        future = Future()
        self._io_loop.add_timeout(datetime.timedelta(seconds=self._timeout),
                                  functools.partial(self._on_timeout, future=future))

        def open_callback(unused_connection):
            self.logger.info("created connection")
            self._current_status = self.OPEN_STATUS
            future.set_result(unused_connection)

        def open_error_callback(connection, exception):
            self.logger.error("open connection with error: %s", exception)
            self._current_status = self.CLOSE_STATUS
            future.set_exception(exception)

        def close_callback(connection, reply_code, reply_text):
            self.logger.error("closing connection: reply code:%s, reply_text: %s. system will exist",
                              reply_code, reply_text)
            self._current_status = self.CLOSE_STATUS

        TornadoConnection(self._parameter,
                          on_open_callback=open_callback,
                          on_open_error_callback=open_error_callback,
                          on_close_callback=close_callback,
                          custom_ioloop=self._io_loop)
        return future
