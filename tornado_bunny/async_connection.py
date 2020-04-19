import sys
import logging

from cached_property import cached_property
from pika import URLParameters, ConnectionParameters
from pika.adapters.tornado_connection import TornadoConnection
from tornado import gen
from tornado.queues import Queue


class AsyncConnection(object):
    INIT_STATUS = "init"
    CONNECTING_STATUS = "connecting"
    OPEN_STATUS = "open"
    CLOSE_STATUS = "close"
    TIMEOUT_STATUS = 'timeout'

    def __init__(self, rabbitmq_url, io_loop, timeout=10):
        self.should_reconnect = False

        self._parameters = ConnectionParameters("127.0.0.1") if rabbitmq_url in ["localhost", "127.0.0.1"] else \
            URLParameters(rabbitmq_url)
        self._io_loop = io_loop
        self._timeout = timeout
        self._connection_queue = Queue(maxsize=1)
        self._current_status = self.INIT_STATUS

    @cached_property
    def logger(self):
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.DEBUG)
        sh = logging.StreamHandler(sys.stdout)
        sh.setLevel(logging.DEBUG)
        logger.addHandler(sh)
        return logger

    @property
    def status_ok(self):
        return self._current_status != self.CLOSE_STATUS \
               and self._current_status != self.TIMEOUT_STATUS

    @gen.coroutine
    def get_connection(self):
        if self._current_status == self.INIT_STATUS:
            self._current_status = self.CONNECTING_STATUS
            self._connect()
        conn = yield self._top()
        return conn

    def _connect(self):
        try:
            self._try_connect()
        except Exception as e:
            self.logger.error("failed to connect rabbitmq. %s", e)

    @gen.coroutine
    def _top(self):
        conn = yield self._connection_queue.get()
        self._connection_queue.put(conn)
        return conn

    def _on_timeout(self):
        if self._current_status == self.CONNECTING_STATUS:
            self.logger.error("creating connection timed out")
            self._current_status = self.TIMEOUT_STATUS
            self.stop()

    def _open_callback(self, connection):
        self.logger.info("created connection")
        self._current_status = self.OPEN_STATUS
        self._connection_queue.put(connection)

    def _open_error_callback(self, connection, exception):
        self.logger.error(f"open connection with error: {exception}")
        self._current_status = self.CLOSE_STATUS
        self.reconnect()

    def _close_callback(self, connection, reply_code, reply_text):
        self.logger.error(f"closing connection: reply code: {reply_code}, reply_text: {reply_text}. system will exist")
        self._current_status = self.CLOSE_STATUS
        self.reconnect()

    def reconnect(self):
        self.should_reconnect = True
        self.stop()

    def stop(self):
        self.logger.info('Stopping')
        try:
            self._io_loop.stop()
        except:
            pass

        if self.should_reconnect:
            self._current_status = self.INIT_STATUS
            self._connect()
            self._io_loop.start()

    def _try_connect(self):
        self.logger.info("creating connection")
        self._io_loop.call_later(self._timeout, self._on_timeout)

        TornadoConnection(self._parameters,
                          on_open_callback=self._open_callback,
                          on_open_error_callback=self._open_error_callback,
                          on_close_callback=self._close_callback,
                          custom_ioloop=self._io_loop)
