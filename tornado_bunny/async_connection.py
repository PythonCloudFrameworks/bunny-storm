import asyncio
from logging import Logger
from typing import Union

from aio_pika import connect_robust, RobustConnection, RobustChannel

from . import RabbitMQConnectionData


class AsyncConnection:
    _logger: Logger
    _loop: asyncio.AbstractEventLoop
    _connection: Union[RobustConnection, None]
    _channel: Union[RobustChannel, None]

    _rabbitmq_url: RabbitMQConnectionData
    _properties: dict
    _prefetch_count: int

    _timeout: int
    _connection_attempts: int
    _attempt_backoff: int

    _connection_lock: asyncio.Lock

    def __init__(self, rabbitmq_connection_data: RabbitMQConnectionData, logger: Logger,
                 loop: asyncio.AbstractEventLoop = None, properties: dict = None,  timeout: Union[int, float] = 10,
                 connection_attempts: int = 5, attempt_backoff: int = 5):
        self._logger = logger
        self._loop = loop or asyncio.get_event_loop()

        self._rabbitmq_connection_data = rabbitmq_connection_data
        self._properties = properties or dict()

        self._timeout = timeout
        self._connection_attempts = connection_attempts
        self._attempt_backoff = attempt_backoff

        self._connection_lock = asyncio.Lock()
        self._connection = None

    @property
    def logger(self) -> Logger:
        return self._logger

    async def get_connection(self) -> RobustConnection:
        await self._connection_lock.acquire()
        if not self.is_connected():
            try:
                self._connection = await self._connect()
            except ConnectionError:
                self.logger.error("Failed to connect to RabbitMQ, stopping loop.")
                self._loop.stop()
        self._connection_lock.release()
        return self._connection

    def is_connected(self) -> bool:
        return not ((self._connection is None) or (self._connection.connection is None))

    async def _connect(self) -> RobustConnection:
        for attempt_num in range(1, self._connection_attempts + 1):
            self.logger.info(f"Creating connection to RabbitMQ. Attempt: {attempt_num}")
            try:
                connection = await connect_robust(url=self._rabbitmq_connection_data.uri(),
                                                  loop=self._loop,
                                                  timeout=self._timeout,
                                                  client_properties=self._properties)
                return connection
            except (ConnectionError):
                self.logger.error(f"Connection attempt {attempt_num} / {self._connection_attempts} failed")
                if attempt_num < self._connection_attempts:
                    self.logger.debug(f"Going to sleep for {self._attempt_backoff} seconds")
                    await asyncio.sleep(self._attempt_backoff)
        raise ConnectionError(f"Failed to connect to RabbitMQ server {self._connection_attempts} times")

    async def close(self) -> None:
        if not self.is_connected() or self._connection.is_closed:
            return
        await self._connection.close()
