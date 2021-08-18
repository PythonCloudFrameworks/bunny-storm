import asyncio
from logging import Logger
from typing import Union

from aio_pika import connect_robust, RobustConnection

from . import RabbitMQConnectionData


class AsyncConnection:
    """
    Responsible for the management of a connection to a RabbitMQ server
    """
    _logger: Logger
    _loop: asyncio.AbstractEventLoop

    _connection: Union[RobustConnection, None]
    _rabbitmq_url: RabbitMQConnectionData
    _properties: dict

    _timeout: int
    _connection_attempts: int
    _attempt_backoff: int

    _connection_lock: asyncio.Lock

    def __init__(self, rabbitmq_connection_data: RabbitMQConnectionData, logger: Logger,
                 loop: asyncio.AbstractEventLoop = None, properties: dict = None,  timeout: Union[int, float] = 10,
                 connection_attempts: int = 5, attempt_backoff: int = 5):
        """
        :param rabbitmq_connection_data: RabbitMQConnectionData instance containing desired connection credentials
        :param logger: Logger
        :param loop: Event loop to use. Defaults to current event loop if None is passed.
        :param properties: Connection properties
        :param timeout: Connection timeout
        :param connection_attempts: Connection retry attempts
        :param attempt_backoff: Time waited between connection attempts
        """
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
        """
        :return: self._logger
        """
        return self._logger

    async def get_connection(self) -> RobustConnection:
        """
        Gets or creates a connection to a RabbitMQ server.
        If we already have an open connection, returns it, otherwise opens a new connection.
        Avoids race conditions using a Lock.
        If creating a new connection fails, stops the event loop and raises an exception.
        :return: Robust RabbitMQ connection
        """
        async with self._connection_lock:
            if not self.is_connected():
                try:
                    self._connection = await self._connect()
                except ConnectionError:
                    self.logger.error("Failed to connect to RabbitMQ, stopping loop.")
                    self._loop.stop()
                    raise
        return self._connection

    def is_connected(self) -> bool:
        """
        :return: Whether or not there is currently an open connection stored in this object
        """
        return not ((self._connection is None) or (self._connection.connection is None) or self._connection.is_closed)

    async def _connect(self) -> RobustConnection:
        """
        Attempts to create a robust connection to a RabbitMQ server.
        Contains a retry mechanism which attempts to connect a configurable amount of times, with a configurable timeout
        and configurable backoff between each connection attempt.
        :return: Created robust connection
        :raise: ConnectionError in case connecting fails
        """
        for attempt_num in range(1, self._connection_attempts + 1):
            uri = self._rabbitmq_connection_data.uri()
            self.logger.info(f"Connecting to RabbitMQ, URI: {uri}, Attempt: {attempt_num}")
            try:
                connection = await connect_robust(url=uri,
                                                  loop=self._loop,
                                                  timeout=self._timeout,
                                                  client_properties=self._properties)
                return connection
            except (asyncio.TimeoutError, ConnectionError):
                self.logger.error(f"Connection attempt {attempt_num} / {self._connection_attempts} failed")
                if attempt_num < self._connection_attempts:
                    self.logger.debug(f"Going to sleep for {self._attempt_backoff} seconds")
                    await asyncio.sleep(self._attempt_backoff)
            except BaseException:
                raise ConnectionError("Unexpected exception during connection attempt")
        raise ConnectionError(f"Failed to connect to RabbitMQ server {self._connection_attempts} times")

    async def close(self) -> None:
        """
        Closes the connection stored in this object
        """
        if not self.is_connected():
            return
        await self._connection.close()
