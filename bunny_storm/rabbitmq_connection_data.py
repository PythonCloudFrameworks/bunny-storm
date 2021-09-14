from dataclasses import dataclass


@dataclass
class RabbitMQConnectionData:
    """
    Dataclass responsible for organizing RabbitMQ connection credentials and parameters.
    Allows to name connections using connection_name.
    Use uri() to get the connection URI which corresponds to the credentials and parameters given.
    """
    username: str = "guest"
    password: str = "guest"
    host: str = "localhost"
    port: int = 5672
    virtual_host: str = "/"
    connection_name: str = ""

    def uri(self) -> str:
        """
        Creates connection URI for a RabbitMQ server with the given connection credentials.
        :return: Connection URI
        """
        vhost = "" if self.virtual_host == "/" else self.virtual_host
        name_query = f"?name={self.connection_name}" if self.connection_name else ""
        return f"amqp://{self.username}:{self.password}@{self.host}:{self.port}/{vhost}{name_query}"
