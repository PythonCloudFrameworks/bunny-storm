from dataclasses import dataclass


@dataclass
class RabbitMQConnectionData:
    username: str = "guest"
    password: str = "guest"
    host: str = "localhost"
    port: int = 5672
    virtual_host: str = "/"
    connection_name: str = ""

    def uri(self) -> str:
        vhost = "" if self.virtual_host == "/" else self.virtual_host
        name_query = f"?name={self.connection_name}" if self.connection_name else ""
        return f"amqp://{self.username}:{self.password}@{self.host}:{self.port}/{vhost}{name_query}"
