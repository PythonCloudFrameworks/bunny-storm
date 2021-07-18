from dataclasses import dataclass


@dataclass
class RabbitMQConnectionData:
    username: str = "guest"
    password: str = "guest"
    host: str = "localhost"
    port: int = 5672
    virtual_host: str = "/"

    def uri(self) -> str:
        vhost = "" if self.virtual_host == "/" else self.virtual_host
        return f"amqp://{self.username}:{self.password}@{self.host}:{self.port}/{vhost}"
