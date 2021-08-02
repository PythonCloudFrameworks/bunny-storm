from tornado_bunny import RabbitMQConnectionData


def test_connection_data_creation() -> None:
    # Arrange
    expected_user = "user"
    expected_pass = "pass"
    expected_host = "8.8.8.8"
    default_port = 5672
    default_vhost = "/"

    expected_uri = f"amqp://{expected_user}:{expected_pass}@{expected_host}:{default_port}/"

    # Act
    connection_data = RabbitMQConnectionData(username=expected_user, password=expected_pass, host=expected_host)

    # Assert
    assert connection_data.username == expected_user
    assert connection_data.password == expected_pass
    assert connection_data.host == expected_host
    assert connection_data.port == default_port
    assert connection_data.virtual_host == default_vhost
    assert connection_data.uri() == expected_uri
