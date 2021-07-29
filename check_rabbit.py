import pika


def main():
    credentials = pika.PlainCredentials("guest", "guest")
    parameters = pika.ConnectionParameters(credentials=credentials, host="localhost", port=5672)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    print(channel)


if __name__ == '__main__':
    main()
