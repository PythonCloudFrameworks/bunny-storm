import sys
import asyncio

from tornado_bunny import AsyncAdapter, RabbitMQConnectionData


def calc_fib(message):
    n = int(message)
    a, b = 0, 1
    for i in range(n):
        a, b = b, a + b
    return a


async def handle_message(logger, message):
    logger.info("Fibonacci calc request {}".format(message))
    res = str(calc_fib(message.body)).encode()
    logger.info("Fibonacci calc result {}".format(res))
    return res


async def handle_test(logger, message):
    logger.info(f"Got message: {message}")
    logger.info("Test succeeded!")


if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    print("Creating Rabbit Listener")
    configuration = dict(
        receive=dict(
            incoming_1=dict(
                exchange_name="test_rpc",
                exchange_type="direct",
                routing_key="fib_calc",
                queue_name="fib_calc_q",
                durable=True,
                auto_delete=False,
                prefetch_count=1
            ),
            incoming_2=dict(
                exchange_name="test_2",
                exchange_type="direct",
                routing_key="test_2",
                queue_name="test_2",
                durable=True,
                auto_delete=False,
                prefetch_count=1
            )
        ),
        publish=dict(
            outgoing=dict(
                exchange_name="test_server",
                exchange_type="direct",
                routing_key="fib_server",
                durable=True,
                auto_delete=False,
                prefetch_count=1
            )
        )
    )
    # Using Tornado IO Loop
    loop = asyncio.get_event_loop()
    rabbit_connection_data = RabbitMQConnectionData(username="test_user",
                                                    password="pass123",
                                                    virtual_host="vhost",
                                                    connection_name="executor")
    rabbit_connection = AsyncAdapter(rabbitmq_connection_data=rabbit_connection_data,
                                     configuration=configuration,
                                     loop=loop)
    loop.create_task(rabbit_connection.receive(handler=handle_message, queue=configuration["receive"]["incoming_1"]["queue_name"]))
    loop.create_task(rabbit_connection.receive(handler=handle_test, queue=configuration["receive"]["incoming_2"]["queue_name"]))
    loop.run_forever()
