import sys
import asyncio
import tornado.ioloop
from tornado_bunny import TornadoAdapter

RABBIT_URI = "amqp://test_user:pass123@192.168.56.102:5672/"


def calc_fib(message):
    n = int(message)
    a, b = 0, 1
    for i in range(n):
        a, b = b, a + b
    return a


def handle_message(message):
    # return calc_fib(message)
    print("Fibonacci calc request {}".format(message))
    res = calc_fib(message)
    print("Fibonacci calc result {}".format(res))
    f = asyncio.Future()
    f.set_result(res)
    return f


if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    # Create Rabbit listener
    print("Creating Rabbit Listener")
    io_loop = tornado.ioloop.IOLoop.instance()
    rabbit_connection = TornadoAdapter(rabbitmq_url=RABBIT_URI, io_loop=io_loop)
    rabbit_connection.receive(exchange="test_rpc", routing_key="fib_calc", queue_name="fib_calc_q", handler=handle_message)
    io_loop.start()

