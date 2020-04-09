import sys
import asyncio
import logging
import tornado.ioloop
import tornado.web
from tornado import gen
from RabbitAdapter import TornadoAdapter


RABBIT_URI = "amqp://test_user:pass123@192.168.56.102:5672/"


class MainHandler(tornado.web.RequestHandler):
    @gen.coroutine
    def get(self):
        self.write("Fibonacci calculator: Just pass num=<SOME-NUMBER> at URL Parameters | ")
        num = self.get_argument("num", None, True)
        self.write("Calculating Fibonacci for " + str(num))
        # Send RabbitMQ message to the Calculator Microservice and wait for result
        res = yield self.application.rabbit_connection.rpc(exchange="test_rpc", routing_key="fib_calc", body=num, timeout=200, ttl=200)
        self.write(" | Result: {}".format(int(res)))
        print("Result: {}".format(int(res)))


def make_fibonacci_app():
    return tornado.web.Application([
        (r"/", MainHandler),
    ])


if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    # Create Fibonacci web app
    app = make_fibonacci_app()
    app.listen(8888)
    io_loop = tornado.ioloop.IOLoop.instance()
    app.rabbit_connection = TornadoAdapter(rabbitmq_url=RABBIT_URI, io_loop=io_loop)
    logging.basicConfig(level=logging.INFO)
    logger = app.rabbit_connection.logger
    io_loop.start()
