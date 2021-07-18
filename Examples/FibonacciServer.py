import sys
import asyncio

from aiohttp import web
from tornado_bunny import AsyncAdapter, RabbitMQConnectionData

rabbit_adapter = None


async def handle(request):
    await rabbit_adapter.publish(body=b"Second test message", exchange="test_2")
    num = request.match_info.get("num", "10")
    rpc_result = await rabbit_adapter.rpc(body=num.encode(),
                                          receive_queue="fib_server_q",
                                          publish_exchange="test_rpc",
                                          timeout=200,
                                          ttl=200)
    text = f"The {num} Fibonacci number is {int(rpc_result)}"
    return web.Response(text=text)


async def make_fibonacci_app():
    await rabbit_adapter.publish(body=b"First second test message", exchange="test_2")
    app = web.Application()
    app.add_routes([web.get("/{num}", handle)])
    return app


if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    configuration = dict(
        publish=dict(
            outgoing_1=dict(
                exchange_name="test_rpc",
                exchange_type="direct",
                routing_key="fib_calc",
                queue_name="fib_calc_q",
                durable=True,
                auto_delete=False,
                prefetch_count=1
            ),
            outgoing_2=dict(
                exchange_name="test_2",
                exchange_type="direct",
                routing_key="test_2",
                queue_name="test_2",
                durable=True,
                auto_delete=False,
                prefetch_count=1
            )
        ),
        receive=dict(
            incoming=dict(
                exchange_name="test_server",
                exchange_type="direct",
                routing_key="fib_server",
                queue_name="fib_server_q",
                durable=True,
                auto_delete=False,
                prefetch_count=1
            )
        )
    )
    # Using AsyncIO IO Loop
    loop = asyncio.get_event_loop()
    rabbit_connection_data = RabbitMQConnectionData(username="test_user", password="pass123", virtual_host="vhost")
    rabbit_adapter = AsyncAdapter(rabbitmq_connection_data=rabbit_connection_data,
                                  configuration=configuration,
                                  loop=loop)
    web.run_app(app=make_fibonacci_app(), port=8888)
    loop.run_forever()
