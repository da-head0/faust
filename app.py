import faust
TOPIC = 'order'

app = faust.APP('order-app', broker='kafka://localhost')

# How messages are serialized
class Order(faust.Record, serializer='json'):
    order_date: str
    order_id: str

order_topic = app.topic(TOPIC, value_type=Order)

@app.agent(order_topic)
async def add_order_sum(stream):
    async for order in stream:
        # process infinite stream of orders
        print(f'Order for {order.order_id}: {order.order_date}')
