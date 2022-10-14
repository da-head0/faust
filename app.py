import faust
TOPIC = 'orderPayCharged'

app = faust.App('get-pocket-order', broker='0.0.0.0:9092')

# How messages are serialized
class Order(faust.Record, serializer='json'):
    sentAt: str
    version: int
    topic: str
    payload: dict

order_topic = app.topic(TOPIC, value_type=Order)

# 방법 1
@app.agent(order_topic)
async def add_order_sum(stream):
    async for order in stream:
        # process infinite stream of orders
        print(f'Order for {order.payload['orderNo']}: {order.payload.orderDate}')


# 방법 2 - 이게 좀 더 느린 듯도 하고...
@app.task
async def add_order_sum():
    async for order in order_topic.stream():
        # process infinite stream of orders
        print(f"Order for {order.payload['orderNo']}: {order.payload['orderDate']}")
        
        
# 문제 : 뭐 하나만 오류 있어도 죽음 개복치임
if __name__ == '__main__':
    app.main()
