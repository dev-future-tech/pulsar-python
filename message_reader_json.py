import pulsar
from pulsar.schema import JsonSchema
from message_classes import Product
import traceback



client = pulsar.Client("pulsar://localhost:6650")
consumer = client.subscribe(topic="persistent://my-tenant/my-namespace/basic-topic-6",
                            subscription_name="json_reader",
                            schema=JsonSchema(Product))

while True:
    msg = consumer.receive()
    ex = msg.value()

    try:
        consumer.acknowledge(msg)
        print("Received message '{}'".format(ex))
        print("Product name is {}, {}".format(ex.product_name, ex.product_description))
    except Exception as e:
        print(e)
        traceback.print_exc()
