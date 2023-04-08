from message_classes import Product
import pulsar
from pulsar.schema import JsonSchema

client = pulsar.Client("pulsar://localhost:6650")
producer = client.create_producer(topic="persistent://my-tenant/my-namespace/basic-topic-6", schema=JsonSchema(Product))

product = Product(
    product_id=6754,
    product_name="Aussie Berry",
    product_description="A berry from Australia"
)

producer.send(product)
