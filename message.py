import pulsar
from pulsar.schema import *


class Product(Record):
    _avro_namespace = 'pulsar.workshop'
    product_id = Integer()
    product_name = String()
    product_description = String()


client = pulsar.Client('pulsar://localhost:6650')

consumer = client.subscribe(topic='persistent://my-tenant/my-namespace/basic-topic-5',
                            subscription_name='python-listener',
                            schema=AvroSchema(Product))

while True:
    msg = consumer.receive()
    try:
        print("Received message '{}' id='{}'".format(msg.data.product_name, msg.message_id))
        consumer.acknowledge(msg)
    except Exception:
        consumer.negative_acknowledge(msg)

client.close()
