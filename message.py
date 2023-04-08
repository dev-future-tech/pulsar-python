import pulsar
from pulsar.schema import *
from fastavro.schema import load_schema
import traceback

schema_definition = load_schema("product.avsc")

client = pulsar.Client('pulsar://localhost:6650')

consumer = client.subscribe(topic='persistent://my-tenant/my-namespace/basic-topic-5',
                            subscription_name='python-listener',
                            schema=AvroSchema(None, schema_definition=schema_definition))

while True:
    msg = consumer.receive()
    ex = msg.value()
    try:
        print("Received message '{}'".format(ex))
        print("Product name is {}".format(ex["product_name"]))
        consumer.acknowledge(msg)
    except Exception as e:
        consumer.negative_acknowledge(msg)
        print(e)
        traceback.print_exc()

client.close()
