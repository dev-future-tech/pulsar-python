from fastavro.schema import load_schema
from pulsar.schema import *
import pulsar

schema_definition = load_schema("product.avsc")

avro_schema = AvroSchema(None, schema_definition=schema_definition)
client = pulsar.Client('pulsar://localhost:6650')
producer = client.create_producer(topic="persistent://my-tenant/my-namespace/basic-topic-5", schema=avro_schema)

product = {
    "product_id": 54321,
    "product_name": "snake berry",
    "product_description": "A berry provided by Python"
}

producer.send(product)
