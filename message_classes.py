from pulsar.schema import *

class Product(Record):
    _avro_namespace = "com.pulsar.workshop"
    product_id = Integer()
    product_name = String()
    product_description = String()
