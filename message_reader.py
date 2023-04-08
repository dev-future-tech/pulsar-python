from fastavro import writer, reader
from fastavro.schema import load_schema

parsed_schema = load_schema('product.avsc')

records = [
    {u'product_id': 123, u'product_name': u'blueberry', u'product_description': u'A blue berry' }
]

with open('weather.avro', 'wb') as out:
    writer(out, parsed_schema, records)

with open('weather.avro', 'rb') as fo:
    for record in reader(fo):
        print(record)