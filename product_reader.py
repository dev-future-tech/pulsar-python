from fastavro import writer, reader, parse_schema
from message_reader import schema

parsed_schema = parse_schema(schema)

with open('product.avro', 'rb') as fo:
    for record in reader(fo):
        print(record)
