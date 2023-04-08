import pulsar


client = pulsar.Client('pulsar://localhost:6650')
producer = client.create_producer("persistent://my-tenant/my-namespace/basic-topic-5")

animal = {
    "genus" : "felinus",
    "species" : "domesticus"
}

try:
    producer.send(animal)
except Exception as e:
    print('Error sending animal')
    print(e)

