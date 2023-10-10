from kafka import KafkaConsumer
import json
import pymongo

def consume_message():
    mongo_url ="mongodb://admin:password@localhost:27017/?authMechanism=DEFAULT"

    myclient = pymongo.MongoClient(mongo_url)
    mydb = myclient['kafka-data']
    mycol = mydb['wikimedia-recentchange']

    consumer = KafkaConsumer(
        'wikimedia.recentchange',
        bootstrap_servers=['localhost:9092'],
        api_version=(0, 10, 1),
        group_id = 'python-wikimedia',
        auto_offset_reset = 'latest',
        enable_auto_commit = True,
        auto_commit_interval_ms = 10000
    )

    for message in consumer:
        meta_data = message
        (topic, partition, offset, timestamp, timestamp_type,
         key, value, headers, checksum, serialized_key_size,
         serialized_value_size, serialized_header_size) = meta_data
        json_data = json.loads(value.decode('utf-8'))
        x = mycol.insert_one(json_data)
        consumer.commit()

if __name__ == '__main__':
    consume_message()
