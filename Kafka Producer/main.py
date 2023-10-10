from kafka import KafkaProducer
import json
import requests

def send_message():
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        api_version=(0, 10, 1),
        value_serializer= lambda v: json.dumps(v).encode('utf-8')
    )
    url = "https://stream.wikimedia.org/v2/stream/recentchange"
    response = requests.get(url,stream=True)
    for line in response.iter_lines():
        try:
            event_message = line.decode('utf-8')
            lines = event_message.split("\n")
            data_line = next(line for line in lines if line.strip().startswith("data: "))
            data_str = data_line.replace("data: ", "")
            data = json.loads(data_str)
            metadata = producer.send(topic='wikimedia.recentchange', value=data)
        except Exception:
            pass
    producer.flush()
    producer.close()


if __name__ == '__main__':
    send_message()
