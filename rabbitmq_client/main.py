import pika
import json

from minio import Minio
from minio.commonconfig import CopySource
from pika.exceptions import AMQPConnectionError

minio_client = Minio(endpoint="192.168.23.49:9000", access_key="fB7tN1WMnkAPzciqkPOG",
                     secret_key="WxzjBKitDBoBViQLpEhx4LywKyj20PKVxWtwpWJM", secure=False)


class LegalConsumer:

    def __init__(self):
        try:
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host="192.168.23.49",
                    port=5672,
                    credentials=pika.PlainCredentials(
                        "admin", "admin"
                    ),
                )
            )
            self.channel = connection.channel()
            queue_args = {'x-message-ttl': 20000}
            self.channel.queue_declare(queue="MinIO", durable=True, arguments=queue_args)
            self.channel.basic_consume(queue="MinIO", on_message_callback=self.callback)
            self.channel.start_consuming()
        except AMQPConnectionError as e:
            raise e

    def on_response(self, ch, method, props, body):
        if self.corr_id == props.correlation_id:
            self.response = body

    def callback(self, ch, method, proper, body):
        data = json.loads(body)
        print(data)
        try:
            source_exists = minio_client.bucket_exists(data['source_bucket'])
            destination_exists = minio_client.bucket_exists(data['destination_bucket'])
            # if not (source_exists and destination_exists):
            #     return {"detail": "Source or destination bucket does not exist"}
            for item in data['file_list']:
                minio_client.copy_object(
                    data['destination_bucket'],
                    item,
                    CopySource(data['source_bucket'], item)  # source bucket/object
                )
                minio_client.remove_object(data['source_bucket'], item)
            response = {
                "status_code": 1200,
            }
            self.channel.basic_publish(exchange='', routing_key=proper.reply_to,
                                       properties=pika.BasicProperties(correlation_id=proper.correlation_id),
                                       body=json.dumps(response))
            self.channel.basic_ack(delivery_tag=method.delivery_tag)

        except Exception as e:
            response = {
                "status_code": 1400
            }
            self.channel.basic_publish(exchange='', routing_key=proper.reply_to,
                                       properties=pika.BasicProperties(correlation_id=proper.correlation_id),
                                       body=json.dumps(response))
            self.channel.basic_ack(delivery_tag=method.delivery_tag)


a = LegalConsumer()
