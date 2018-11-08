import sys
import time

import pika
import json

import logging
from utils.utils import init_logger
from irodsManager.irods2Dataverse import exporter

# Credentials
credentials = pika.PlainCredentials('user', 'password')

logger = logging.getLogger('iRODS to Dataverse')


def fake_pid(ch, method, properties, body):
    print(" [x] Received %r" % body)
    logger.info(" [x] Received %r" % body)
    time.sleep(5)
    try:
        data = json.loads(body)
        path = "/nlmumc/projects/" + data['project'] + "/" + data['collection']

        exporter("DataHub", "resources/config.ini", path, data['delete'], data['restrict'])

        ch.basic_publish(
            exchange='datahub.events_tx',
            routing_key='projectCollection.exporter.executed',
            body=json.dumps(data)
        )
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except json.decoder.JSONDecodeError:
        logger.error("json.loads" % body)

    print(" [x] Sent projectCollection.exporter.executed")
    logger.info(" [x] Sent projectCollection.exporter.executed")


def main():
    init_logger()
    # connection = pika.BlockingConnection(pika.ConnectionParameters('http://137.120.31.131', 15672, '/', credentials))
    params = pika.URLParameters("amqp://user:password@137.120.31.131:5672/%2f")
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    q = channel.queue_declare(queue='repository.Exporter', durable=True)
    channel.queue_bind(
        exchange='datahub.events_tx',
        queue='repository.Exporter',
        routing_key='projectCollection.exporter.requested'
    )
    channel.basic_consume(fake_pid, queue='repository.Exporter')
    print(' [*] Waiting for queue. To exit press CTRL+C')
    logger.info(' [*] Waiting for queue. To exit press CTRL+C')

    channel.start_consuming()


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        sys.exit(0)
