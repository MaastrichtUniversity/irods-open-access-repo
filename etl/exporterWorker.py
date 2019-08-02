#!/usr/bin/env python3
import os
import sys
import signal
import time
import logging

import pika
import json

from zenodoManager.irods2Zenodo import ZenodoExporter
from figshareManager.irods2Figshare import FigshareExporter
from dataverseManager.irods2Dataverse import DataverseExporter
from easyManager.irods2Easy import EasyExporter
from irodsManager.irodsClient import irodsClient

log_level = os.environ['LOG_LEVEL']
logging.basicConfig(level=logging.getLevelName(log_level), format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger('root')


def collection_etl(ch, method, properties, body):
    try:
        data = json.loads(body.decode("utf-8"))
        # remove user API token from logs
        log_data = data.copy()
        log_data.pop("token")
        logger.info(f" [x] Received %r" % log_data)
    except json.decoder.JSONDecodeError:
        logger.error("json.loads %r" % body.decode("utf-8").replace("\"", "", 3))
    else:
        path = "/nlmumc/projects/" + data['project'] + "/" + data['collection']
        irods_client = irodsClient(host=os.environ['IRODS_HOST'], port=1247, user=os.environ['IRODS_USER'],
                                   password=os.environ['IRODS_PASS'], zone='nlmumc')
        irods_client.prepare(path)
        logger.info(f" [x] Create {data['repository']} exporter worker")
        class_name = data['repository'] + 'Exporter'
        exporter = globals()[class_name]()
        exporter.init_export(irods_client, data)
        ch.basic_ack(delivery_tag=method.delivery_tag)
        logger.info(" [x] Sent projectCollection.exporter.executed")

        return True


def main(channel, retry_counter=None):
    channel.queue_declare(queue='repository.collection-etl', durable=True)

    channel.queue_bind(
        exchange='datahub.events_tx',
        queue='repository.collection-etl',
        routing_key='projectCollection.exporter.requested'
    )

    channel.basic_consume(
        collection_etl,
        queue='repository.collection-etl',
    )

    # When connection closed, try again 10 time otherwise quit.
    if retry_counter < 10:
        retry_counter += 1
    else:
        logger.error("Retry connection failed for 10 minutes. Exiting!")
        exit(1)

    try:
        logger.info(' [x] Waiting for queue repository.collection-etl')
        channel.start_consuming()
    except pika.exceptions.ConnectionClosed:
        logger.error(
            "Failed with pika.exceptions.ConnectionClosed: Sleeping for 60 secs before next try."
            + " This was try " + str(retry_counter))
        time.sleep(60)
        new_connection = pika.BlockingConnection(parameters)
        new_ch = new_connection.channel()
        main(new_ch, retry_counter)


def sigterm_handler():
    sys.exit(0)


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, sigterm_handler)
    credentials = pika.PlainCredentials(os.environ['RABBITMQ_USER'], os.environ['RABBITMQ_PASS'])
    parameters = pika.ConnectionParameters(host=os.environ['RABBITMQ_HOST'],
                                           port=5672,
                                           virtual_host='/',
                                           credentials=credentials,
                                           heartbeat_interval=600,
                                           blocked_connection_timeout=300)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    try:
        sys.exit(main(channel, retry_counter=0))
    finally:
        connection.close()
        logger.info("Exiting")
