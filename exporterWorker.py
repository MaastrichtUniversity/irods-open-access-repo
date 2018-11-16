#!/usr/bin/env python3
import sys
import signal

import pika
import json

import logging

from zenodoManager.irods2Zenodo import ZenodoExporter
from figshareManager.irods2Figshare import FigshareExporter
from dataverseManager.irods2Dataverse import DataverseExporter
from exporterUtils.utils import init_logger

logger = logging.getLogger('iRODS to Dataverse')

exporters = {}


def init_exporters():
    # dv = DataverseExporter()
    exporters.update({"Figshare": FigshareExporter()})
    exporters.update({"Zenodo": ZenodoExporter()})
    exporters.update({"Dataverse": DataverseExporter()})


def worker(ch, method, properties, body):
    logger.info(" [x] Received %r" % body)
    try:
        data = json.loads(body)
        ex = exporters.get(data['repository'])
        ex.init_export(data)

        ch.basic_publish(
            exchange='datahub.events_tx',
            routing_key='projectCollection.exporter.executed',
            body=json.dumps(data)
        )
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except json.decoder.JSONDecodeError:
        logger.error("json.loads" % body)
    logger.info(" [x] Sent projectCollection.exporter.executed")


def main():
    init_logger()
    init_exporters()
    params = pika.URLParameters("amqp://user:password@137.120.31.131:5672/%2f")
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    channel.queue_declare(queue='repository.Exporter', durable=True)
    channel.queue_bind(
        exchange='datahub.events_tx',
        queue='repository.Exporter',
        routing_key='projectCollection.exporter.requested'
    )
    channel.basic_consume(worker, queue='repository.Exporter')
    logger.info(' [*] Waiting for queue. To exit press CTRL+C')

    channel.start_consuming()


def sigterm_handler():
    sys.exit(0)


if __name__ == "__main__":
    # Handle the SIGTERM signal from Docker
    signal.signal(signal.SIGTERM, sigterm_handler)
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        sys.exit(0)
    finally:
        # Perform any clean up of connections on closing here
        logger.info("Exiting")
