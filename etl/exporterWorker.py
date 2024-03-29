#!/usr/bin/env python3
import os
import sys
import signal
import time
import logging

import pika
import json

from etl.dataverseManager.irods2Dataverse import DataverseExporter
from etl.irodsManager.irodsClient import irodsClient

log_level = os.environ["LOG_LEVEL"]
logging.basicConfig(level=logging.getLevelName(log_level), format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("root")


def extend_folder_path(session, selected_list):
    """
    If a folder is part of the original selected list, this function will recursively walk into it to retrieve all
    its files children and add them to the original selected list
    """
    extended_list = []
    for path in selected_list.split(",\t"):
        absolute_path = "/nlmumc/projects/" + path
        # Check if the path is collection
        if session.collections.exists(absolute_path):
            collection = session.collections.get(absolute_path)
            for coll, sub, files in collection.walk():
                for file in files:
                    extended_list.append(file.path.replace("/nlmumc/projects/", ""))
        # Or a file
        else:
            extended_list.append(path)

    return extended_list


def collection_etl(ch, method, properties, body):
    try:
        data = json.loads(body.decode("utf-8"))
        logger.info(f" [x] Received %r" % data)
    except:
        logger.error("Failed body message parsing")
    else:
        irods_client = irodsClient(
            host=os.environ["IRODS_HOST"], user=os.environ["IRODS_USER"], password=os.environ["IRODS_PASS"]
        )
        irods_client.prepare(data["project"], data["collection"], data["repository"])
        logger.info(f" [x] Create {data['repository']} exporter worker")
        exporter = None
        if data["repository"] == "Dataverse":
            exporter = DataverseExporter()
        if exporter is not None:
            data["restrict_list"] = extend_folder_path(irods_client.session, data["restrict_list"])
            exporter.init_export(irods_client, data)
            ch.basic_ack(delivery_tag=method.delivery_tag)
            logger.info(" [x] Sent projectCollection.exporter.executed")

        return True


def main(channel, retry_counter=None):
    channel.queue_declare(queue="repository.collection-etl", durable=True)

    channel.queue_bind(
        exchange="datahub.events_tx",
        queue="repository.collection-etl",
        routing_key="projectCollection.exporter.requested",
    )

    channel.basic_consume(queue="repository.collection-etl", on_message_callback=collection_etl)

    # When connection closed, try again 10 time otherwise quit.
    if retry_counter < 10:
        retry_counter += 1
    else:
        logger.error("Retry connection failed for 10 minutes. Exiting!")
        exit(1)

    try:
        logger.info(" [x] Waiting for queue repository.collection-etl")
        channel.start_consuming()
    except pika.exceptions.ConnectionClosed:
        logger.error(
            "Failed with pika.exceptions.ConnectionClosed: Sleeping for 60 secs before next try."
            + " This was try "
            + str(retry_counter)
        )
        time.sleep(60)
        new_connection = pika.BlockingConnection(parameters)
        new_ch = new_connection.channel()
        main(new_ch, retry_counter)


def sigterm_handler():
    sys.exit(0)


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, sigterm_handler)
    credentials = pika.PlainCredentials(os.environ["RABBITMQ_USER"], os.environ["RABBITMQ_PASS"])
    parameters = pika.ConnectionParameters(
        host=os.environ["RABBITMQ_HOST"],
        port=5672,
        virtual_host="/",
        credentials=credentials,
        heartbeat=600,
        blocked_connection_timeout=300,
    )
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    try:
        sys.exit(main(channel, retry_counter=0))
    finally:
        connection.close()
        logger.info("Exiting")
