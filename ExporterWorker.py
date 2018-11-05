import sys
import time

import pika
import json
from dataverseClient import DataverseClient
from irodsClient import irodsClient
from metadataMapper import MetadataMapper
import logging
import configparser

# Credentials
credentials = pika.PlainCredentials('user', 'password')

iRODS_config = {}
dataverse_config = {}
logger = logging.getLogger('iRODS to Dataverse')


def init_logger():
    """
    Initiate logging handler and formatter
    Level DEBUG
    """
    logger.setLevel(logging.DEBUG)
    fh = logging.FileHandler('info.log')
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    logger.addHandler(fh)


def parse_config(ini):
    """
    Parse the configuration file with iRODS & Dataverse credentials

    :param ini: Path to the configuration file *.ini
    """
    print("Read config file")
    logger.info("Read config file")
    config = configparser.ConfigParser()
    config.read(ini)

    # iRODS config init
    iRODS_config.update({"host": config['iRODS']['host']})
    iRODS_config.update({"port": config['iRODS']['port']})
    iRODS_config.update({"user": config['iRODS']['user']})
    iRODS_config.update({"password": config['iRODS']['password']})
    iRODS_config.update({"zone": config['iRODS']['zone']})

    # Dataverse config init
    dataverse_config.update({"host": config['Dataverse']['host']})
    dataverse_config.update({"token": config['Dataverse']['token']})


def exporter(alias, ini, collection, delete=False, restrict=False):
    # Init
    print("Init")
    logger.info("Init")
    parse_config(ini)

    # iRODS
    print("iRODS")
    logger.info("iRODS")
    iclient = irodsClient(iRODS_config)

    logger.info(iRODS_config)
    iclient.connect()
    iclient.read_collection_metadata(collection)

    iColl = iclient.coll
    iColl.metadata.add('exporterState', 'prepare-export', '0')

    # Metadata
    print("Metadata")
    logger.info("Metadata")
    mapper = MetadataMapper(iclient.imetadata)
    md = mapper.read_metadata()

    # Dataverse
    print("Dataverse")
    logger.info("Dataverse")
    host = dataverse_config.get("host")
    token = dataverse_config.get("token")

    iColl.metadata.remove('exporterState', 'prepare-export', '0')
    try:
        iColl.metadata.add('exporterState', 'export', '1')
    except:
        logger.error('export')

    dv = DataverseClient(host, token, alias, iclient)
    dv.import_dataset(md)
    dv.import_files(delete, restrict)

    iColl.metadata.remove('exporterState', 'export', '1')
    try:
        iColl.metadata.add('exporterState', 'exported', '2')
        time.sleep(60)
        iColl.metadata.remove('exporterState', 'exported', '2')
    except:
        logger.error('exporterState')
    iclient.rulemanager.rule_close()
    logger.info("Upload Done")


def fake_pid(ch, method, properties, body):
    print(" [x] Received %r" % body)
    logger.info(" [x] Received %r" % body)
    time.sleep(5)
    try:
        data = json.loads(body)
    except json.decoder.JSONDecodeError:
        data = {"project": "P000000003", "collection": "C000000009", "dataverseAlias": "DataHub"}
    # data['response'] = 201
    # data['pid'] = 'http://hdl.handle.net/21.T12996/%s%s' % (data['project'], data['collection'])

    path = "/nlmumc/projects/"+data['project']+"/"+data['collection']

    exporter("DataHub", "config.ini", path)

    ch.basic_publish(
        exchange='datahub.events_tx',
        routing_key='projectCollection.exporter.executed',
        body=json.dumps(data)
    )
    ch.basic_ack(delivery_tag=method.delivery_tag)
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
