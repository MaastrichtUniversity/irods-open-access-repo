import logging
import time

from utils.utils import init_logger
from utils.utils import init_parser
from utils.utils import parse_config
from dataverseManager.dataverseClient import DataverseClient
from irodsManager.irodsClient import irodsClient
from dataverseManager.dataverseMetadataMapper import MetadataMapper

logger = logging.getLogger('iRODS to Dataverse')

"""
TODO
* Parallel upload

* Archive  => uncompressed

* Error handle

* logger => tail / elk

* Wrapper rules ***

* New AVU icoll.metadata.add('externalLink', 'url') ***

* ePIC PID landing page should also point to data set's location in DataVerse ***
* ePIC PID landing page check the new AVU ***

* demo.Dataverse.nl test / ask more access

* README update

"""


def exporter(alias, ini, collection, delete=False, restrict=False):
    # Init
    print("Init")
    logger.info("Init")
    iRODS_config, dataverse_config = parse_config(ini)

    # iRODS
    print("iRODS")
    logger.info("iRODS")
    iclient = irodsClient(iRODS_config)

    logger.info(iRODS_config)
    iclient.connect()
    iclient.read_collection_metadata(collection)

    iclient.rulemanager.rule_open()

    icoll = iclient.coll
    try:
        icoll.metadata.remove('exporterState', 'in-queue-for-export')
    except:
        logger.error('exporterState: in-queue-for-export')

    try:
        icoll.metadata.add('exporterState', 'prepare-export')
    except:
        logger.error('exporterState: prepare-export')

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

    try:
        icoll.metadata.remove('exporterState', 'prepare-export')
        icoll.metadata.add('exporterState', 'do-export')
    except:
        logger.error('exporterState: export')

    dv = DataverseClient(host, token, alias, iclient)
    dv.import_dataset(md)
    dv.import_files(delete, restrict)

    url = host + "/dataset.xhtml?persistentId=hdl:" + iclient.imetadata.pid + "&version=DRAFT"
    try:
        icoll.metadata.remove('externalLink', url)
    except:
        logger.error('externalLink: '+url)

    try:
        icoll.metadata.remove('exporterState', 'do-export')
        icoll.metadata.add('exporterState', 'exported')
        icoll.metadata.add('externalLink', url)
        time.sleep(15)
        icoll.metadata.remove('exporterState', 'exported')
    except:
        logger.error('exporterState: exported')

    iclient.rulemanager.rule_close()
    logger.info("Upload Done")


def main():
    init_logger()
    args = init_parser()
    exporter(args.alias, args.ini, args.collection, args.delete, args.restrict)
    '''
    # Init
    print("Init")
    args = init_parser()
    init_logger()
    parse_config(args.ini)

    # iRODS
    print("iRODS")
    collection = args.collection
    iclient = irodsClient(iRODS_config)

    iclient.connect()
    iclient.read_collection_metadata(collection)

    iColl = iclient.session.data_objects.get(collection)

    iColl.metadata.add('exporterState', 'prepare-export', '0')

    # Metadata
    print("Metadata")
    mapper = MetadataMapper(iclient.imetadata)
    md = mapper.read_metadata()

    # Dataverse
    print("Dataverse")
    host = dataverse_config.get("host")
    token = dataverse_config.get("token")
    alias = args.dataverseAlias

    iColl.metadata.remove('exporterState', 'prepare-export', '0')
    iColl.metadata.add('exporterState', 'export', '1')

    dv = DataverseClient(host, token, alias, iclient)
    dv.import_dataset(md)
    dv.import_files(args.delete, args.restrict)

    iColl.metadata.remove('exporterState', 'export', '1')
    iColl.metadata.add('exporterState', 'exported', '2')
    '''


if __name__ == "__main__":
        main()
