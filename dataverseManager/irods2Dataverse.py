import logging
import time

# import exporterUtils.init_logger
from exporterUtils.utils import init_parser, init_logger, parse_config
# from exporterUtils.utils import parse_config
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

* demo.Dataverse.nl test / ask more access

* README update

"""
# irods2Dataverse.exporter("DataHub", "resources/config.ini", path, data['delete'], data['restrict'])


class DataverseExporter:

    def __init__(self):
        self.repository = "Dataverse"

    def init_export(self, data):
        path = "/nlmumc/projects/" + data['project'] + "/" + data['collection']
        ini = "resources/config.ini"
        self.do_export("DataHub", ini, path, data['delete'], data['restrict'])

    def do_export(self, alias, ini, collection, delete=False, restrict=False):
        # Init
        logger.info("Init")
        irods_config, dataverse_config = parse_config(ini)

        # iRODS
        logger.info("iRODS")
        iclient = irodsClient(irods_config)
        logger.info(irods_config)
        iclient.connect()
        iclient.read_collection_metadata(collection)

        iclient.rulemanager.rule_open()
        iclient.update_metadata_state('exporterState', 'in-queue-for-export', 'prepare-export')

        # Metadata
        logger.info("Metadata")
        mapper = MetadataMapper(iclient.imetadata)
        md = mapper.read_metadata()

        iclient.update_metadata_state('exporterState', 'prepare-export', 'do-export')
        # Dataverse
        logger.info("Dataverse")
        host = dataverse_config.get("host")
        token = dataverse_config.get("token")

        dv = DataverseClient(host, token, alias, iclient)
        dv.import_dataset(md)
        dv.import_files(delete, restrict)

        url = host + "/dataset.xhtml?persistentId=hdl:" + iclient.imetadata.pid + "&version=DRAFT"
        iclient.update_metadata_state('externalLink', url, url)

        iclient.update_metadata_state('exporterState', 'do-export', 'exported')
        time.sleep(15)
        iclient.update_metadata_state('exporterState', 'exported', '')

        iclient.rulemanager.rule_close()
        logger.info("Upload Done")


def main():
    init_logger()
    args = init_parser()
    dv = DataverseExporter()
    dv.do_export(args.alias, args.ini, args.collection, args.delete, args.restrict)
    '''
    # Init
    logger.info("Init")
    args = init_parser()
    init_logger()
    parse_config(args.ini)

    # iRODS
    logger.info("iRODS")
    collection = args.collection
    iclient = irodsClient(iRODS_config)

    iclient.connect()
    iclient.read_collection_metadata(collection)

    iColl = iclient.session.data_objects.get(collection)

    iColl.metadata.add('exporterState', 'prepare-export', '0')

    # Metadata
    logger.info("Metadata")
    mapper = MetadataMapper(iclient.imetadata)
    md = mapper.read_metadata()

    # Dataverse
    logger.info("Dataverse")
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
