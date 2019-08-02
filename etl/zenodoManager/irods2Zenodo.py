import logging
import sys

from zenodoManager.zenodoClient import ZenodoClient
from zenodoManager.zenodoMetadataMapper import ZenodoMetadataMapper

logger = logging.getLogger('iRODS to Dataverse')


class ZenodoExporter:

    def __init__(self):
        self.repository = "Zenodo"
        self.irods_client = None

    def init_export(self, irods_client, data):
        self.irods_client = irods_client
        try:
            self.do_export(data['token'], data['delete'], data['restrict'], data['dataexport'], data['restrict_list'])
        except:
            print("Unexpected error:", sys.exc_info()[0])
            self.irods_client.session_cleanup()
            raise

    def do_export(self, token, delete=False, restrict=False, data_export=False, restrict_list=""):
        # Metadata
        logger.info("Metadata")
        mapper = ZenodoMetadataMapper(self.irods_client.imetadata)
        md = mapper.read_metadata()

        # Zenodo
        logger.info("Zenodo")
        zn = ZenodoClient(self.irods_client, token)
        zn.create_deposit(md, data_export)
        if data_export:
            zn.import_zip_collection()

        # Cleanup
        self.irods_client.rulemanager.rule_close()
        self.irods_client.session.cleanup()
        logger.info("Upload Done")
