import logging
import os
import time

from dataverseManager.dataverseClient import DataverseClient
from dataverseManager.dataverseMetadataMapper import MetadataMapper

logger = logging.getLogger('iRODS to Dataverse')

"""
TODO
* Parallel upload

* Archive  => uncompressed

* Error handle

* logger => tail / elk

* README update

"""


class DataverseExporter:
    def __init__(self):
        self.repository = "Dataverse"
        self.irods_client = None
        self.metadata_mapper = None
        self.exporter_client = None

    def init_export(self, irods_client, data):
        self.irods_client = irods_client
        try:
            self.do_export("7151", data['delete'], data['restrict'], data['dataexport'], data['restrict_list'])
        finally:
            # self.session_cleanup()
            pass

    def do_export(self, alias, delete=False, restrict=False, data_export=False, restrict_list=""):
        # Metadata
        logger.info("Metadata")
        self.metadata_mapper = MetadataMapper(self.irods_client.imetadata)
        md = self.metadata_mapper.read_metadata()

        # Dataverse
        logger.info("Dataverse")
        self.exporter_client = DataverseClient(os.environ['DATAVERSE_HOST'], os.environ['DATAVERSE_TOKEN'], alias, self.irods_client)
        self.exporter_client.create_dataset(md, data_export)
        if data_export:
            self.exporter_client.import_files(delete, restrict, restrict_list)

        # Cleanup
        self.irods_client.rulemanager.rule_close()
        self.irods_client.session.cleanup()

    def session_cleanup(self):
        logger.error("An error occurred during the upload")
        logger.error("Clean up exporterState AVU")

        self.irods_client.remove_metadata_state('exporterState', 'in-queue-for-export')
        self.irods_client.remove_metadata_state('exporterState', 'prepare-export')
        self.irods_client.remove_metadata_state('exporterState', 'do-export')
        self.irods_client.remove_metadata_state('exporterState', self.exporter_client.last_export)
        logger.error("exporterState: " + self.exporter_client.last_export)

        logger.error("Call rule closeProjectCollection")
        self.irods_client.rulemanager.rule_close()

