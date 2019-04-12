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

* demo.Dataverse.nl test / ask more access

* README update

"""


class DataverseExporter:
    def __init__(self):
        self.repository = "Dataverse"

    def init_export(self, irods_client, data):
        self.do_export("7151", irods_client, data['delete'], data['restrict'])

    def do_export(self, alias, irods_client, delete=False, restrict=False):
        # Metadata
        logger.info("Metadata")
        mapper = MetadataMapper(irods_client.imetadata)
        md = mapper.read_metadata()

        irods_client.update_metadata_state('exporterState', 'prepare-export', 'do-export')

        # Dataverse
        logger.info("Dataverse")
        dv = DataverseClient(os.environ['DATAVERSE_HOST'], os.environ['DATAVERSE_TOKEN'], alias, irods_client)
        dv.import_dataset(md)
        dv.import_files(delete, restrict)

        url = os.environ['DATAVERSE_HOST'] + "/dataset.xhtml?persistentId=hdl:" + irods_client.imetadata.pid + "&version=DRAFT"
        irods_client.update_metadata_state('externalLink', url, url)

        irods_client.update_metadata_state('exporterState', 'do-export', 'exported')
        time.sleep(15)
        irods_client.update_metadata_state('exporterState', 'exported', '')

        irods_client.rulemanager.rule_close()
        logger.info("Upload Done")
