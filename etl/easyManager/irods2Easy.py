import logging
import os
import sys

from easyManager.easyClient import EasyClient
logger = logging.getLogger('iRODS to Dataverse')


class EasyExporter:
    def __init__(self):
        self.repository = "Easy"
        self.irods_client = None
        self.metadata_mapper = None
        self.exporter_client = None

    def init_export(self, irods_client, data):
        self.irods_client = irods_client
        try:
            self.do_export()
        except:
            logger.error("Unexpected error:", sys.exc_info()[0])
            self.session_cleanup()
            raise

    def do_export(self):
        # Easy
        logger.info("Easy")
        self.exporter_client = EasyClient(os.environ['EASY_HOST'], os.environ['EASY_USER'],
                                          os.environ['EASY_PWD'], self.irods_client)
        self.exporter_client.post_it()
        self.exporter_client.final_report()

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
