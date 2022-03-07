from irods.exception import iRODSException
import logging

from irodsrulewrapper.rule import RuleManager

from irodsManager.irodsUtils import irodsMetadata, ExporterState

logger = logging.getLogger('iRODS to Dataverse')


class irodsClient:
    """iRODS client to connect to the iRODS server, to retrieve metadata and data.
    """

    def __init__(self, host=None, port=None, user=None, password=None, zone=None):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.zone = zone

        self.session = None
        self.coll = None
        self.imetadata = irodsMetadata()
        self.rulemanager = None
        self.repository = None

        self.rule_manager = None
        self.instance = None
        self.config = {
            "IRODS_HOST": self.host,
            "IRODS_USER": self.user,
            "IRODS_PASS": self.password,
            "IRODS_CLIENT_SERVER_POLICY": "CS_NEG_REQUIRE",
        }

    def __del__(self):
        # iRODSSession's reference count is always > 0 due to circular
        # references.  Therefore, losing all local references to it doesn't
        # imply that its  __del__() method and subsequent .cleanup() will be
        # called.
        if self.session:
            self.session.cleanup()

    def connect(self):
        logger.info("--\t Connect to iRODS")
        self.rule_manager = RuleManager(admin_mode=True, config=self.config)
        self.session = self.rule_manager.session

    def prepare(self, project_id, collection_id, repository):
        self.connect()
        path = f"/nlmumc/projects/{project_id}/{collection_id}"
        self.coll = self.session.collections.get(path)
        self.repository = repository
        self.read_collection_metadata(project_id, collection_id)

        # clear all exporterState AVU values and re-add in-queue-for-export
        # in case of remaining failed report AVUs like: upload-failed , failed-dataset-creation etc ..
        new_status = f"{repository}:{ExporterState.IN_QUEUE_FOR_EXPORT.value}"
        self.update_metadata_status("exporterState", new_status)

    def read_collection_metadata(self, project_id, collection_id):
        logger.info("--\t Read collection AVU")
        for x in self.coll.metadata.items():
            self.imetadata.__dict__.update({x.name.lower().replace('dcat:', ''): x.value})

        logger.info("--\t Parse collection instance.json")
        instance = self.rule_manager.read_instance_from_collection(project_id, collection_id)
        self.instance = self.rule_manager.parse_general_instance(instance)

    def update_metadata_status(self, attribute, value):
        self.rule_manager.set_collection_avu(self.coll.path, attribute, value)

    def remove_metadata(self, key, value):
        try:
            if value:
                self.coll.metadata.remove(key, value)
        except iRODSException as error:
            logger.error(f"{key} : {value}  {error}")

    def add_metadata(self, key, value, unit=None):
        try:
            if unit is None and value:
                self.coll.metadata.add(key, value)
            elif value:
                self.coll.metadata.add(key, value, unit)
        except iRODSException as error:
            logger.error(f"{key} : {value}  {error}")

    def status_cleanup(self, repository):
        logger.error("An error occurred during the upload")
        logger.error("Clean up exporterState AVU")

        # exporter client crashed, clean all exporterState AVUs
        for state in ExporterState:
            new_status = f"{repository}:{state.value}"
            self.remove_metadata('exporterState', new_status)

        logger.error("Call rule closeProjectCollection")
        self.rulemanager.rule_close()


