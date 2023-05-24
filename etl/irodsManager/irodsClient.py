from irods.exception import iRODSException
import logging

from irodsrulewrapper.rule import RuleManager

from etl.irodsManager.irodsUtils import CollectionAVU, ExporterState, submit_service_desk_ticket

logger = logging.getLogger("iRODS to Dataverse")


class irodsClient:
    """iRODS client to connect to the iRODS server, to retrieve metadata and data."""

    def __init__(self, host=None, user=None, password=None):
        self.collection_object = None
        self.project_id = None
        self.collection_id = None

        self.collection_avu = CollectionAVU()
        self.repository = None
        self.instance = None

        self.rule_manager = None
        self.session = None
        self.config = {
            "IRODS_HOST": host,
            "IRODS_USER": user,
            "IRODS_PASS": password,
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
        self.project_id = project_id
        self.collection_id = collection_id
        self.connect()
        path = f"/nlmumc/projects/{project_id}/{collection_id}"
        self.collection_object = self.session.collections.get(path)
        self.repository = repository
        self.read_collection_metadata(project_id, collection_id)

        # clear all exporterState AVU values and re-add in-queue-for-export
        # in case of remaining failed report AVUs like: upload-failed , failed-dataset-creation etc ..

        self.update_metadata_status(ExporterState.ATTRIBUTE.value, ExporterState.IN_QUEUE_FOR_EXPORT.value)

    def read_collection_metadata(self, project_id, collection_id):
        logger.info("--\t Read collection AVU")
        for x in self.collection_object.metadata.items():
            self.collection_avu.__dict__.update({x.name.lower().replace("dcat:", ""): x.value})

        logger.info("--\t Parse collection instance.json")
        instance = self.rule_manager.read_instance_from_collection(project_id, collection_id)
        self.instance = self.rule_manager.parse_general_instance(instance)

    def update_metadata_status(self, attribute, value):
        new_status = f"{self.repository}:{value}"
        self.rule_manager.set_collection_avu(self.collection_object.path, attribute, new_status)

    def set_error_status(self, attribute, value, depositor, destination, error_message):
        """
        Update the state AVU to an error status.
        Then, create a ticket to jira the service desk to report the error.

        Parameters
        ----------
        attribute: str
            The process attribute
        value: str
            The new value to set
        depositor: str
            The email of the user who started the process
        destination: str
            The dataverse upload destination
        error_message: str
            Some context on what trigger the error
        """
        try:
            self.update_metadata_status(attribute, value)
        except iRODSException as error:
            logger.error(f"{'--':<20}update_metadata_status failed: {error}")

        description = (
            f"DataVerseNL export towards {destination} failed for collection {self.collection_id}"
            f" in project {self.project_id}"
        )
        error_message = f"{value}: {error_message}"
        submit_service_desk_ticket(depositor, description, error_message)

    def remove_metadata(self, key, value):
        try:
            if value:
                self.collection_object.metadata.remove(key, value)
        except iRODSException as error:
            logger.error(f"{key} : {value}  {error}")

    def add_metadata(self, key, value, unit=None):
        try:
            if unit is None and value:
                self.collection_object.metadata.add(key, value)
            elif value:
                self.collection_object.metadata.add(key, value, unit)
        except iRODSException as error:
            logger.error(f"{key} : {value}  {error}")

    def status_cleanup(self, repository):
        logger.error("An error occurred during the upload")
        logger.error("Clean up exporterState AVU")

        # exporter client crashed, clean all exporterState AVUs
        for state in ExporterState:
            new_status = f"{repository}:{state.value}"
            self.remove_metadata(ExporterState.ATTRIBUTE.value, new_status)

        logger.error("Call rule closeProjectCollection")
        self.rule_manager.close_project_collection(self.project_id, self.collection_id)
