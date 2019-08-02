from irods.session import iRODSSession
from irods.exception import iRODSException
from irods.meta import iRODSMeta
import xml.etree.ElementTree as ET
import logging

from irodsManager.irodsRuleManager import RuleManager
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

    def connect(self):
        logger.info("--\t Connect to iRODS")
        self.session = iRODSSession(host=self.host,
                                    port=self.port,
                                    user=self.user,
                                    password=self.password,
                                    zone=self.zone)

    def prepare(self, path):
        logger.info("iRODS")
        self.connect()
        self.coll = self.session.collections.get(path)
        self.read_collection_metadata()
        self.rulemanager = RuleManager(self.session, self.coll)
        self.rulemanager.rule_open()
        # clear all exporterState AVU values and re-add in-queue-for-export
        # in case of remaining failed report AVUs like: upload-failed , failed-dataset-creation etc ..
        self.coll.metadata['exporterState'] = iRODSMeta('exporterState', ExporterState.IN_QUEUE_FOR_EXPORT.value)
        self.update_metadata_state(ExporterState.IN_QUEUE_FOR_EXPORT.value, ExporterState.CREATE_EXPORTER.value)

    @staticmethod
    def read_tag(root, tag):
        if root.find(tag).text is not None:
            tag_id = root.find(tag).get("id").split(":", 1)
            return {"vocabulary": tag_id[0], "uri": tag_id[1].strip("class:"), "name": root.find(tag).text}
        else:
            return []

    @staticmethod
    def read_tag_list(root, tag):
        tag_list = []
        for k in root.findall(tag):
            for i in k.iter():
                if i.text is not None:
                    tag_list.append(i.text)
        return tag_list

    @staticmethod
    def read_tag_node(root, tag):
        node_list = []
        for i in root.iterfind(tag):
            for k in i:
                if k.text is not None:
                    node_list.append(k.text)
        return node_list

    @staticmethod
    def read_tag_node_dict(root, tag):
        node_dict = {}
        for i in root.iterfind(tag):
            for k in i:
                if k.text is not None:
                    node_dict.update({k.tag: k.text})
        return [node_dict]

    def read_collection_metadata(self):
        logger.info("--\t Read collection metadata")
        for x in self.coll.metadata.items():
            self.imetadata.__dict__.update({x.name.lower(): x.value})

        logger.info("--\t Parse collection metadata.xml")
        meta_xml = self.coll.path + "/metadata.xml"
        buff = self.session.data_objects.open(meta_xml, 'r')
        root = ET.fromstring(buff.read())
        self.imetadata.date = root.find("date").text

        # optional
        self.imetadata.description = root.find("description").text

        self.imetadata.tissue = self.read_tag(root, "tissue")
        self.imetadata.technology = self.read_tag(root, "technology")
        self.imetadata.organism = self.read_tag(root, "organism")

        self.imetadata.factors = self.read_tag_node(root, "factors")
        self.imetadata.protocol = self.read_tag_node_dict(root, "protocol")
        self.imetadata.contact = self.read_tag_node_dict(root, "contact")

        self.imetadata.articles = self.read_tag_list(root, "article")

    def update_metadata_state(self, old_value, new_value, key=ExporterState.ATTRIBUTE.value):
        try:
            if old_value != '' and new_value != '':
                self.coll.metadata.remove(key, old_value)
        except iRODSException as error:
            logger.error(f"{key} : {old_value}  {error}")

        try:
            if new_value != '':
                self.coll.metadata.add(key, new_value)
        except iRODSException as error:
            logger.error(f"{key} : {new_value}  {error}")

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

    def session_cleanup(self):
        logger.error("An error occurred during the upload")
        logger.error("Clean up exporterState AVU")

        # exporter client crashed, clean all exporterState AVUs
        for state in ExporterState:
            self.remove_metadata('exporterState', state.value)

        logger.error("Call rule closeProjectCollection")
        self.rulemanager.rule_close()


