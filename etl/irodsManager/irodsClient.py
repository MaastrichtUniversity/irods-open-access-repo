from irods.session import iRODSSession
from irods.exception import iRODSException
import xml.etree.ElementTree as ET
import logging

from irodsManager.irodsRuleManager import RuleManager

logger = logging.getLogger('iRODS to Dataverse')


class irodsClient():
    """
    iRODS client to connect to the iRODS server, to retrieve metadata and data.
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
        self.read_collection_metadata(path)
        self.rulemanager.rule_open()

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

    def read_collection_metadata(self, collection_path):
        logger.info("--\t Read collection metadata")
        self.coll = self.session.collections.get(collection_path)
        for x in self.coll.metadata.items():
            self.imetadata.__dict__.update({x.name.lower(): x.value})

        logger.info("--\t Parse collection metadata.xml")
        meta_xml = collection_path + "/metadata.xml"
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

        self.rulemanager = RuleManager(self.session, self.coll)

        self.update_metadata_state('exporterState', 'in-queue-for-export', 'prepare-export')

    def update_metadata_state(self, key, old_value, new_value):
        try:
            self.coll.metadata.remove(key, old_value)
        except iRODSException as error:
            logger.error(f"{key} : {old_value}  {error}")

        try:
            if new_value != '':
                self.coll.metadata.add(key, new_value)
        except iRODSException as error:
            logger.error(f"{key} : {new_value}  {error}")

    def remove_metadata_state(self, key, value):
        try:
            if value:
                self.coll.metadata.remove(key, value)
        except iRODSException as error:
            logger.error(f"{key} : {value}  {error}")

    def add_metadata_state(self, key, value, unit=None):
        try:
            if unit is None and value:
                self.coll.metadata.add(key, value)
            elif value:
                self.coll.metadata.add(key, value, unit)
        except iRODSException as error:
            logger.error(f"{key} : {value}  {error}")


class irodsMetadata:
    """
    Store all metadata from iRODS
    """

    def __init__(self):
        self.title = None
        self.creator = None
        self.description = None
        self.date = None
        self.pid = None

        self.byteSize = None
        self.numFiles = None

        self.tissue = None
        self.technology = None
        self.organism = None
        self.factors = None
        self.protocol = None
        self.contact = None
        self.articles = None

        self.dataset_json = None
