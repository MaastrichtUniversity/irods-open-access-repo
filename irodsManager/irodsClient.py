from irods.session import iRODSSession
import xml.etree.ElementTree as ET
import logging

from irodsManager.irodsRuleManager import RuleManager
from irodsManager.irodsMetadata import irodsMetadata

logger = logging.getLogger('iRODS to Dataverse')


class irodsClient():

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

    def connect(self, host=None, port=None, user=None, password=None, zone=None):
        logger.info("Connect to iRODS")

        if host and port and user and password and zone is None:
            self.host = host
            self.port = port
            self.user = user
            self.password = password
            self.zone = zone

        self.session = iRODSSession(host=self.host,
                                    port=self.port,
                                    user=self.user,
                                    password=self.password,
                                    zone=self.zone)

    def read_tag(self, root, tag):
        if root.find(tag).text is not None:
            tag_id = root.find(tag).get("id").split(":", 1)
            return {"vocabulary": tag_id[0], "uri": tag_id[1], "name": root.find(tag).text}
        else:
            return []

    def read_tag_list(self, root, tag):
        tag_list = []
        for k in root.findall(tag):
            for i in k.iter():
                if i.text is not None:
                    tag_list.append(i.text)
        return tag_list

    def read_tag_node(self, root, tag):
        node_list = []
        for i in root.iterfind(tag):
            for k in i:
                if k.text is not None:
                    node_list.append(k.text)
        return node_list

    def read_tag_node_dict(self, root, tag):
        node_dict = {}
        for i in root.iterfind(tag):
            for k in i:
                if k.text is not None:
                    node_dict.update({k.tag: k.text})
        return [node_dict]

    def read_collection_metadata(self, collection_fullpath):
        logger.info("Read collection metadata")
        self.coll = self.session.collections.get(collection_fullpath)
        for x in self.coll.metadata.items():
            self.imetadata.__dict__.update({x.name.lower(): x.value})

        logger.info("Parse collection metadata.xml")
        meta_xml = collection_fullpath + "/metadata.xml"
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

        self.rulemanager = RuleManager(collection_fullpath, self.session, self.coll)

    def update_metadata_state(self, key, old_value, new_value):
        try:
            self.coll.metadata.remove(key, old_value)
        except:
            logger.error(key + ': ' + old_value)

        try:
            if new_value != '':
                self.coll.metadata.add(key, new_value)
        except:
            logger.error(key + ': ' + new_value)
        pass
