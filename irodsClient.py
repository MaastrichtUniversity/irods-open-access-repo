from irods.session import iRODSSession
import xml.etree.ElementTree as ET
import logging

from irodsMetadata import irodsMetadata

logger = logging.getLogger('iRODS to Dataverse')

class irodsClient():

    def __init__(self, host='', port='', user='', password='', zone=''):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.zone = zone
        self.session = None
        self.coll = None
        self.imetadata = irodsMetadata()

    def connect(self, host, port, user, password, zone):
        print("Connect to iRODS")
        logger.info("Connect to iRODS")
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.zone = zone
        self.session = iRODSSession(host=host,
                                    port=port,
                                    user=user,
                                    password=password,
                                    zone=zone)

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

    def read_collection(self, collection_fullpath):
        print("Read collection metadata")
        logger.info("Read collection metadata")
        self.coll = self.session.collections.get(collection_fullpath)
        for x in self.coll.metadata.items():
            self.imetadata.__dict__.update({x.name.lower(): x.value})

        print("Parse collection metadata.xml")
        logger.info("Parse collection metadata")
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

    def write(self, files_path):
        print("Copy local collection replica")
        logger.info("Copy local collection replica")
        for data in self.coll.data_objects:
            # physical_path = session.data_objects.get(data.path).replicas[0].path
            buff = self.session.data_objects.open(data.path, 'r').read()
            with open(files_path + data.name, 'wb') as f:
                f.write(buff)
