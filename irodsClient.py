from irods.session import iRODSSession
import xml.etree.ElementTree as ET
import logging

logger = logging.getLogger('iRODS to Dataverse')

iRODS_config = {}
# meta_dict = {}


class irodsClient():

    def __init__(self, host='', port='', user='', password='', zone=''):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.zone = zone
        self.session = None
        self.coll = None

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

    def read_collection(self, collection_fullpath):
        meta_dict = {}
        print("Read collection metadata")
        logger.info("Read collection metadata")
        self.coll = self.session.collections.get(collection_fullpath)
        for x in self.coll.metadata.items():
            meta_dict.update({x.name: x.value})

        print("Parse collection metadata.xml")
        logger.info("Parse collection metadata")
        meta_xml = collection_fullpath + "/metadata.xml"
        buff = self.session.data_objects.open(meta_xml, 'r')
        e = ET.fromstring(buff.read())
        meta_dict.update({"description": e.find("description").text})
        meta_dict.update({"date": e.find("date").text})

        return meta_dict

    def write(self, files_path):

        print("Copy local collection replica")
        logger.info("Copy local collection replica")
        for data in self.coll.data_objects:
            # physical_path = session.data_objects.get(data.path).replicas[0].path
            buff = self.session.data_objects.open(data.path, 'r').read()
            with open(files_path + data.name, 'wb') as f:
                f.write(buff)
