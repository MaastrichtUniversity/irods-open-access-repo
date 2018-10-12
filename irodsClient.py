from irods.session import iRODSSession
import xml.etree.ElementTree as ET
import logging

from irodsMetadata import irodsMetadata

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
        meta_dict = {}

        print("Read collection metadata")
        logger.info("Read collection metadata")
        self.coll = self.session.collections.get(collection_fullpath)
        for x in self.coll.metadata.items():
            meta_dict.update({x.name: x.value})
            self.imetadata.__dict__.update({x.name.lower(): x.value})

        print("Parse collection metadata.xml")
        logger.info("Parse collection metadata")
        meta_xml = collection_fullpath + "/metadata.xml"
        buff = self.session.data_objects.open(meta_xml, 'r')
        root = ET.fromstring(buff.read())

        self.imetadata.date = root.find("date").text
        meta_dict.update({"date": root.find("date").text})

        # opt
        self.imetadata.description = root.find("description").text
        meta_dict.update({"description": root.find("description").text})

        # meta_dict.update({"tissue": [root.find("tissue").get("id"), root.find("tissue").text]})
        # meta_dict.update({"technology": [root.find("technology").get("id"), root.find("technology").text]})
        # meta_dict.update({"organism": [root.find("organism").get("id"), root.find("organism").text]})

        meta_dict.update({"tissue": self.read_tag(root, "tissue")})
        meta_dict.update({"technology": self.read_tag(root, "technology")})
        meta_dict.update({"organism": self.read_tag(root, "organism")})

        self.imetadata.tissue = self.read_tag(root, "tissue")
        self.imetadata.technology = self.read_tag(root, "technology")
        self.imetadata.organism = self.read_tag(root, "organism")

        # factors = []
        # for i in root.iterfind("factors"):
        #     for k in i:
        #         if k.text is not None:
        #             print(k.tag, k.text)
        #             factors.append(k.text)
        meta_dict.update({"factors": self.read_tag_node(root, "factors")})
        meta_dict.update({"protocol": self.read_tag_node_dict(root, "protocol")})
        meta_dict.update({"contact": self.read_tag_node_dict(root, "contact")})

        self.imetadata.factors = self.read_tag_node(root, "factors")
        self.imetadata.protocol = self.read_tag_node_dict(root, "protocol")
        self.imetadata.contact = self.read_tag_node_dict(root, "contact")

        # articles = []
        # for k in root.findall("article"):
        #     for i in k.iter():
        #         if i.text is not None:
        #             print(i.tag, i.text)
        #             articles.append(i.text)
        meta_dict.update({"articles": self.read_tag_list(root, "article")})
        self.imetadata.articles = self.read_tag_list(root, "article")

        # for i in root.iterfind("protocol"):
        #     for k in i:
        #         if k.text is not None:
        #             print(k.tag, k.text)
        #
        # for i in root.iterfind("contact"):
        #     for k in i:
        #         if k.text is not None:
        #             print(k.tag, k.text)

        # for k in e.findall("article"):
        #     for i in k.iter():
        #         print(i.tag, i.text)

        # print(e.find("tissue").get("id"), e.find("tissue").text)
        # print(e.find("technology").get("id"), e.find("technology").text)
        # print(e.find("organism").get("id"), e.find("organism").text)
        # # print(e.find("organism").attrib)
        # # print(e.find("organism").text)
        #
        #
        # # for k in e.findall("protocol"):
        #     for i in k.iter("protocol"):
        #         if i is None:
        #             print("prot1")
        #             print(i.tag)
        #         else:
        #             print("prot2")
        #             print(i.tag, i.text)
        #
        #
        # for i in e.iterfind("protocol"):
        #     for k in i:
        #         if k.text is None:
        #                 print("prot1")
        #                 print(k.tag)
        #         else:
        #                 print("prot2")
        #                 print(k.tag, k.text)

        #
        # # protocol
        # for k in e.findall("protocol"):
        #     for i in k.iter("protocol"):
        #         if i is None:
        #             print("prot1")
        #             print(i.tag)
        #         else:
        #             print("prot2")
        #             print(i.tag, i.text)
        #
        # # article
        # for k in e.findall("article"):
        #     for i in k.iter():
        #             print(i.tag, i.text)
        #
        # print("factors:")
        # for k in e.find("factors").findall("factor"):
        #     print(k.text)
        #
        # for k in e.findall("contact"):
        #     print("k\tcontact:")
        #     for i in k.iter():
        #         print("i\t", i.tag, i.text)
        '''
        <factors>
            <factor></factor>
            <factor>time</factor>
            <factor>hours</factor>
        </factors>
        <organism id="ncbitaxon:http://purl.obolibrary.org/obo/NCBITaxon_9606">Homo sapiens</organism>
        <tissue id="efo:http://www.ebi.ac.uk/efo/EFO_0001907">dorsal skin</tissue>
        <technology id="ero:http://purl.obolibrary.org/obo/ERO_0000336">cell staining</technology>
        
        <article></article>
        <article>https://doi.org/10.1016/j.cell.2018.09.035</article>
        
         <contact>
            <lastName>Doe</lastName>
            <firstName>John</firstName>
            <midInitials>M</midInitials>
            <email>john.doe@email.com</email>
            <phone>+31 6 43 25 26 27</phone>
            <address>neverland</address>
            <affiliation></affiliation>
            <role>Magicien</role>
          </contact>
        
        
        
        
        <protocol>
            <name>Prot</name>
            <filename>Prot.txt</filename>
        </protocol>
        '''
        # print(self.irm .__dict__)
        return meta_dict

    def write(self, files_path):

        print("Copy local collection replica")
        logger.info("Copy local collection replica")
        for data in self.coll.data_objects:
            # physical_path = session.data_objects.get(data.path).replicas[0].path
            buff = self.session.data_objects.open(data.path, 'r').read()
            with open(files_path + data.name, 'wb') as f:
                f.write(buff)
