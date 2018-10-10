import dataverse.connection
from irods.session import iRODSSession
import xml.etree.ElementTree as ET
import requests
import json

import os
import configparser
import logging

import argparse

parser = argparse.ArgumentParser(usage='%(prog)s [options]',
                                 description='Upload data from iRODS to Dataverse.')
parser.add_argument('-i', "--ini", required=True,
                    help='path to the config.ini file')
parser.add_argument('-c', "--collection", required=True,
                    help='path to the iRODS collection ')
parser.add_argument('-a', "--dataverseAlias", required=True,
                    help='alias or id of the dataverse where to upload the files)')
args = parser.parse_args()

logger = logging.getLogger('iRODS to Dataverse')

iRODS_config = {}
dataverse_config = {}
meta_dict = {}

"""
TODO
*Map dataset metadata json *** Done
*Delete collection after update
"""


def set_logger():
    logger.setLevel(logging.DEBUG)
    # create file handler which logs even debug messages
    fh = logging.FileHandler('info.log')
    fh.setLevel(logging.DEBUG)
    fh.setLevel(logging.ERROR)
    # create console handler with a higher log level
    # ch = logging.StreamHandler()
    # ch.setLevel(logging.ERROR)
    # create formatter and add it to the handlers
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    # ch.setFormatter(formatter)
    # add the handlers to the logger
    logger.addHandler(fh)
    # logger.addHandler(ch)


def parse_config():
    print("Read config file")
    logger.info("Read config file")
    config = configparser.ConfigParser()
    config.read(args.ini)

    # iRODS config init
    iRODS_config.update({"host": config['iRODS']['host']})
    iRODS_config.update({"port": config['iRODS']['port']})
    iRODS_config.update({"user": config['iRODS']['user']})
    iRODS_config.update({"password": config['iRODS']['password']})
    iRODS_config.update({"zone": config['iRODS']['zone']})
    iRODS_config.update({"collection_fullpath": args.collection})
    # iRODS_config.update({"collection_fullpath": config['iRODS']['collection_fullpath']})
    iRODS_config.update({"tmp_datasetfilepath": config['iRODS']['tmp_datasetfilepath']})

    # Dataverse config init
    dataverse_config.update({"host": config['Dataverse']['host']})
    dataverse_config.update({"token": config['Dataverse']['token']})
    dataverse_config.update({"alias": args.dataverseAlias})

    # dataverse_config.update({"alias": config['Dataverse']['dataverse_alias']})


def irods_init():
    print("Connect to iRODS")
    logger.info("Connect to iRODS")
    with iRODSSession(host=iRODS_config.get("host"),
                      port=iRODS_config.get("port"), user=iRODS_config.get("user"),
                      password=iRODS_config.get("password"), zone=iRODS_config.get("zone")) as session:
        pass
    return session


def irods_read(i_session):
    collection_path = iRODS_config.get("collection_fullpath")

    print("Read collection metadata")
    logger.info("Read collection metadata")
    coll = i_session.collections.get(collection_path)
    for x in coll.metadata.items():
        meta_dict.update({x.name: x.value})

    print("Parse collection metadata.xml")
    logger.info("Parse collection metadata")
    meta_xml = collection_path + "/metadata.xml"
    buff = i_session.data_objects.open(meta_xml, 'r')
    e = ET.fromstring(buff.read())
    meta_dict.update({"description": e.find("description").text})
    meta_dict.update({"date": e.find("date").text})

    return coll


def irods_write(coll):
    files_path = iRODS_config.get("tmp_datasetfilepath")

    print("Copy local collection replica")
    logger.info("Copy local collection replica")
    for data in coll.data_objects:
        # physical_path = session.data_objects.get(data.path).replicas[0].path
        buff = session.data_objects.open(data.path, 'r').read()
        with open(files_path + data.name, 'wb') as f:
            f.write(buff)


def dataverse_init():
    host = dataverse_config.get("host")
    token = dataverse_config.get("token")
    return connection.Connection(host, token, use_https=False)


def auth_dataverse(token):
    return token, None


def check_dataset_exist():
    print("Check_dataset_exist")
    logger.info("check_dataset_exist")

    pid = meta_dict.get("PID")
    url = dataverse_config.get("host") + "/api/datasets/:persistentId/?persistentId=hdl:" + pid
    resp = requests.get(
        url=url,
        headers={'X-Dataverse-key': dataverse_config.get("token")},
    )
    # print(resp.status_code)
    logger.info(resp.content)
    return resp.status_code


def import_dataset():
    print("Map metadata")
    logger.info("Map metadata")
    md = read_metadata()

    pid = meta_dict.get("PID")
    url = dataverse_config.get("host") + "/api/dataverses/" + dataverse_config.get(
        "alias") + "/datasets/:import?pid=hdl:" + pid + "&release=no"
    # print(url)

    if check_dataset_exist() == 200:
        print("Dataset already exists, skip import")
        logger.info("Dataset already exists, skip import")

    elif check_dataset_exist() == 404:
        print("Import dataset")
        logger.info("Import dataset")
        resp = requests.post(
            url,
            data=json.dumps(md),
            headers={'Content-type': 'application/json',
                     'X-Dataverse-key': dataverse_config.get("token")},
        )
        # print(resp.content)
        logger.info("resp.content")


def import_files():
    pid = meta_dict.get("PID")

    url_file = dataverse_config.get("host") + "/api/datasets/:persistentId/add?persistentId=hdl:" + pid
    upload_file(url_file, iRODS_config.get("tmp_datasetfilepath"))

    print("Upload Done")
    logger.info("Upload Done")


def upload_file(url, tmp_datasetfilepath):
    print("Upload files:")
    logger.info("Upload files:")

    for file in os.listdir(tmp_datasetfilepath):
        print("--\t" + file)
        logger.info("--\t" + file)
        files = {'file': open(tmp_datasetfilepath + file, 'rb'),
                 'jsonData': '{"description": "My API test description.", "categories": ["Data"], "restrict": "false"}'}
        resp = requests.post(
            url,
            files=files,
            headers={'X-Dataverse-key': dataverse_config.get("token")},
        )
        # print(resp.status_code)
        if resp.status_code == 400:
            print("--\t\t\t" + json.loads(resp.content)['message'])
            logger.error("--\t\t\t" + json.loads(resp.content)['message'])
        else:
            logger.info(resp.content)


def read_metadata():
    title = meta_dict.get("title")
    creator = meta_dict.get("creator")
    description = meta_dict.get("description")
    date = meta_dict.get("date")

    # dataset_json = None
    with open('template.json') as f:
        dataset_json = json.load(f)

    md = dataset_json['datasetVersion']

    pid = meta_dict.get("PID")
    print("--\t" + pid)
    logger.info("--\t" + pid)
    pid = pid.split("/")
    update_pid(md, pid[0], pid[1])

    new = add_author(creator)
    update_fields(md, new)
    new = add_description(description)
    update_fields(md, new)
    new = add_date(date)
    update_fields(md, new)
    new = add_title(title)
    update_fields(md, new)
    new = add_subject()
    update_fields(md, new)
    new = add_contact_email(creator)
    update_fields(md, new)

    dataset_json['datasetVersion'] = md
    # print(json.dumps(dataset_json, indent=4))
    logger.info(json.dumps(dataset_json, indent=4))

    return dataset_json


def update_pid(md, authority, identifier, hdl="hdl"):
    md["protocol"] = hdl
    md["authority"] = authority
    md["identifier"] = identifier

    # print(json.dumps(md, indent=4))


def update_fields(md, new):
    fields = md["metadataBlocks"]["citation"]["fields"]
    fields.append(new)
    # print(json.dumps(md, indent=4))


def add_author(author, affiliation):
    new = {
        "typeName": "author",
        "multiple": True,
        "value": [
            {
                "authorAffiliation": {
                    "typeName": "authorAffiliation",
                    "multiple": False,
                    "value": affiliation,
                    "typeClass": "primitive"
                },
                "authorName": {
                    "typeName": "authorName",
                    "multiple": False,
                    "value": author,
                    "typeClass": "primitive"
                }
            }
        ],
        "typeClass": "compound"
    }
    return new


def add_author(author):
    new = {
        "typeName": "author",
        "multiple": True,
        "value": [
            {
                "authorName": {
                    "typeName": "authorName",
                    "multiple": False,
                    "value": author,
                    "typeClass": "primitive"
                }
            }
        ],
        "typeClass": "compound"
    }
    return new


def add_title(title):
    new = {
        "typeName": "title",
        "multiple": False,
        "value": title,
        "typeClass": "primitive"
    }
    return new


def add_description(description):
    new = {
        "typeName": "dsDescription",
        "multiple": True,
        "value": [
            {
                "dsDescriptionValue": {
                    "typeName": "dsDescriptionValue",
                    "multiple": False,
                    "value": description,
                    "typeClass": "primitive"
                }
            }
        ],
        "typeClass": "compound"
    }
    return new


def add_subject():
    new = {
        "typeName": "subject",
        "multiple": True,
        "value": [
            "Medicine, Health and Life Sciences"
        ],
        "typeClass": "controlledVocabulary"
    }
    return new


def add_date(date):
    new = {
        "typeName": "productionDate",
        "multiple": False,
        "value": date,
        "typeClass": "primitive"
    }
    return new


def add_contact_email(email):
    new = {
        "typeName": "datasetContact",
        "multiple": True,
        "value": [
            {
                "datasetContactEmail": {
                    "typeName": "datasetContactEmail",
                    "multiple": False,
                    "value": email,
                    "typeClass": "primitive"
                }
            }
        ],
        "typeClass": "compound"
    }
    return new


set_logger()
parse_config()
session = irods_init()
coll = irods_read(session)
irods_write(coll)
import_dataset()
import_files()

# md = read_metadata()
