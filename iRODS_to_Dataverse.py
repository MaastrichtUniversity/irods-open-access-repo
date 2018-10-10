import requests
import json

import os
import configparser
import logging

import argparse

from irodsClient import irodsClient
from metadataMapper import metadataMapper

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

"""
TODO
*Map dataset metadata json *** Done
*Delete collection after update
"""


def init_logger():
    logger.setLevel(logging.DEBUG)
    # create file handler which logs even debug messages
    fh = logging.FileHandler('info.log')
    # fh.setLevel(logging.DEBUG)
    # fh.setLevel(logging.ERROR)
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


def import_dataset(md):
    print("Map metadata")
    logger.info("Map metadata")

    url = dataverse_config.get("host") + "/api/dataverses/" + dataverse_config.get(
        "alias") + "/datasets/:import?pid=hdl:" + pid + "&release=no"

    status = check_dataset_exist()
    # print(status)
    if status == 200:
        print("Dataset already exists, skip import")
        logger.info("Dataset already exists, skip import")

    elif status == 404:
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


init_logger()
parse_config()

host = iRODS_config.get("host")
port = iRODS_config.get("port")
user = iRODS_config.get("user")
password = iRODS_config.get("password")
zone = iRODS_config.get("zone")
path = iRODS_config.get("tmp_datasetfilepath")
collection= iRODS_config.get("collection_fullpath")

irc = irodsClient()
irc.connect(host, port, user, password, zone)
meta_dict = irc.read_collection(collection)
irc.write(path)

title = meta_dict.get("title")
creator = meta_dict.get("creator")
description = meta_dict.get("description")
date = meta_dict.get("date")
pid = meta_dict.get("PID")

mapper = metadataMapper(title, creator, description, date, pid)
md = mapper.read_metadata()
import_dataset(md)
import_files()