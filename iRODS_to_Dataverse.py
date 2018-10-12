import configparser
import logging

import argparse
import os
from builtins import print

from dataverseClient import dataverseClient
from irodsClient import irodsClient
from metadataMapper import MetadataMapper


logger = logging.getLogger('iRODS to Dataverse')
iRODS_config = {}
dataverse_config = {}

"""
TODO
*Generate checksum
*Delete collection after update
"""


def init_parser():
    parser = argparse.ArgumentParser(usage='%(prog)s [options]',
                                     description='Upload data from iRODS to Dataverse.')
    parser.add_argument('-i', "--ini", required=True,
                        help='path to the config.ini file')
    parser.add_argument('-c', "--collection", required=True,
                        help='path to the iRODS collection ')
    parser.add_argument('-a', "--dataverseAlias", required=True,
                        help='alias or id of the dataverse where to upload the files)')
    args = parser.parse_args()

    return args


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


def parse_config(ini, collection, dataverse_alias):
    print("Read config file")
    logger.info("Read config file")
    config = configparser.ConfigParser()
    config.read(ini)

    # iRODS config init
    iRODS_config.update({"host": config['iRODS']['host']})
    iRODS_config.update({"port": config['iRODS']['port']})
    iRODS_config.update({"user": config['iRODS']['user']})
    iRODS_config.update({"password": config['iRODS']['password']})
    iRODS_config.update({"zone": config['iRODS']['zone']})
    # iRODS_config.update({"collection_fullpath": collection})
    # iRODS_config.update({"collection_fullpath": config['iRODS']['collection_fullpath']})
    iRODS_config.update({"tmp_datasetfilepath": config['iRODS']['tmp_datasetfilepath']})

    # Dataverse config init
    dataverse_config.update({"host": config['Dataverse']['host']})
    dataverse_config.update({"token": config['Dataverse']['token']})
    dataverse_config.update({"alias": dataverse_alias})

    # dataverse_config.update({"alias": config['Dataverse']['dataverse_alias']})

def create_tmp_dir(path, collection):
    path = path + collection + os.sep
    os.makedirs(path,exist_ok=True)

    return path

def main():
    #Init
    args = init_parser()
    init_logger()
    parse_config(args.ini, args.collection, args.dataverseAlias)

    #iRODS
    host = iRODS_config.get("host")
    port = iRODS_config.get("port")
    user = iRODS_config.get("user")
    password = iRODS_config.get("password")
    zone = iRODS_config.get("zone")
    path = iRODS_config.get("tmp_datasetfilepath")
    collection = args.collection
    # collection = iRODS_config.get("collection_fullpath")

    path = create_tmp_dir(path, collection)

    iclient = irodsClient()
    iclient.connect(host, port, user, password, zone)
    meta_dict = iclient.read_collection(collection)
    # print(meta_dict.keys())
    # print(meta_dict)
    print(iclient.imetadata.__dict__)
    # print(list(meta_dict.keys()))
    iclient.write(path)

    # # Metadata mapping
    # title = meta_dict.get("title")
    # creator = meta_dict.get("creator")
    # description = meta_dict.get("description")
    # date = meta_dict.get("date")
    # pid = meta_dict.get("PID")
    #
    # mapper = metadataMapper(title, creator, description, date, pid)
    mapper = MetadataMapper(iclient.imetadata)
    md = mapper.read_metadata()

    #Dataverse
    host = dataverse_config.get("host")
    token = dataverse_config.get("token")
    alias = dataverse_config.get("alias")
    # path = dataverse_config.get("tmp_datasetfilepath")

    dv = dataverseClient(host, alias, token, path, iclient.imetadata.pid, md)
    dv.import_dataset()
    dv.import_files()


if __name__ == "__main__":
    main()
