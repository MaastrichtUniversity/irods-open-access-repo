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
    fh = logging.FileHandler('info.log')
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    logger.addHandler(fh)


def parse_config(ini):
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

    # Dataverse config init
    dataverse_config.update({"host": config['Dataverse']['host']})
    dataverse_config.update({"token": config['Dataverse']['token']})


def create_tmp_dir(path, collection):
    path = path + collection + os.sep
    os.makedirs(path, exist_ok=True)

    return path


def main():
    #Init
    args = init_parser()
    init_logger()
    parse_config(args.ini)

    #iRODS
    collection = args.collection
    iclient = irodsClient(iRODS_config)
    iclient.connect()
    iclient.read_collection(collection)

    mapper = MetadataMapper(iclient.imetadata)
    md = mapper.read_metadata()

    #Dataverse
    host = dataverse_config.get("host")
    token = dataverse_config.get("token")
    alias = args.dataverseAlias

    dv = dataverseClient(host, alias, token, iclient, md)
    dv.import_dataset()
    dv.import_files()


if __name__ == "__main__":
    main()
