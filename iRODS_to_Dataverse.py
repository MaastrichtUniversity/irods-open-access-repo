import configparser
import logging

import argparse
from builtins import print

from dataverseClient import DataverseClient
from irodsClient import irodsClient
from metadataMapper import MetadataMapper

logger = logging.getLogger('iRODS to Dataverse')
iRODS_config = {}
dataverse_config = {}

"""
TODO
*Add/Remove AVU without unit
*Parallel upload

*Archive  => uncompressed
"""


def init_parser():
    """
    Initiate argument parser

    :rtype: argparse
    :return: parse_args
    """
    parser = argparse.ArgumentParser(usage='%(prog)s [options]',
                                     description='Upload data from iRODS to Dataverse.')
    parser.add_argument('-i', "--ini", required=True,
                        help='path to the config.ini file')
    parser.add_argument('-c', "--collection", required=True,
                        help='path to the iRODS collection ')
    parser.add_argument('-a', "--dataverseAlias", required=True,
                        help='alias or id of the dataverse where to upload the files')
    parser.add_argument('-d', "--delete", required=False, action='store_true',
                        help='delete the collections files after upload')
    parser.add_argument('-r', "--restrict", required=False, action='store_true',
                        help='restrict all uploaded files')
    args = parser.parse_args()

    return args


def init_logger():
    """
    Initiate logging handler and formatter
    Level DEBUG
    """
    logger.setLevel(logging.DEBUG)
    fh = logging.FileHandler('info.log')
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    logger.addHandler(fh)


def parse_config(ini):
    """
    Parse the configuration file with iRODS & Dataverse credentials

    :param ini: Path to the configuration file *.ini
    """
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


def main():
    # Init
    print("Init")
    args = init_parser()
    init_logger()
    parse_config(args.ini)

    # iRODS
    print("iRODS")
    collection = args.collection
    iclient = irodsClient(iRODS_config)

    iclient.connect()
    iclient.read_collection_metadata(collection)

    iColl = iclient.session.data_objects.get(collection)

    iColl.metadata.add('exporterState', 'prepare-export', '0')

    # Metadata
    print("Metadata")
    mapper = MetadataMapper(iclient.imetadata)
    md = mapper.read_metadata()

    # Dataverse
    print("Dataverse")
    host = dataverse_config.get("host")
    token = dataverse_config.get("token")
    alias = args.dataverseAlias

    iColl.metadata.remove('exporterState', 'prepare-export', '0')
    iColl.metadata.add('exporterState', 'export', '1')

    dv = DataverseClient(host, token, alias, iclient)
    dv.import_dataset(md)
    dv.import_files(args.delete, args.restrict)

    iColl.metadata.remove('exporterState', 'export', '1')
    iColl.metadata.add('exporterState', 'exported', '2')


if __name__ == "__main__":
        main()
