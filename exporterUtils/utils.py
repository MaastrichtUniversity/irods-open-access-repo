import configparser
import argparse
import logging
import os

logger = logging.getLogger('iRODS to Dataverse')


def init_logger():
    """
    Initiate logging handler and formatter
    Level DEBUG
    """
    # log_level = os.environ['LOG_LEVEL']
    logging.basicConfig(level=logging.getLevelName('INFO'), format='%(asctime)s %(levelname)s %(message)s')

    # logger.setLevel(logging.DEBUG)
    # fh = logging.FileHandler('info.log')
    # formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    # fh.setFormatter(formatter)
    # logger.addHandler(fh)


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
                        help='alias or id of the dataverseManager where to upload the files')
    parser.add_argument('-d', "--delete", required=False, action='store_true',
                        help='delete the collections files after upload')
    parser.add_argument('-r', "--restrict", required=False, action='store_true',
                        help='restrict all uploaded files')
    args = parser.parse_args()

    return args


def parse_config(ini):
    """
    Parse the configuration file with iRODS & Dataverse credentials

    :param ini: Path to the configuration file *.ini
    """
    logger.info("Read config file")
    config = configparser.ConfigParser()
    config.read(ini)

    iRODS_config = {}
    dataverse_config = {}

    # iRODS config init
    iRODS_config.update({"host": config['iRODS']['host']})
    iRODS_config.update({"port": config['iRODS']['port']})
    iRODS_config.update({"user": config['iRODS']['user']})
    iRODS_config.update({"password": config['iRODS']['password']})
    iRODS_config.update({"zone": config['iRODS']['zone']})

    # Dataverse config init
    dataverse_config.update({"host": config['Dataverse']['host']})
    dataverse_config.update({"token": config['Dataverse']['token']})

    return iRODS_config, dataverse_config
