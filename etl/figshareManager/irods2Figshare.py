import logging
import time

from irodsManager.irodsClient import irodsClient
from figshareManager.figshareClient import FigshareClient
# from exporterUtils.utils import parse_config

logger = logging.getLogger('iRODS to Dataverse')


class FigshareExporter:

    def __init__(self):
        self.repository = "Figshare"

    def init_export(self, data):
        path = "/nlmumc/projects/" + data['project'] + "/" + data['collection']
        ini = "resources/config.ini"
        self.do_export(ini, path, data['token'])

    def do_export(self, ini, collection, token):
        # Init
        logger.info("Init")
        config = parse_config(ini)
        irods_config = config[0]
        logger.info(irods_config)

        # iRODS
        logger.info("iRODS")
        iclient = irodsClient(irods_config)
        iclient.connect()
        iclient.read_collection_metadata(collection)

        iclient.update_metadata_state('exporterState', 'in-queue-for-export', 'prepare-export')

        # Metadata
        # logger.info("Metadata")
        # logger.info("Metadata")
        # mapper = ZenodoMetadataMapper(iclient.imetadata)
        # md = mapper.read_metadata()

        iclient.rulemanager.rule_open()
        iclient.update_metadata_state('exporterState', 'prepare-export', 'do-export')

        fs = FigshareClient(iclient, token)
        fs.create_article()
        fs.import_files()

        url = "https://figshare.com/account/articles/" + str(fs.article_id)
        iclient.update_metadata_state('externalLink', url, url)

        iclient.update_metadata_state('exporterState', 'do-export', 'exported')
        time.sleep(15)
        iclient.update_metadata_state('exporterState', 'exported', '')

        iclient.rulemanager.rule_close()
        logger.info("Upload Done")


def main():
    path = "/nlmumc/projects/P000000003/C000000001"
    # self.TOKEN = '12e89a66fec6b2553a2ee297d69348dac1bfe97dd8371564fc56629cae32c14256a31382e898de8f5079a9a9eb68e8b61d1dfad960c509b6a5f8dfa6bce7c8f0'
    fs = FigshareExporter()
    fs.do_export("/opt/app/resources/config.ini", path)


if __name__ == "__main__":
        main()
