import hashlib
import json
import logging
import requests
import time

from irodsManager.irodsUtils import get_bag_generator, bag_generator_faker
from requests_toolbelt.multipart.encoder import MultipartEncoder
from http import HTTPStatus
from xml.etree import ElementTree

logger = logging.getLogger('iRODS to Dataverse')


class EasyClient:
    """
    Easy client to import bagged collection
    """

    def __init__(self, host, user, pwd, irodsclient):
        """
        :param host: String IP of the EASY's host
        :param user: String user name
        :param pwd: String user password
        :param irodsclient: irodsClient object - client to iRODS database user
        """
        self.host = host
        self.user = user
        self.pwd = pwd

        self.irods_client = irodsclient
        self.pid = irodsclient.imetadata.pid
        self.collection = irodsclient.coll
        self.session = irodsclient.session
        self.rulemanager = irodsclient.rulemanager

        self.dataset_status = None
        self.dataset_url = None
        self.dataset_deposit_url = "https://act.easy.dans.knaw.nl/sword2/collection/1"
        self.dataset_pid = None
        self.last_export = None

        self.upload_success = {}

        self.deletion = False
        self.restrict = False
        self.restrict_list = []

        self.zip_name = "debug_archive.zip"

    # from memory_profiler import profile
    # @profile
    def post_it(self):
        collection = self.collection
        imetadata = self.irods_client.imetadata
        rulemanager = self.rulemanager

        self.irods_client.update_metadata_state('exporterState', 'prepare-export', 'prepare-bag')
        upload_success = {}
        irods_md5 = hashlib.md5()
        size_bundle = bag_generator_faker(collection, self.session, upload_success,
                                          rulemanager, irods_md5, imetadata)

        print(f"stream size: {size_bundle}")
        md5_hexdigest = irods_md5.hexdigest()
        print(f"{'--':<30}irods buffer MD5: {md5_hexdigest}")

        self.irods_client.update_metadata_state('exporterState', 'prepare-bag', 'zip-bag')

        upload_success = {}
        bundle_md5 = hashlib.md5()
        bundle_iterator = get_bag_generator(collection, self.session, upload_success,
                                            rulemanager, bundle_md5, imetadata, size_bundle)

        print(f"{'--':<30}Post bundle")
        self.irods_client.update_metadata_state('exporterState', 'zip-bag', 'upload-bag')

        resp = requests.post(
            self.dataset_deposit_url,
            data=bundle_iterator,
            auth=(self.user, self.pwd),
            headers={
                "Content-Disposition": "filename=debug_archive00.zip",
                "Content-MD5": f"{md5_hexdigest}",
                "In-Progress": "false",
                "Packaging": "http://purl.org/net/sword/package/SimpleZip",
                "Content-Type": "application/octet-stream"
            },
        )
        bundle_md5_hexdigest = bundle_md5.hexdigest()
        print(f"{'--':<30}request buffer MD5: {bundle_md5_hexdigest}")
        print(f"{'--':<30}status_code: {resp.status_code}")
        if resp.status_code == 201:
            logger.debug(f"{'--':<30}{resp.content.decode('utf-8')}")
            self.check_status(resp.content.decode('utf-8'))
        else:
            logger.error(f"{'--':<30}{resp.content.decode('utf-8')}")
            raise

    def check_status(self, content):
        print(f"{'--':<30}Check deposit status")
        ElementTree.register_namespace("atom", "http://www.w3.org/2005/Atom")
        ElementTree.register_namespace("terms", "http://purl.org/net/sword/terms/")

        root = ElementTree.fromstring(content)
        href = root.find("./{http://www.w3.org/2005/Atom}link/[@rel='http://purl.org/net/sword/terms/statement']").get(
            'href')

        previous_term = "UPLOADED"
        self.irods_client.update_metadata_state('exporterState', 'upload-bag', previous_term)
        while True:
            resp = requests.get(href, auth=(self.user, self.pwd))

            if resp.status_code != 200:
                content = resp.content.decode("utf-8")
                print(resp.status_code)
                print(content)
                break

            content = resp.content.decode("utf-8")
            root = ElementTree.fromstring(content)
            category = root.find("./{http://www.w3.org/2005/Atom}category")
            term_refreshed = category.get('term')
            if term_refreshed == "INVALID" or term_refreshed == "REJECTED" or term_refreshed == "FAILED":
                print(term_refreshed)
                print(category.text)
                if previous_term != term_refreshed:
                    logger.info(f"{'--':<30}Update state {term_refreshed}")
                    self.irods_client.update_metadata_state('exporterState', previous_term, term_refreshed)
                break
            elif term_refreshed == "ARCHIVED":
                print(term_refreshed)
                print(category.text)
                if previous_term != term_refreshed:
                    logger.info(f"{'--':<30}Update state {term_refreshed}")
                    self.irods_client.update_metadata_state('exporterState', previous_term, term_refreshed)
                break
            else:
                print(term_refreshed)
                print(category.text)
                if previous_term != term_refreshed:
                    logger.info(f"{'--':<30}Update state {term_refreshed}")
                    self.irods_client.update_metadata_state('exporterState', previous_term, term_refreshed)
                previous_term = term_refreshed
                time.sleep(15)

    def final_report(self):
        logger.info("Report final progress")
        # self.irods_client.add_metadata_state('externalPID', self.dataset_pid, "Easy")
        self.irods_client.update_metadata_state('exporterState', 'ARCHIVED', 'exported')
        time.sleep(5)
        self.irods_client.remove_metadata_state('exporterState', 'exported')
        logger.info("Upload Done")
