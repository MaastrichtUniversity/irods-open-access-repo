import hashlib
import logging
import requests
import time

from irodsManager.irodsUtils import get_bag_generator, bag_generator_faker, ExporterClient, ExporterState as Status
from http import HTTPStatus
from multiprocessing import Pool
from xml.etree import ElementTree

logger = logging.getLogger('iRODS to Dataverse')


class EasyClient(ExporterClient):
    """Easy client to import bagged collection
    """

    def __init__(self, host, user, pwd, token, irodsclient):
        """
        :param host: String IP of the EASY's host
        :param user: String user name
        :param pwd: String user password
        :param irodsclient: irodsClient object - client to iRODS database user
        """
        self.host = host
        self.user = user
        self.pwd = pwd
        self.token = token

        self.irods_client = irodsclient
        self.collection = irodsclient.coll
        self.session = irodsclient.session
        self.rulemanager = irodsclient.rulemanager
        self.imetadata = irodsclient.imetadata

        self.dataset_status = None
        self.dataset_url = None
        self.dataset_deposit_url = f"{self.host}/sword2/collection/1"
        self.dataset_pid = None
        self.last_export = None

        self.pool = None
        self.result = None
        # self.bag_md5 = None

        self.upload_success = {}

        self.deletion = False
        self.restrict = False
        self.restrict_list = []

        self.zip_name = "debug_archive.zip"

    def post_it(self):
        logger.info(f"{'--':<10}Prepare bag")

        self.irods_client.update_metadata_state('create-exporter', 'prepare-bag')

        self.pool = Pool(processes=1)
        self.result = self.pool.apply_async(self.run_checksum, [self.collection.path])

        irods_md5 = hashlib.md5()
        bag_size = bag_generator_faker(self.irods_client, self.upload_success, irods_md5)
        md5_hexdigest = irods_md5.hexdigest()
        logger.info(f"{'--':<20}Stream predicted size: {bag_size}")
        logger.info(f"{'--':<20}iRODS buffer MD5: {md5_hexdigest}")

        self.irods_client.update_metadata_state('prepare-bag', Status.VALIDATE_CHECKSUM.value)
        self.pool.close()
        self.pool.join()
        chksums = self.result.get()
        count = 0
        # validated = False
        for k in self.upload_success.keys():
            if self.upload_success[k] == chksums[k]:
                self.upload_success.update({k: True})
                count += 1
        if count == len(self.upload_success):
            # validated = True
            logger.info(f"{'--':<20}iRODS & buffer SHA-256 checksum: validated")
            self.irods_client.update_metadata_state(Status.VALIDATE_CHECKSUM.value, 'prepare-bag')
        else:
            logger.error(f"{'--':<20}SHA-256 checksum: failed")
            self.irods_client.update_metadata_state(Status.VALIDATE_UPLOAD.value, Status.UPLOAD_CORRUPTED.value)

        # print(validated)

        self.irods_client.update_metadata_state('prepare-bag', 'zip-bag')
        self.upload_success = {}

        bag_md5 = hashlib.md5()
        bag_iterator = get_bag_generator(self.irods_client, self.upload_success, bag_md5, bag_size)

        logger.info(f"{'--':<10}Upload bag")
        self.irods_client.update_metadata_state('zip-bag', 'upload-bag')

        resp = requests.post(
            self.dataset_deposit_url,
            data=bag_iterator,
            auth=(self.user, self.pwd),
            headers={
                "X-Authorization": self.token,
                "Content-Disposition": "filename=debug_archive00.zip",
                "Content-MD5": f"{md5_hexdigest}",
                "In-Progress": "false",
                "Packaging": "http://purl.org/net/sword/package/SimpleZip",
                "Content-Type": "application/octet-stream"
            },
        )
        logger.info(f"{'--':<20}Bag buffer MD5: {bag_md5.hexdigest()}")
        if resp.status_code == HTTPStatus.CREATED:
            logger.debug(f"{'--':<30}{resp.content.decode('utf-8')}")
            self.check_status(resp.content.decode('utf-8'))
        else:
            logger.error(f"{'--':<30}status_code: {resp.status_code}")
            logger.error(f"{'--':<30}{resp.content.decode('utf-8')}")

    def check_status(self, content):
        logger.info(f"{'--':<20}Check deposit status")
        ElementTree.register_namespace("atom", "http://www.w3.org/2005/Atom")
        ElementTree.register_namespace("terms", "http://purl.org/net/sword/terms/")

        root = ElementTree.fromstring(content)
        href = root.find("./{http://www.w3.org/2005/Atom}link/[@rel='http://purl.org/net/sword/terms/statement']").get(
            'href')
        previous_term = "UPLOADED"
        self.irods_client.update_metadata_state('upload-bag', previous_term)
        while True:
            resp = requests.get(href,
                                auth=(self.user, self.pwd),
                                headers={"X-Authorization": self.token}
                                )
            if resp.status_code != HTTPStatus.OK:
                content = resp.content.decode("utf-8")
                logger.debug(f"{'--':<30}{resp.status_code}")
                logger.debug(f"{'--':<30}{content}")
                break

            content = resp.content.decode("utf-8")
            root = ElementTree.fromstring(content)
            category = root.find("./{http://www.w3.org/2005/Atom}category")
            term_refreshed = category.get('term')
            if term_refreshed == "INVALID" or term_refreshed == "REJECTED" or term_refreshed == "FAILED":
                logger.error(f"{'--':<30}state: {term_refreshed}")
                logger.error(f"{'--':<30}state description: {category.text}")
                self.irods_client.update_metadata_state(previous_term, term_refreshed)
                break
            elif term_refreshed == "ARCHIVED":
                logger.info(f"{'--':<30}state: {term_refreshed}")
                logger.info(f"{'--':<30}state description: {category.text}")
                self.dataset_pid = category.text
                self.irods_client.update_metadata_state(previous_term, term_refreshed)
                break
            else:
                logger.info(f"{'--':<30}state: {term_refreshed}")
                logger.info(f"{'--':<30}state description: {category.text}")
                if previous_term != term_refreshed:
                    self.irods_client.update_metadata_state(previous_term, term_refreshed)
                previous_term = term_refreshed
                time.sleep(15)

    def final_report(self):
        logger.info("Report final progress")
        self.irods_client.add_metadata('externalPID', self.dataset_pid, "Easy")
        self.irods_client.update_metadata_state('ARCHIVED', 'exported')
        time.sleep(5)
        self.irods_client.remove_metadata('exporterState', 'exported')
        logger.info("Upload Done")
