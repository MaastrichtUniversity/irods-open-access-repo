import hashlib
import json
import logging
import io
import requests
import time

from http import HTTPStatus

logger = logging.getLogger('iRODS to Dataverse')


class DataverseClient:
    """
    Dataverse client to import datasets and files
    """
    READ_BUFFER_SIZE = 1024 * 1048576

    def __init__(self, host, token, alias, irodsclient):
        """

        :param host: String IP of the dataverseManager's host
        :param token: String token credential
        :param alias: String Alias/ID of the dataverseManager where to import dataset & files
        :param irodsclient: irodsClient object - client to user iRODS database
        """
        self.host = host
        self.alias = alias
        self.token = token

        self.irods_client = irodsclient
        self.pid = irodsclient.imetadata.pid
        self.collection = irodsclient.coll
        self.session = irodsclient.session
        self.rulemanager = irodsclient.rulemanager

        self.dataset_status = None
        self.dataset_url = None
        self.dataset_pid = None
        self.last_export = None

    def create_dataset(self, md, data_export=False):
        logger.info(f"{'--':<10}Dataset - request creation")
        self.irods_client.update_metadata_state('exporterState', 'prepare-export', 'do-export')
        url = f"{self.host}/api/dataverses/{self.alias}/datasets/"

        resp = requests.post(
            url,
            data=json.dumps(md),
            headers={'Content-type': 'application/json',
                     'X-Dataverse-key': self.token},
        )
        self.dataset_status = resp.status_code
        if self.dataset_status == HTTPStatus.CREATED.value:
            self.dataset_pid = json.loads(resp.content.decode("utf-8"))['data']['persistentId']
            logger.info(f"{'--':<20}Dataset created with pid: {self.dataset_pid}")
        else:
            logger.error(resp.content)

        if not data_export and self.dataset_status == HTTPStatus.CREATED.value:
            logger.info("Report final progress no data")
            self.dataset_url = f"{self.host}/dataset.xhtml?persistentId={self.dataset_pid}&version=DRAFT"
            self.irods_client.add_metadata_state('externalLink', self.dataset_url)
            self.irods_client.update_metadata_state('exporterState', 'do-export', 'exported')
            time.sleep(5)
            self.irods_client.remove_metadata_state('exporterState', 'exported')
            logger.info("Upload Done no data")

    def import_files(self, deletion=False, restrict=False, restrict_list=''):
        url_dataset = f"{self.host}/api/datasets/:persistentId/add?persistentId={self.dataset_pid}"

        if self.dataset_status == HTTPStatus.CREATED.value:
            self.import_collection(url_dataset, deletion, restrict, restrict_list)
            logger.info("Report final progress")
            self.irods_client.add_metadata_state('externalLink', self.dataset_url)
            self.irods_client.update_metadata_state('exporterState', 'do-export', 'exported')
            time.sleep(5)
            self.irods_client.remove_metadata_state('exporterState', 'exported')
            logger.info("Upload Done")
        else:
            logger.error(f"{'--':<20}Skip import_files")
            self.irods_client.remove_metadata_state('exporterState', 'do-export')

    def import_collection(self, url, deletion, restrict, restrict_list):
        logger.info(f"{'--':<10}Upload files")
        restrict_list = restrict_list.split(", ")
        logger.info(f"{'--':<20}restrict_list{restrict_list}")
        upload_success = {}

        self.irods_client.update_metadata_state('exporterState', 'do-export', 'do-export-start')
        self.last_export = 'do-export-start'

        for data in self.collection.data_objects:
            logger.info(f"{'--':<20}{data.name}")

            display_name = data.name
            if len(data.name) > 15:
                display_name = data.name[:15] + "..."

            self.irods_client.update_metadata_state('exporterState', self.last_export, 'do-export-' + display_name)
            self.last_export = 'do-export-' + display_name

            logger.info(f"{'--':<30}calculate checksum")
            buff = self.session.data_objects.open(data.path, 'r')

            irods_sha = hashlib.sha256()
            irods_md5 = hashlib.md5()
            buff_read = io.BytesIO()
            for chunk in iter(lambda: buff.read(self.READ_BUFFER_SIZE), b''):
                irods_sha.update(chunk)
                irods_md5.update(chunk)
                buff_read.write(chunk)

            buff_read.seek(0)

            md5_hexdigest = irods_md5.hexdigest()
            sha_hexdigest = irods_sha.hexdigest()

            logger.info(f"{'--':<30}local MD5: {md5_hexdigest}")
            logger.info(f"{'--':<30}local SHA-256: {sha_hexdigest}")

            logger.info(f"{'--':<30}Query iRODS checksum")
            irods_hash_decode = self.rulemanager.rule_checksum(data.name)
            logger.info(f"{'--':<30}iRODS SHA-256: {irods_hash_decode}")

            if sha_hexdigest == irods_hash_decode:
                logger.info(f"{'--':<30}SHA-256 match: True")

                flag = "false"
                if restrict:
                    flag = "true"
                elif len(restrict_list) >= 0 and data.name in restrict_list:
                    flag = "true"

                files = {'file': (data.name, buff_read),
                         'jsonData': '{"description": "My API test description.",'
                                     ' "categories": ["Data"],'
                                     ' "restrict": "' + flag + '"'
                                     '}'
                         }

                resp = requests.post(
                    url,
                    files=files,
                    headers={'X-Dataverse-key': self.token}
                )
                if resp.status_code == HTTPStatus.OK.value:
                    md5 = json.loads(resp.content.decode("utf-8"))['data']['files'][0]['dataFile']['md5']
                    logger.info(f"{'--':<30}Dataverse MD5: {md5}")
                    if md5 == md5_hexdigest:
                        logger.info(f"{'--':<30}Dataverse MD5 match: True")
                        upload_success.update({data.name: True})
                    else:
                        logger.error(f"{'--':<30}Dataverse MD5 match: False")
                else:
                    logger.error(f"{'--':<30}{resp.content.decode('utf-8')}")

            else:
                logger.error(f"{'--':<30}SHA-256 checksum failed")
                logger.error(f"{'--':<30}Skip upload: {data.name}")

        self.irods_client.update_metadata_state('exporterState', self.last_export, 'do-export')
        self.dataset_url = f"{self.host}/dataset.xhtml?persistentId={self.dataset_pid}&version=DRAFT"

        logger.info(f"{'--':<10} Upload success: {repr(upload_success)}")

        if deletion:
            self.rulemanager.rule_deletion(upload_success)
