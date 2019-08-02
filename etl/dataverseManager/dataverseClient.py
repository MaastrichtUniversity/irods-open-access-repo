import hashlib
import json
import logging
import requests
import time

from irodsManager.irodsUtils import get_zip_generator, zip_generator_faker, ExporterClient, ExporterState as Status

from requests_toolbelt.multipart.encoder import MultipartEncoder
from http import HTTPStatus
from multiprocessing import Pool

logger = logging.getLogger('iRODS to Dataverse')


class DataverseClient(ExporterClient):
    """
    Dataverse client to import datasets and files
    """

    def __init__(self, host, token, alias, irodsclient):
        """
        :param host: String IP of the dataverseManager's host
        :param token: String token credential
        :param alias: String Alias/ID of the dataverseManager where to import dataset & files
        :param irodsclient: irodsClient object - client to iRODS database user
        """
        self.host = host
        self.alias = alias
        self.token = token

        self.irods_client = irodsclient
        self.collection = irodsclient.coll
        self.session = irodsclient.session
        self.rulemanager = irodsclient.rulemanager

        self.pool = None
        self.result = None
        self.irods_md5 = None

        self.dataset_status = None
        self.dataset_deposit_url = None
        self.dataset_pid = None
        self.last_export = None

        self.upload_success = {}
        self.upload_status_code = None

        self.deletion = False
        self.restrict = False
        self.restrict_list = []

        self.zip_name = f"Test_{irodsclient.imetadata.title}"

    def create_dataset(self, md, data_export=False):
        logger.info(f"{'--':<10}Dataset - request creation")

        self.irods_client.update_metadata_state(Status.CREATE_EXPORTER.value, Status.CREATE_DATASET.value)
        url = f"{self.host}/api/dataverses/{self.alias}/datasets/"

        resp = requests.post(
            url,
            data=json.dumps(md),
            headers={'Content-type': 'application/json',
                     'X-Dataverse-key': self.token
                     },
        )
        self.dataset_status = resp.status_code
        if self.dataset_status == HTTPStatus.CREATED.value:
            self.dataset_pid = resp.json()['data']['persistentId']
            self.dataset_deposit_url = f"{self.host}/api/datasets/:persistentId/add?persistentId={self.dataset_pid}"
            logger.info(f"{'--':<20}Dataset created with pid: {self.dataset_pid}")
        else:
            logger.error(f"{'--':<20}Create dataset failed")
            logger.error(resp.content)
            self.irods_client.update_metadata_state(Status.CREATE_DATASET.value, Status.CREATE_DATASET_FAILED.value)

        if not data_export and self.dataset_status == HTTPStatus.CREATED.value:
            self.irods_client.update_metadata_state(Status.CREATE_DATASET.value, Status.FINALIZE.value)
            self._final_report()

    def import_files(self, deletion=False, restrict=False, restrict_list=''):

        self.deletion = deletion
        self.restrict = restrict
        if len(restrict_list) > 0:
            self.restrict_list = restrict_list.split(", ")

        if self.dataset_deposit_url is not None:
            self.irods_client.update_metadata_state(Status.CREATE_DATASET.value, Status.PREPARE_COLLECTION.value)
            self.pool = Pool(processes=1)
            self.result = self.pool.apply_async(self.run_checksum, [self.collection.path])

            size_bundle = self._prepare_zip()
            response = self._zip_collection(size_bundle)
            validated = self._validate_checksum()
            if validated:
                self._validate_upload(response)
                if self.deletion:
                    self.rulemanager.rule_deletion(self.upload_success)
                self._final_report()
        else:
            logger.error(f"{'--':<20}Dataset unknown")
            self.irods_client.update_metadata_state(Status.CREATE_DATASET.value, Status.DATASET_UNKNOWN.value)

    def _prepare_zip(self):
        logger.info(f"{'--':<10}Prepare zip")

        self.irods_client.update_metadata_state(Status.PREPARE_COLLECTION.value, Status.ZIP_COLLECTION.value)
        irods_md5 = hashlib.md5()
        size_bundle = zip_generator_faker(self.irods_client, self.upload_success, irods_md5, self.restrict_list)
        md5_hexdigest = irods_md5.hexdigest()
        logger.info(f"{'--':<20}Buffer faker MD5: {md5_hexdigest}")

        return size_bundle

    def _zip_collection(self, size_bundle):
        logger.info(f"{'--':<10}Upload zip")

        self.irods_client.update_metadata_state(Status.ZIP_COLLECTION.value, Status.UPLOAD_ZIPPED_COLLECTION.value)
        self.irods_md5 = hashlib.md5()
        bundle_iterator = get_zip_generator(self.irods_client, self.upload_success,
                                            self.irods_md5, self.restrict_list, size_bundle)
        flag = "false"
        if self.restrict:
            flag = "true"
        json_data = {
            "restrict": flag
        }
        multipart_encoder = MultipartEncoder(
            fields={'jsonData': json.dumps(json_data),
                    'file': (self.zip_name, bundle_iterator)
                    }
        )
        resp = requests.post(
            self.dataset_deposit_url,
            data=multipart_encoder,
            headers={'Content-Type': multipart_encoder.content_type,
                     'X-Dataverse-key': self.token
                     },
        )

        return resp

    def _validate_checksum(self):
        logger.info(f"{'--':<10}Validate checksum")

        self.irods_client.update_metadata_state(Status.UPLOAD_ZIPPED_COLLECTION.value, Status.VALIDATE_CHECKSUM.value)
        self.pool.close()
        self.pool.join()
        chksums = self.result.get()
        count = 0
        validated = False
        for k in self.upload_success.keys():
            if self.upload_success[k] == chksums[k]:
                self.upload_success.update({k: True})
                count += 1
        if count == len(self.upload_success):
            validated = True
            logger.info(f"{'--':<20}iRODS & buffer SHA-256 checksum: validated")
        else:
            logger.error(f"{'--':<20}SHA-256 checksum: failed")
            self.irods_client.update_metadata_state(Status.VALIDATE_UPLOAD.value, Status.UPLOAD_CORRUPTED.value)

        return validated

    def _validate_upload(self, resp):
        logger.info(f"{'--':<10}Validate upload")

        self.irods_client.update_metadata_state(Status.VALIDATE_CHECKSUM.value, Status.VALIDATE_UPLOAD.value)
        md5_hexdigest = self.irods_md5.hexdigest()
        logger.info(f"{'--':<20}Buffer MD5: {md5_hexdigest}")

        if resp.status_code == HTTPStatus.OK.value:
            md5_dataverse = resp.json()['data']['files'][0]['dataFile']['md5']
            logger.info(f"{'--':<20}Dataverse MD5: {md5_dataverse}")
            if md5_dataverse == md5_hexdigest:
                logger.info(f"{'--':<30}Checksum MD5 validated")
                self.irods_client.update_metadata_state(Status.VALIDATE_UPLOAD.value, Status.FINALIZE.value)
            else:
                logger.error(f"{'--':<30}Checksum MD5 match: False")
                self.irods_client.update_metadata_state(Status.VALIDATE_UPLOAD.value, Status.UPLOAD_CORRUPTED.value)
        else:
            logger.error(f"{'--':<30}{resp.content.decode('utf-8')}")
            self.irods_client.update_metadata_state(Status.VALIDATE_UPLOAD.value, Status.UPLOAD_FAILED.value)

    def _final_report(self):
        logger.info(f"{'--':<10}Report final progress")
        self.irods_client.add_metadata('externalPID', self.dataset_pid, "Dataverse")
        self.irods_client.update_metadata_state(Status.FINALIZE.value, Status.EXPORTED.value)
        time.sleep(5)
        self.irods_client.remove_metadata(Status.ATTRIBUTE.value, Status.EXPORTED.value)
        logger.info(f"{'--':<10}Export Done")
