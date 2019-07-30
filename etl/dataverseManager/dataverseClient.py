import hashlib
import json
import logging
import requests
import time

from irodsManager.irodsUtils import get_zip_generator, zip_generator_faker
from irodsManager.irodsRuleManager import RuleManager

from requests_toolbelt.multipart.encoder import MultipartEncoder
from http import HTTPStatus
from multiprocessing import Pool


logger = logging.getLogger('iRODS to Dataverse')


class DataverseClient:
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
        self.pid = irodsclient.imetadata.pid
        self.collection = irodsclient.coll
        self.session = irodsclient.session
        self.rulemanager = irodsclient.rulemanager

        self.dataset_status = None
        self.dataset_url = None
        self.dataset_deposit_url = None
        self.dataset_pid = None
        self.last_export = None

        self.upload_success = {}
        self.upload_status_code = None

        self.deletion = False
        self.restrict = False
        self.restrict_list = []

        self.tar_name = "test_sub_collection.tar"
        self.zip_name = f"test_{irodsclient.imetadata.title}"

    def create_dataset(self, md, data_export=False):
        logger.info(f"{'--':<10}Dataset - request creation")
        self.irods_client.update_metadata_state('exporterState', 'create-exporter', 'create-dataset')
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
            self.dataset_pid = json.loads(resp.content.decode("utf-8"))['data']['persistentId']
            self.dataset_url = f"{self.host}/dataset.xhtml?persistentId={self.dataset_pid}&version=DRAFT"
            logger.info(f"{'--':<20}Dataset created with pid: {self.dataset_pid}")
        else:
            logger.error(resp.content)

        if not data_export and self.dataset_status == HTTPStatus.CREATED.value:
            self.irods_client.update_metadata_state('exporterState', 'create-dataset', 'finalize')
            self.final_report()

    def import_files(self, deletion=False, restrict=False, restrict_list=''):
        self.dataset_deposit_url = f"{self.host}/api/datasets/:persistentId/add?persistentId={self.dataset_pid}" \
            f"&key={self.token}"
        self.deletion = deletion
        self.restrict = restrict
        if len(restrict_list) > 0:
            self.restrict_list = restrict_list.split(", ")

        if self.dataset_status == HTTPStatus.CREATED.value:
            self.irods_client.update_metadata_state('exporterState', 'create-dataset', 'prepare-collection')
            self.import_zip_collection()
            if self.deletion:
                self.rulemanager.rule_deletion(self.upload_success)
            if self.upload_status_code:
                self.final_report()
        else:
            logger.error(f"{'--':<20}Wrong dataset status")
            self.irods_client.update_metadata_state('exporterState', 'create-dataset', 'failed-dataset-creation')

    def import_zip_collection(self):
        flag = "false"
        if self.restrict:
            flag = "true"

        pool = Pool(processes=1)
        result = pool.apply_async(self.run_checksum, [self.collection.path])

        logger.info(f"{'--':<10}Prepare zip")

        self.irods_client.update_metadata_state('exporterState', 'prepare-collection', 'zip-collection')
        irods_md5 = hashlib.md5()
        size_bundle = zip_generator_faker(self.collection, self.session,
                                          self.upload_success, self.rulemanager,
                                          irods_md5, self.restrict_list)
        md5_hexdigest = irods_md5.hexdigest()
        logger.info(f"{'--':<20}Buffer faker MD5: {md5_hexdigest}")

        irods_md5 = hashlib.md5()
        bundle_iterator = get_zip_generator(self.collection, self.session,
                                            self.upload_success, self.rulemanager,
                                            irods_md5, size_bundle, self.restrict_list)

        json_data = {"description": "My API test description.",
                     "categories": ["Data"],
                     "restrict": flag
                     }

        multipart_encoder = MultipartEncoder(
            fields={'jsonData': json.dumps(json_data),
                    'file': (self.zip_name, bundle_iterator)
                    }
        )

        logger.info(f"{'--':<10}Upload zip")

        self.irods_client.update_metadata_state('exporterState', 'zip-collection', 'upload-zipped-collection')
        resp = requests.post(
            self.dataset_deposit_url,
            data=multipart_encoder,
            headers={'Content-Type': multipart_encoder.content_type},
        )

        logger.info(f"{'--':<10}Validate upload")

        self.irods_client.update_metadata_state('exporterState', 'upload-zipped-collection', 'validate-upload')
        pool.close()
        pool.join()
        chksums = result.get()
        count = 0
        for k in self.upload_success.keys():
            if self.upload_success[k] == chksums[k]:
                self.upload_success.update({k: True})
                count += 1
        if count == len(self.upload_success):
            self.upload_status_code = True
            logger.info(f"{'--':<20}iRODS & buffer SHA-256 checksum: validated")
        else:
            self.upload_status_code = False
            logger.error(f"{'--':<20}SHA-256 checksum: failed")

        md5_hexdigest = irods_md5.hexdigest()
        logger.info(f"{'--':<20}Buffer MD5: {md5_hexdigest}")

        if resp.status_code == HTTPStatus.OK.value:
            md5_dataverse = json.loads(resp.content.decode("utf-8"))['data']['files'][0]['dataFile']['md5']
            logger.info(f"{'--':<20}Dataverse MD5: {md5_dataverse}")
            if md5_dataverse == md5_hexdigest:
                logger.info(f"{'--':<30}Checksum MD5 match: True")
                self.irods_client.update_metadata_state('exporterState', 'validate-upload', 'finalize')
            else:
                logger.error(f"{'--':<30}Checksum MD5 match: False")
                self.irods_client.update_metadata_state('exporterState', 'validate-upload', 'upload-corrupted')
        else:
            logger.error(f"{'--':<30}{resp.content.decode('utf-8')}")
            self.irods_client.update_metadata_state('exporterState', 'validate-upload', 'upload-failed')

    def final_report(self):
        logger.info(f"{'--':<10}Report final progress")
        self.irods_client.add_metadata_state('externalPID', self.dataset_pid, "Dataverse")
        self.irods_client.update_metadata_state('exporterState', 'finalize', 'exported')
        time.sleep(5)
        self.irods_client.remove_metadata_state('exporterState', 'exported')
        logger.info(f"{'--':<10}Export Done")

    @staticmethod
    def run_checksum(path):
        return RuleManager.rule_collection_checksum(path)
