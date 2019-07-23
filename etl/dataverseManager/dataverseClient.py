import hashlib
import json
import logging
import requests
import time

from irodsManager.irodsUtils import get_zip_generator, zip_generator_faker, MultiPurposeReader
from requests_toolbelt.multipart.encoder import MultipartEncoder
from http import HTTPStatus

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

        self.deletion = False
        self.restrict = False
        self.restrict_list = []

        self.tar_name = "test_sub_collection.tar"
        self.zip_name = "test_sub_collection"

    def create_dataset(self, md, data_export=False):
        logger.info(f"{'--':<10}Dataset - request creation")
        self.irods_client.update_metadata_state('exporterState', 'prepare-export', 'do-export')
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
            self.final_report()

    def import_files(self, deletion=False, restrict=False, restrict_list=''):
        self.dataset_deposit_url = f"{self.host}/api/datasets/:persistentId/add?persistentId={self.dataset_pid}" \
            f"&key={self.token}"
        self.deletion = deletion
        self.restrict = restrict
        self.restrict_list = restrict_list.split(", ")

        if self.dataset_status == HTTPStatus.CREATED.value:
            self.import_collection()
            self.final_report()
        else:
            logger.error(f"{'--':<20}Skip import_files")
            self.irods_client.remove_metadata_state('exporterState', 'do-export')

    def import_collection(self):
        logger.info(f"{'--':<10}Upload files")
        logger.info(f"{'--':<20}restrict_list{self.restrict_list}")
        self.irods_client.update_metadata_state('exporterState', 'do-export', 'do-export-start')
        self.last_export = 'do-export-start'

        self.import_bundle_collection()

        self.irods_client.update_metadata_state('exporterState', self.last_export, 'do-export')
        logger.info(f"{'--':<10} Upload success: {repr(self.upload_success)}")

        if self.deletion:
            self.rulemanager.rule_deletion(self.upload_success)

    def import_bundle_collection(self):
        flag = "false"
        if self.restrict:
            flag = "true"

        irods_md5 = hashlib.md5()
        size_bundle = zip_generator_faker(self.collection, self.session,
                                          self.upload_success, self.rulemanager,
                                          irods_md5)
        md5_hexdigest = irods_md5.hexdigest()
        logger.info(f"{'--':<30}buffer faker MD5: {md5_hexdigest}")

        irods_md5 = hashlib.md5()
        bundle_iterator = get_zip_generator(self.collection, self.session,
                                            self.upload_success, self.rulemanager,
                                            irods_md5, size_bundle)

        json_data = {"description": "My API test description.",
                     "categories": ["Data"],
                     "restrict": flag
                     }

        multipart_encoder = MultipartEncoder(
            fields={'jsonData': json.dumps(json_data),
                    'file': (self.zip_name, bundle_iterator)
                    }
        )
        logger.info(f"{'--':<30}Post bundle")
        resp = requests.post(
            self.dataset_deposit_url,
            data=multipart_encoder,
            headers={'Content-Type': multipart_encoder.content_type},
        )

        logger.info(f"{'--':<30}calculate checksum")
        md5_hexdigest = irods_md5.hexdigest()
        logger.info(f"{'--':<30}buffer MD5: {md5_hexdigest}")

        if resp.status_code == HTTPStatus.OK.value:
            md5 = json.loads(resp.content.decode("utf-8"))['data']['files'][0]['dataFile']['md5']
            logger.info(f"{'--':<30}Dataverse MD5: {md5}")
            if md5 == md5_hexdigest:
                logger.info(f"{'--':<30}Dataverse MD5 match: True")
            else:
                logger.error(f"{'--':<30}Dataverse MD5 match: False")
                logger.error(f"{'--':<30}SHA-256 checksum failed")
        else:
            logger.error(f"{'--':<30}{resp.content.decode('utf-8')}")
            # http status 400 - bad request the request sent by the client was syntactically incorrect
            # check incorrect size prediction

    def import_sub_collection(self, coll):
        logger.info(f"{'--':<20}{coll.path}")
        for data in coll.data_objects:
            self.import_file(data)
        for sub in coll.subcollections:
            self.import_sub_collection(sub)

    def import_file(self, data):
        logger.info(f"{'--':<20}{data.name}")

        display_name = data.name
        if len(display_name) > 10:
            display_name = data.name[:10] + "..."

        self.irods_client.update_metadata_state('exporterState', self.last_export, 'do-export-' + display_name)
        self.last_export = 'do-export-' + display_name

        irods_sha = hashlib.sha256()
        irods_md5 = hashlib.md5()

        logger.info(f"{'--':<30}Query iRODS checksum")
        irods_hash_decode = self.rulemanager.rule_checksum(data.path)
        logger.info(f"{'--':<30}iRODS SHA-256: {irods_hash_decode}")

        min_path = data.path.replace("/nlmumc/projects/", "")

        flag = "false"
        if self.restrict:
            flag = "true"
        elif len(self.restrict_list) >= 0 and min_path in self.restrict_list:
            flag = "true"

        json_data = {"description": "My API test description.",
                     "categories": ["Data"],
                     "restrict": flag
                     }

        m = MultipartEncoder(
            fields={
                'jsonData': json.dumps(json_data),
                'file': (data.name,
                         MultiPurposeReader(self.session.data_objects.open(data.path, 'r'),
                                            data.size,
                                            irods_md5,
                                            irods_sha)
                         )
            }
        )
        logger.info(f"{'--':<30}Post file")
        resp = requests.post(
            self.dataset_deposit_url,
            data=m,
            headers={
                'Content-Type': m.content_type
            },
        )

        logger.info(f"{'--':<30}calculate checksum")
        md5_hexdigest = irods_md5.hexdigest()
        sha_hexdigest = irods_sha.hexdigest()
        logger.info(f"{'--':<30}buffer MD5: {md5_hexdigest}")
        logger.info(f"{'--':<30}buffer SHA: {sha_hexdigest}")

        if resp.status_code == HTTPStatus.OK.value:
            md5 = json.loads(resp.content.decode("utf-8"))['data']['files'][0]['dataFile']['md5']
            logger.info(f"{'--':<30}Dataverse MD5: {md5}")
            if md5 == md5_hexdigest and sha_hexdigest == irods_hash_decode:
                logger.info(f"{'--':<30}Dataverse MD5 match: True")
                logger.info(f"{'--':<30}SHA-256 match: True")
                self.upload_success.update({data.name: True})
            else:
                logger.error(f"{'--':<30}Dataverse MD5 match: False")
                logger.error(f"{'--':<30}SHA-256 checksum failed")
                logger.error(f"{'--':<30}Upload corrupted: {data.name}")
        else:
            logger.error(f"{'--':<30}{resp.content.decode('utf-8')}")

    def final_report(self):
        logger.info("Report final progress")
        self.irods_client.add_metadata_state('externalPID', self.dataset_pid, "Dataverse")
        self.irods_client.update_metadata_state('exporterState', 'do-export', 'exported')
        time.sleep(5)
        self.irods_client.remove_metadata_state('exporterState', 'exported')
        logger.info("Upload Done")
