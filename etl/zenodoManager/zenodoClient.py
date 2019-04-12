
import hashlib
import json
import logging
import io
from builtins import print

import requests

logger = logging.getLogger('iRODS to Dataverse')


class ZenodoClient:
    """
    Zenodo client to import datasets and files
    """

    # READ_BUFFER_SIZE = 1024 * io.DEFAULT_BUFFER_SIZE
    READ_BUFFER_SIZE = 1024 *  1048576
    # READ_BUFFER_SIZE = 65536
    HTTP_STATUS_OK = 200
    HTTP_STATUS_Created = 201
    HTTP_STATUS_BadRequest = 400
    HTTP_STATUS_NotFound = 404

    def __init__(self, irodsclient, token):
        """

        :param host: String IP of the dataverseManager's host
        :param token: String token credential
        :param alias: String Alias/ID of the dataverseManager where to import dataset & files
        :param irodsclient: irodsClient object - client to user iRODS database
        """
        self.host = "https://sandbox.zenodo.org"
        self.token = token
        self.irodsclient = irodsclient
        self.pid = irodsclient.imetadata.pid
        self.collection = irodsclient.coll
        self.session = irodsclient.session
        self.rulemanager = irodsclient.rulemanager
        self.deposition_status = None
        self.deposition_id = None

        self.access = {'access_token': self.token}

    def create_deposit(self, md):
        headers = {"Content-Type": "application/json"}

        r = requests.post(self.host + '/api/deposit/depositions',
                          params=self.access, data=json.dumps(md),
                          headers=headers)

        self.deposition_status = r.status_code
        logger.info(self.deposition_status)
        #201
        logger.info(r.json())
        self.deposition_id = r.json()['id']
        pass

    def import_files(self):

        logger.info("Upload files:")

        upload_success = {}

        self.irodsclient.update_metadata_state('exporterState', 'do-export', 'do-export-start')

        last_export = 'do-export-start'
        for data in self.collection.data_objects:
            logger.info("--\t" + data.name)

            self.irodsclient.update_metadata_state('exporterState', last_export, 'do-export-' + data.name)
            last_export = 'do-export-' + data.name

            buff = self.session.data_objects.open(data.path, 'r')
            irods_sha = hashlib.sha256()
            irods_md5 = hashlib.md5()
            buff_read = bytes()
            for chunk in self.chunks(buff, self.READ_BUFFER_SIZE):
                irods_sha.update(chunk)
                irods_md5.update(chunk)
                buff_read = buff_read + chunk

            md5_hexdigest = irods_md5.hexdigest()
            sha_hexdigest = irods_sha.hexdigest()
            irods_hash_decode = self.rulemanager.rule_checksum(data.name)

            if sha_hexdigest == irods_hash_decode:
                logger.info("--\t\t\t SHA-256 test:\t True")
                request_data = {'filename': data.name}
                files = {'file': (data.name, buff_read)}
                r = requests.post(self.host + '/api/deposit/depositions/%s/files' % self.deposition_id,
                                  params=self.access, data=request_data,
                                  files=files)
                logger.info(r.status_code)
                logger.info(r.json())

        self.irodsclient.update_metadata_state('exporterState', last_export, 'do-export')
        pass

    def chunks(self, f, chunksize=io.DEFAULT_BUFFER_SIZE):
        return iter(lambda: f.read(chunksize), b'')
