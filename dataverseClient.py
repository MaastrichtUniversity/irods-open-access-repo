
import hashlib
import json
import logging
import io
import requests

logger = logging.getLogger('iRODS to Dataverse')


class DataverseClient:
    """
    Dataverse client to import datasets and files
    """

    READ_BUFFER_SIZE = 128 * io.DEFAULT_BUFFER_SIZE

    HTTP_STATUS_OK = 200
    HTTP_STATUS_Created = 201
    HTTP_STATUS_BadRequest = 400
    HTTP_STATUS_NotFound = 404

    def __init__(self, host, token, alias, irodsclient):
        """

        :param host: String IP of the dataverse's host
        :param token: String token credential
        :param alias: String Alias/ID of the dataverse where to import dataset & files
        :param irodsclient: irodsClient object - client to user iRODS database
        """
        self.host = host
        self.alias = alias
        self.token = token
        self.pid = irodsclient.imetadata.pid
        self.collection = irodsclient.coll
        self.session = irodsclient.session
        self.rulemanager = irodsclient.rulemanager
        self.dataset_status = None

    def check_dataset_exist(self):
        print("Check if dataset exists")
        logger.info("Check if dataset exists")

        url = self.host + "/api/datasets/:persistentId/?persistentId=hdl:" + self.pid
        resp = requests.get(
            url=url,
            headers={'X-Dataverse-key': self.token},
        )
        logger.info(resp.content)
        return resp.status_code

    def import_dataset(self, md):
        url = self.host + "/api/dataverses/" + self.alias + "/datasets/:import?pid=hdl:" + self.pid + "&release=no"

        status = self.check_dataset_exist()
        if status == self.HTTP_STATUS_OK:
            logger.info("--\t" + "Dataset already exists, skip import")
            self.dataset_status = status

        elif status == self.HTTP_STATUS_NotFound:
            logger.info("Import dataset")
            resp = requests.post(
                url,
                data=json.dumps(md),
                headers={'Content-type': 'application/json',
                         'X-Dataverse-key': self.token},
            )
            self.dataset_status = resp.status_code
            if resp.status_code == self.HTTP_STATUS_Created:
                logger.info("--\t" + "CREATED")
            else:
                logger.error(resp.content)

    def import_files(self, deletion=False, restrict=False):
        url_file = self.host + "/api/datasets/:persistentId/add?persistentId=hdl:" + self.pid

        # if self.dataset_status == 404:
        #     self.upload_file(url_file, deletion)

        if self.dataset_status == self.HTTP_STATUS_OK:
            self.import_file(url_file, deletion, restrict)

        elif self.dataset_status == self.HTTP_STATUS_Created:
            self.import_file(url_file, deletion, restrict)

        else:
            logger.error("Skip import_files")

    def import_file(self, url, deletion, restrict):
        self.rulemanager.rule_open()
        logger.info("Upload files:")

        upload_success = {}

        for data in self.collection.data_objects:
            logger.info("--\t" + data.name)

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

                flag = "false"
                if restrict:
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
                    headers={'X-Dataverse-key': self.token},
                )
                if resp.status_code == self.HTTP_STATUS_OK:
                    logger.info("--\t\t\t uploaded")
                    md5 = json.loads(resp.content.decode("utf-8"))['data']['files'][0]['dataFile']['md5']
                    if md5 == md5_hexdigest:
                        logger.info("--\t\t\t MD5 test:\t True")
                        upload_success.update({data.name: True})
                else:
                    logger.error(resp.content.decode("utf-8"))

            else:
                logger.error("SHA-256 checksum failed")
                logger.error("Skip upload:\t" + data.name)

        logger.info(upload_success)
        if deletion:
            self.rulemanager.rule_deletion(upload_success)


    def chunks(self, f, chunksize=io.DEFAULT_BUFFER_SIZE):
        return iter(lambda: f.read(chunksize), b'')
