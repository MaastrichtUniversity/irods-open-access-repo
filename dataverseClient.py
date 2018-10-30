import base64
import binascii
import hashlib
import json
import logging
import unicodedata
import io
import requests
from irods.rule import Rule

logger = logging.getLogger('iRODS to Dataverse')


class dataverseClient():

    READ_BUFFER_SIZE = 128 * io.DEFAULT_BUFFER_SIZE

    def __init__(self, host, alias, token, irodsclient, md, path):
        self.host = host
        self.alias = alias
        self.token = token
        self.pid = irodsclient.imetadata.pid
        self.md = md
        self.path = path
        self.collection = irodsclient.coll
        self.session = irodsclient.session
        self.dataset_status = None

        split = self.path.split("/")
        self.project = split[3]
        self.collectionID = split[4]

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

    def import_dataset(self):
        print("Map metadata")
        logger.info("Map metadata")

        url = self.host + "/api/dataverses/" + self.alias + "/datasets/:import?pid=hdl:" + self.pid + "&release=no"

        status = self.check_dataset_exist()
        if status == 200:
            print("--\t" + "Dataset already exists, skip import")
            logger.info("--\t" + "Dataset already exists, skip import")
            self.dataset_status = status

        elif status == 404:
            print("Import dataset")
            logger.info("Import dataset")
            resp = requests.post(
                url,
                data=json.dumps(self.md),
                headers={'Content-type': 'application/json',
                         'X-Dataverse-key': self.token},
            )
            self.dataset_status = resp.status_code
            if resp.status_code == 201:
                print("--\t" + "CREATED")
                logger.info("--\t" + "CREATED")
            else:
                logger.error(resp.content)

    def import_files(self, deletion=False):
        url_file = self.host + "/api/datasets/:persistentId/add?persistentId=hdl:" + self.pid

        if self.dataset_status == 404:
            self.upload_file(url_file, deletion)

        elif self.dataset_status == 200:
            self.upload_file(url_file, deletion)

        elif self.dataset_status == 201:
            self.upload_file(url_file, deletion)

        else:
            print("Error skip import_files")
            logger.error("Skip import_files")

    def parse_rule_output(self, out_param_array):
        buff = out_param_array.MsParam_PI[0].inOutStruct.stdoutBuf.buf
        buff = buff.decode('utf-8')
        buf_cleaned = "".join(ch for ch in buff if unicodedata.category(ch)[0] != "C")

        return buf_cleaned

    def rule_open(self):

        print("Rule open")
        logger.info("Rule open")

        open_rule = Rule(self.session, "openProjectCollection.r")
        open_rule.params.update({"*project": "\'" + self.project + "\'"})
        open_rule.params.update({"*projectCollection": "\'" + self.collectionID + "\'"})

        print(open_rule.params)

        open_rule.execute()

    def rule_close(self):

        print("Rule close")
        logger.info("Rule close")

        open_rule = Rule(self.session, "closeProjectCollection.r")
        open_rule.params.update({"*project": "\'" + self.project + "\'"})
        open_rule.params.update({"*projectCollection": "\'" + self.collectionID + "\'"})

        print(open_rule.params)

        open_rule.execute()

    def rule_deletion(self, upload_success):
        print("Rule deletion")
        logger.info("Rule deletion")

        # Check if all the files have been succesfully uploaded before deletion
        if len(upload_success) == len(self.collection.data_objects):
            logger.info("--\t\t\t Start deletion")
            for data in self.collection.data_objects:
                if data.name != "metadata.xml":
                    rule = Rule(self.session, "deleteDataObject.r")
                    rule.params.update({"*project":  "\'"+self.project+"\'"})
                    rule.params.update({"*projectCollection": "\'"+self.collectionID+"\'"})
                    rule.params.update({"*fileName": "\'"+data.name+"\'"})
                    out = self.parse_rule_output(rule.execute())
                    if out == "0":
                        print("--\t\t\t Delete:\t" + data.name)
                        logger.info("--\t\t\t File:\t" + data.name)
                    else:
                        print("**\t\t\t Delete:\t" + data.name)
                        logger.error("--\t\t\t File:\t" + data.name)
                        print("**\t\t\t Delete:\t" + out)
                        logger.error("--\t\t\t File:\t" + out)
            logger.info("--\t\t\t End deletion")
        else:
            print("Deletion skipped. collection.files != uploaded.files")
            logger.info("Deletion skipped. collection.files != uploaded.files")

    def rule_checksum(self, name):
        print("--\t\t\t Rule checksum")
        logger.info("--\t\t\t Rule checksum")

        rule = Rule(self.session, "checksums.r")
        rule.params.update({"*project": "\'" + self.project + "\'"})
        rule.params.update({"*projectCollection": "\'" + self.collectionID + "\'"})
        rule.params.update({"*fileName": "\'" + name + "\'"})

        irods_hash = self.parse_rule_output(rule.execute()).split('sha2:')[1]
        base_hash = base64.b64decode(irods_hash)
        irods_hash_decode = binascii.hexlify(base_hash).decode("utf-8")

        self.session.cleanup()

        return irods_hash_decode

    def chunks(self, f, chunksize=io.DEFAULT_BUFFER_SIZE):
        return iter(lambda: f.read(chunksize), b'')

    # def gen(self, r):
    #     for chunk in r.iter_content(chunk_size=io.DEFAULT_BUFFER_SIZE):
    #         if chunk:
    #             yield chunk

    def upload_file(self, url, deletion=False):

        self.rule_open()
        print("Upload files:")
        logger.info("Upload files:")

        upload_success = {}

        for data in self.collection.data_objects:
            print("--\t" + data.name)
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
            irods_hash_decode = self.rule_checksum(data.name)

            if sha_hexdigest == irods_hash_decode:
                print("--\t\t\t SHA-256 test:\t True")
                logger.info("--\t\t\t SHA-256 test:\t True")

                files = {'file': (data.name, buff_read),
                         'jsonData': '{"description": "My API test description.",'
                                     ' "categories": ["Data"],'
                                     ' "restrict": "false"'
                                     '}'
                         }

                resp = requests.post(
                    url,
                    files=files,
                    headers={'X-Dataverse-key': self.token},
                )
                if resp.status_code == 200:
                    print("--\t\t\t uploaded")
                    logger.info("--\t\t\t uploaded")
                    # print("--\t\t\t" + json.loads(resp.content.decode("utf-8"))['status'])
                    # logger.info(resp.content.decode("utf-8"))
                    md5 = json.loads(resp.content.decode("utf-8"))['data']['files'][0]['dataFile']['md5']
                    # logger.info(md5_hexdigest)
                    # logger.info(md5)
                    if md5 == md5_hexdigest:
                        print("Dataverse check md5:\t True")
                        logger.info("--\t\t\t MD5 test:\t True")
                        upload_success.update({data.name: True})

                elif resp.status_code == 400:
                    print(resp.content.decode("utf-8"))
                    logger.error("--\t\t\t" + json.loads(resp.content.decode("utf-8"))['status'])
                    logger.error("--\t\t\t" + json.loads(resp.content.decode("utf-8"))['message'])
                else:
                    logger.error(resp.content.decode("utf-8"))
                    print(resp.status_code)
                    print("--\t\t\t" + json.loads(resp.content.decode("utf-8"))['status'])

            else:
                print("SHA-256 checksum failed")
                print("Skip upload:\t" + data.name)
                logger.info("SHA-256 checksum failed")
                logger.info("Skip upload:\t" + data.name)

        logger.info(upload_success)
        if deletion:
            self.rule_deletion(upload_success)
        self.rule_close()
        print("Upload Done")
        logger.info("Upload Done")