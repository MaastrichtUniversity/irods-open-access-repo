import base64
import binascii
import hashlib
import json
import logging
import unicodedata
import io
import requests
from irods.rule import Rule
from requests.auth import HTTPBasicAuth

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
            print("--\t" + "Dataset already exists, abort import")
            logger.info("--\t" + "Dataset already exists, abort import")
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
            if resp.status_code == 201:
                print("--\t" + "CREATED")
                self.dataset_status = resp.status_code
            else:
                logger.info(resp.content)
                self.dataset_status = resp.status_code

    def import_files(self):
        url_file = self.host + "/api/datasets/:persistentId/add?persistentId=hdl:" + self.pid

        if self.dataset_status == 404:
            self.upload_file(url_file)
            print("Upload Done")
            logger.info("Upload Done")

        elif self.dataset_status == 200:
            self.upload_file(url_file)

        elif self.dataset_status == 201:
            self.upload_file(url_file)

        else:
            print("Abort import_files")
            logger.info("Abort import_files")

    def parse_rule_output(self, out_param_array):
        buff = out_param_array.MsParam_PI[0].inOutStruct.stdoutBuf.buf
        buff = buff.decode('utf-8')
        buf_cleaned = "".join(ch for ch in buff if unicodedata.category(ch)[0] != "C")

        return buf_cleaned

    def rule_open(self):
        split = self.path.split("/")
        project = split[3]
        collectionID = split[4]

        print("Rule open")
        logger.info("Rule open")

        open_rule = Rule(self.session, "openProjectCollection.r")
        open_rule.params.update({"*project": "\'" + project + "\'"})
        open_rule.params.update({"*projectCollection": "\'" + collectionID + "\'"})

        print(open_rule.params)

        open_rule.execute()

    def rule_close(self):
        split = self.path.split("/")
        project = split[3]
        collectionID = split[4]

        print("Rule close")
        logger.info("Rule close")

        open_rule = Rule(self.session, "closeProjectCollection.r")
        open_rule.params.update({"*project": "\'" + project + "\'"})
        open_rule.params.update({"*projectCollection": "\'" + collectionID + "\'"})

        print(open_rule.params)

        open_rule.execute()

    def chunks(self, f, chunksize=io.DEFAULT_BUFFER_SIZE):
        return iter(lambda: f.read(chunksize), b'')

    def gen(self, r):
        for chunk in r.iter_content(chunk_size=io.DEFAULT_BUFFER_SIZE):
            if chunk:
                yield chunk

    def upload_file(self, url):
        print("Upload files:")
        logger.info("Upload files:")
        self.rule_open()

        for data in self.collection.data_objects:
            buff = self.session.data_objects.open(data.path, 'r')

            irods_sha = hashlib.sha256()
            irods_md5 = hashlib.md5()

            buff_read = bytes()
            for chunk in self.chunks(buff, self.READ_BUFFER_SIZE):

                irods_sha.update(chunk)
                irods_md5.update(chunk)

                buff_read = buff_read + chunk

            split = data.path.split("/")
            project = split[3]
            collectionID = split[4]

            print("Rule checksum")
            logger.info("Rule checksum")

            cksRule = Rule(self.session, "checksums.r")
            cksRule.params.update({"*project":  "\'"+project+"\'"})
            cksRule.params.update({"*projectCollection": "\'"+collectionID+"\'"})
            cksRule.params.update({"*fileName": "\'"+data.name+"\'"})

            irods_hash = self.parse_rule_output(cksRule.execute()).split('sha2:')[1]
            # print(irods_hash)
            base_hash = base64.b64decode(irods_hash)
            irods_hash_decode = binascii.hexlify(base_hash).decode("utf-8")

            # print(irods_hash_decode)

            sha = irods_sha.hexdigest()
            print(sha)

            print("SHA-256 test:\t", sha == irods_hash_decode)
            print("iRODS md5:\t", irods_md5.hexdigest())

            self.session.cleanup()

            print("--\t" + data.name)
            logger.info("--\t" + data.name)

            files = {'file': (data.name, buff_read),
                     'jsonData': '{"description": "My API test description.",'
                                 ' "categories": ["Data"], "restrict": "false"}'}

            resp = requests.post(
                url,
                files=files,
                headers={'X-Dataverse-key': self.token},
            )
            if resp.status_code == 200:
                print("--\t\t\t" + json.loads(resp.content)['status'])
                logger.info(resp.content)
                md5 = json.loads(resp.content)['data']['files'][0]['dataFile']['md5']
                print(md5)
                print("Dataverse check md5:\t", md5 == irods_md5.hexdigest())
                if md5 == irods_md5.hexdigest() and data.name != "metadata.xml":
                    rule = Rule(self.session, "deleteDataObject.r")
                    rule.params.update({"*project":  "\'"+project+"\'"})
                    rule.params.update({"*projectCollection": "\'"+collectionID+"\'"})
                    rule.params.update({"*fileName": "\'"+data.name+"\'"})
                    out = self.parse_rule_output(rule.execute())
                    if out == 0:
                        print("File:\t" + data.name + "\t deleted")
            elif resp.status_code == 400:
                print("--\t\t\t" + json.loads(resp.content)['message'])
                logger.error("--\t\t\t" + json.loads(resp.content)['message'])
            else:
                logger.info(resp.content)
                print(resp.status_code)
                print("--\t\t\t" + json.loads(resp.content)['status'])

        self.rule_close()