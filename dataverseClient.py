import json
import logging
import requests

logger = logging.getLogger('iRODS to Dataverse')


class dataverseClient():

    def __init__(self, host, alias, token, irodsclient, md):
        self.host = host
        self.alias = alias
        self.token = token
        self.pid = irodsclient.imetadata.pid
        self.md = md
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

    def upload_file(self, url):
        print("Upload files:")
        logger.info("Upload files:")

        for data in self.collection.data_objects:
            buff = self.session.data_objects.open(data.path, 'r')

            print("--\t" + data.name)
            logger.info("--\t" + data.name)
            files = {'file': (data.name, buff),
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
            elif resp.status_code == 400:
                print("--\t\t\t" + json.loads(resp.content)['message'])
                logger.error("--\t\t\t" + json.loads(resp.content)['message'])
            else:
                print(resp.status_code)
                print("--\t\t\t" + json.loads(resp.content)['status'])
                logger.info(resp.content)
