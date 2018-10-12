import json
import logging
import requests

import os

logger = logging.getLogger('iRODS to Dataverse')


class dataverseClient():

    def __init__(self, host, alias, token, path, pid, md):
        self.host = host
        self.alias = alias
        self.token = token
        self.path = path
        self.pid = pid
        self.md = md
        self.dataset_status = None

    def check_dataset_exist(self):
        print("Check if dataset exists")
        logger.info("Check if dataset exists")
        #
        # pid = meta_dict.get("PID")
        url = self.host + "/api/datasets/:persistentId/?persistentId=hdl:" + self.pid
        resp = requests.get(
            url=url,
            headers={'X-Dataverse-key': self.token},
        )
        # print(resp.status_code)
        logger.info(resp.content)
        return resp.status_code

    def import_dataset(self):
        print("Map metadata")
        logger.info("Map metadata")

        url = self.host + "/api/dataverses/" + self.alias + "/datasets/:import?pid=hdl:" + self.pid + "&release=no"

        status = self.check_dataset_exist()
        # print(status)
        if status == 200:
            print("Dataset already exists, abort import")
            logger.info("Dataset already exists, abort import")
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
        """

        :type status: int
        """

        # pid = meta_dict.get("PID")
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

        for file in os.listdir(self.path):
            print("--\t" + file)
            logger.info("--\t" + file)
            files = {'file': open(self.path + file, 'rb'),
                     'jsonData': '{"description": "My API test description.",'
                                 ' "categories": ["Data"], "restrict": "false"}'}
            resp = requests.post(
                url,
                files=files,
                headers={'X-Dataverse-key': self.token},
            )
            # print(resp.status_code)
            if resp.status_code == 200:
                print("--\t\t\t" + json.loads(resp.content)['status'])
                # logger.info("--\t\t\t" + json.loads(resp.content)['status'])
                logger.info(resp.content)
            if resp.status_code == 400:
                print("--\t\t\t" + json.loads(resp.content)['message'])
                logger.error("--\t\t\t" + json.loads(resp.content)['message'])
            # else:
            #     logger.info(resp.content)
