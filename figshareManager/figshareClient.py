#!/usr/bin/env python

import hashlib
import json

import logging

import requests
from requests.exceptions import HTTPError

logger = logging.getLogger('iRODS to Dataverse')


class FigshareClient:

    def __init__(self, irodsclient, token):
        self.base_url = 'https://api.figshare.com/v2/{endpoint}'
        self.token = token
        # self.chunk_size = 1048576
        self.chunk_size = 1024 * 1048576

        self.irodsclient = irodsclient
        self.md = irodsclient.imetadata
        self.pid = irodsclient.imetadata.pid
        self.collection = irodsclient.coll
        self.session = irodsclient.session
        self.rulemanager = irodsclient.rulemanager

        self.article_id = None

    def raw_issue_request(self, method, url, data=None, binary=False):
        headers = {'Authorization': 'token ' + self.token}
        if data is not None and not binary:
            data = json.dumps(data)
        response = requests.request(method, url, headers=headers, data=data)
        try:
            response.raise_for_status()
            try:
                data = json.loads(response.content)
            except ValueError:
                data = response.content
        except HTTPError as error:
            logger.info('Caught an HTTPError: {}'.format(error.message))
            logger.info('Body:\n' + response.content)
            raise

        return data

    def issue_request(self, method, endpoint, *args, **kwargs):
        return self.raw_issue_request(method, self.base_url.format(endpoint=endpoint), *args, **kwargs)

    def list_articles(self):
        result = self.issue_request('GET', 'account/articles')
        logger.info('Listing current articles:')
        if result:
            for item in result:
                logger.info(u'  {url} - {title}'.format(**item))
        else:
            logger.info('  No articles.')

    def create_article(self):

        data = { # You may add any other information about the article here as you wish.
            'title': self.md.title,
            "description": self.md.description
        }
        result = self.issue_request('POST', 'account/articles', data=data)
        logger.info('Created article:' + result['location'])

        result = self.raw_issue_request('GET', result['location'])
        self.article_id = result['id']

        return result['id']

    def list_files_of_article(self, article_id):
        result = self.issue_request('GET', 'account/articles/{}/files'.format(article_id))
        logger.info('Listing files for article {}:'.format(article_id))
        if result:
            for item in result:
                logger.info('  {id} - {name}'.format(**item))
        else:
            logger.info('  No files.')

    def get_file_check_data(self, file_name):
        with open(file_name, 'rb') as fin:
            md5 = hashlib.md5()
            size = 0
            data = fin.read(self.chunk_size)
            while data:
                size += len(data)
                md5.update(data)
                data = fin.read(self.chunk_size)
            return md5.hexdigest(), size

    def initiate_new_upload(self, article_id, file_name, md5, size):
        endpoint = 'account/articles/{}/files'
        endpoint = endpoint.format(article_id)

        # md5, size = self.get_file_check_data(file_name)
        data = {'name': file_name,
                'md5': md5,
                'size': size}

        result = self.issue_request('POST', endpoint, data=data)
        logger.info('Initiated file upload:' + result['location'])

        result = self.raw_issue_request('GET', result['location'])

        return result

    def complete_upload(self, article_id, file_id):
        self.issue_request('POST', 'account/articles/{}/files/{}'.format(article_id, file_id))

    def get_article(self, article_id):
        result = self.issue_request('GET', 'account/articles/{}'.format(article_id))
        logger.info('Article:' + result['doi'])

    def check_upload(self, article_id, file_id):
        result = self.issue_request('GET', 'account/articles/{}/files/{}'.format(article_id, file_id))

        logger.info("supplied_md5:  "+result['name'] + " " + result['supplied_md5'])
        logger.info("computed_md5:  "+result['name'] + " " + result['computed_md5'])

    def upload_parts(self, file_info, buff):
        url = '{upload_url}'.format(**file_info)
        result = self.raw_issue_request('GET', url)

        logger.info('Uploading parts:')
        logger.info(result['parts'])

        for part in result['parts']:
            logger.info(part)
            self.upload_part(file_info, buff, part)

    def upload_part(self, file_info, stream, part):
        udata = file_info.copy()
        udata.update(part)
        url = '{upload_url}/{partNo}'.format(**udata)

        data = stream[part['startOffset']:part['endOffset']+ 1]

        self.raw_issue_request('PUT', url, data=data, binary=True)
        logger.info('  Uploaded part {partNo} from {startOffset} to {endOffset}'.format(**part))

    def import_files(self):
        logger.info("Upload files:")

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
            size = 0
            for chunk in self.chunks(buff, self.chunk_size):
                size += len(chunk)
                irods_sha.update(chunk)
                irods_md5.update(chunk)
                buff_read = buff_read + chunk

            md5_hexdigest = irods_md5.hexdigest()
            sha_hexdigest = irods_sha.hexdigest()
            irods_hash_decode = self.rulemanager.rule_checksum(data.name)

            if sha_hexdigest == irods_hash_decode and size != 0:
                file_info = self.initiate_new_upload(self.article_id, data.name, md5_hexdigest, size)
                logger.info(file_info)
                # Until here we used the figshare API; following lines use the figshare upload service API.
                self.upload_parts(file_info, buff_read)
                # We return to the figshare API to complete the file upload process.
                self.complete_upload(self.article_id, file_info['id'])
                self.check_upload(self.article_id, file_info['id'])
                logger.info("Figshare: uploaded")

        self.irodsclient.update_metadata_state('exporterState', last_export, 'do-export')

    def chunks(self, f, chunksize):
        return iter(lambda: f.read(chunksize), b'')
