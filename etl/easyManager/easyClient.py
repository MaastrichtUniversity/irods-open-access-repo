import hashlib
import json
import logging
import requests
import time

from irodsManager.irodsUtils import get_bag_generator, bag_generator_faker
from requests_toolbelt.multipart.encoder import MultipartEncoder
from http import HTTPStatus

logger = logging.getLogger('iRODS to Dataverse')


class EasyClient:
    """
    Dataverse client to import datasets and files
    """

    def __init__(self, host, user, pwd, irodsclient):
        """
        :param host: String IP of the dataverseManager's host
        :param token: String token credential
        :param alias: String Alias/ID of the dataverseManager where to import dataset & files
        :param irodsclient: irodsClient object - client to iRODS database user
        """
        self.host = host
        self.user = user
        self.pwd = pwd

        self.irods_client = irodsclient
        self.pid = irodsclient.imetadata.pid
        self.collection = irodsclient.coll
        self.session = irodsclient.session
        self.rulemanager = irodsclient.rulemanager

        self.dataset_status = None
        self.dataset_url = None
        self.dataset_deposit_url = "https://act.easy.dans.knaw.nl/sword2/collection/1"
        self.dataset_pid = None
        self.last_export = None

        self.upload_success = {}

        self.deletion = False
        self.restrict = False
        self.restrict_list = []

        self.zip_name = "debug_archive.zip"

    from memory_profiler import profile
    @profile
    def post_it(self):

        self.irods_client.update_metadata_state('exporterState', 'prepare-export', 'do-export')
        collection = self.collection
        imetadata = self.irods_client.imetadata
        rulemanager = self.rulemanager

        upload_success = {}
        irods_md5 = hashlib.md5()
        size_bundle = bag_generator_faker(collection, self.session, upload_success,
                                          rulemanager, irods_md5, imetadata)

        print(f"stream size: {size_bundle}")
        md5_hexdigest = irods_md5.hexdigest()
        print(f"{'--':<30}irods buffer MD5: {md5_hexdigest}")

        upload_success = {}
        bundle_md5 = hashlib.md5()
        bundle_iterator = get_bag_generator(collection, self.session, upload_success,
                                            rulemanager, bundle_md5, imetadata, size_bundle)

        print(f"{'--':<30}Post bundle")

        resp = requests.post(
            "https://act.easy.dans.knaw.nl/sword2/collection/1",
            data=bundle_iterator,
            auth=('jmelius', 'Qszxaw369*'),
            headers={
                # 'Content-Type': multipart_encoder.content_type,
                "Content-Disposition": "filename=debug_archive00.zip",
                "Content-MD5": f"{md5_hexdigest}",
                "In-Progress": "false",
                "Packaging": "http://purl.org/net/sword/package/SimpleZip",
                # "Content-Type": "application/zip"
                "Content-Type": "application/octet-stream"
            },
        )
        bundle_md5_hexdigest = bundle_md5.hexdigest()
        print(f"{'--':<30}request buffer MD5: {bundle_md5_hexdigest}")
        print(f"{'--':<30}status_code: {resp.status_code}")
        if resp.status_code == 201:
            print(f"{'--':<30}{resp.content.decode('utf-8')}")
        else:
            print(f"{'--':<30}{resp.content.decode('utf-8')}")

        # path = "/nlmumc/projects/P000000005/C000000001"
        # collection = session.collections.get(path)
        # upload_success = {}
        # data, size = collection_zip_preparation(collection, rulemanager, upload_success)
        # stream = UnseekableStream()
        # zip_iterator = zip_collection(data, stream, session, upload_success, imetadata)
        # bar = tqdm(total=size_bundle, unit="bytes", smoothing=0.1, unit_scale=True)
        # bundle_md5 = hashlib.md5()
        # bundle_iterator = IteratorAsBinaryFile(size_bundle, archive_generator2(zip_iterator, stream, bar), bundle_md5)
        #
        # print(f"{'--':<30}Post bundle")
        #
        # resp = requests.post(
        #     "https://act.easy.dans.knaw.nl/sword2/collection/1",
        #     data=bundle_iterator,
        #     auth=('jmelius', 'Qszxaw369*'),
        #     headers={
        #              # 'Content-Type': multipart_encoder.content_type,
        #              "Content-Disposition": "filename=debug_archive00.zip",
        #              "Content-MD5": f"{md5_hexdigest}",
        #              "In-Progress": "false",
        #              "Packaging": "http://purl.org/net/sword/package/SimpleZip",
        #              # "Content-Type": "application/zip"
        #              "Content-Type": "application/octet-stream"
        #              },
        # )
        # bundle_md5_hexdigest = bundle_md5.hexdigest()
        # print(f"{'--':<30}request buffer MD5: {bundle_md5_hexdigest}")
        # print(f"{'--':<30}status_code: {resp.status_code}")
        # if resp.status_code == 201:
        #     print(f"{'--':<30}{resp.content.decode('utf-8')}")
        # else:
        #     print(f"{'--':<30}{resp.content.decode('utf-8')}")

    def final_report(self):
        logger.info("Report final progress")
        # self.irods_client.add_metadata_state('externalPID', self.dataset_pid, "Easy")
        self.irods_client.update_metadata_state('exporterState', 'do-export', 'exported')
        time.sleep(5)
        self.irods_client.remove_metadata_state('exporterState', 'exported')
        logger.info("Upload Done")
