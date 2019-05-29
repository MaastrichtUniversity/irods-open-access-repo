import hashlib
import json
import logging
import requests
import time
import io
import tarfile

from io import BytesIO
from tqdm import tqdm
from requests_toolbelt.multipart.encoder import MultipartEncoder, CustomBytesIO, encode_with
from requests.utils import super_len
from http import HTTPStatus

logger = logging.getLogger('iRODS to Dataverse')


class MultiPurposeReader:
    """I wish I was a real file"""

    def __init__(self, buffer, length, md5, sha, bar):
        self.len = None if length is None else int(length)
        self._raw = buffer
        self.md5 = md5
        self.sha = sha
        self.bar = bar

    def read(self, chunk_size):

        force_size = 1024 * io.DEFAULT_BUFFER_SIZE

        if chunk_size == -1 or chunk_size == 0 or chunk_size > force_size:
            chunk = self._raw.read(chunk_size) or b''
        else:
            chunk = self._raw.read(force_size) or b''

        self.len -= len(chunk)

        if not chunk:
            self.len = 0

        self.md5.update(chunk)
        self.sha.update(chunk)
        self.bar.update(len(chunk))
        return chunk


class IteratorAsBinaryFile(object):
    def __init__(self, size, iterator, md5, encoding='utf-8'):
        #: The expected size of the upload
        self.size = int(size)
        self.md5 = md5
        if self.size < 0:
            raise ValueError(
                'The size of the upload must be a positive integer'
            )

        #: Attribute that requests will check to determine the length of the
        #: body. See bug #80 for more details
        self.len = self.size
        #: The iterator used to generate the upload data
        self.iterator = iterator

        #: Encoding the iterator is using
        self.encoding = encoding

        # The buffer we use to provide the correct number of bytes requested
        # during a read
        self._buffer = CustomBytesIO()

    def _get_bytes(self):
        try:
            return encode_with(next(self.iterator), self.encoding)
        except StopIteration:
            return b''

    def _load_bytes(self, size):
        self._buffer.smart_truncate()
        amount_to_load = size - super_len(self._buffer)
        bytes_to_append = True

        while amount_to_load > 0 and bytes_to_append:
            bytes_to_append = self._get_bytes()
            amount_to_load -= self._buffer.append(bytes_to_append)

    def read(self, size=-1):
        size = int(size)
        if size == -1:
            return b''.join(self.iterator)

        self._load_bytes(size)
        s = self._buffer.read(size)
        if not s:
            self.len = 0

        if size < 0:
            self.len = 0

        self.md5.update(encode_with(s, self.encoding))
        return encode_with(s, self.encoding)


class FileStream(object):
    def __init__(self):
        self.buffer = BytesIO()
        self.offset = 0

    def write(self, s):
        self.buffer.write(s)
        self.offset += len(s)

    def tell(self):
        return self.offset

    def close(self):
        self.buffer.close()

    def pop(self):
        s = self.buffer.getvalue()
        self.buffer.close()

        self.buffer = BytesIO()
        return s


def tar_generator(func, streaming_fp, bar):
    try:
        for i in func:
            s = streaming_fp.pop()
            if len(s) > 0:
                bar.update(len(s))
                yield s
    except StopIteration:
        return b''


def calc_tar_block(nb):
    if nb < 512:
        return 512
    if divmod(nb, 512)[1] != 0:
        return nb + 512
    else:
        return nb


BLOCK_SIZE = 1024 * io.DEFAULT_BUFFER_SIZE


# https://gist.github.com/chipx86/9598b1e4a9a1a7831054
def stream_build_tar(tar_name, collection, data, streaming_fp, session, upload_success):
    tar = tarfile.TarFile.open(tar_name, 'w|', streaming_fp)
    yield

    for f in data:
        filepath = f.path.replace(collection.path, '')
        tar_info = tarfile.TarInfo(filepath)

        tar_info.size = f.size
        tar.addfile(tar_info)

        buff = session.data_objects.open(f.path, 'r')

        irods_sha = hashlib.sha256()

        while True:
            s = buff.read(BLOCK_SIZE)
            if len(s) > 0:
                tar.fileobj.write(s)
                irods_sha.update(s)
                yield

            if len(s) < BLOCK_SIZE:
                blocks, remainder = divmod(tar_info.size, tarfile.BLOCKSIZE)

                if remainder > 0:
                    tar.fileobj.write(tarfile.NUL * (tarfile.BLOCKSIZE - remainder))
                    yield
                    blocks += 1

                tar.offset += blocks * tarfile.BLOCKSIZE
                break
        buff.close()

        sha_hexdigest = irods_sha.hexdigest()
        logger.info(f"{'--':<30}buffer {f.name} SHA: {sha_hexdigest}")
        if upload_success.get(f.path) == sha_hexdigest:
            logger.info(f"{'--':<30}SHA-256 {f.name}  match: True")
            upload_success.update({f.path: True})

    tar.close()
    yield


class DataverseClient:
    """
    Dataverse client to import datasets and files
    """
    READ_BUFFER_SIZE = 1024 * 1048576

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

    def create_dataset(self, md, data_export=False):
        logger.info(f"{'--':<10}Dataset - request creation")
        self.irods_client.update_metadata_state('exporterState', 'prepare-export', 'do-export')
        url = f"{self.host}/api/dataverses/{self.alias}/datasets/"

        resp = requests.post(
            url,
            data=json.dumps(md),
            headers={'Content-type': 'application/json',
                     'X-Dataverse-key': self.token},
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
        self.restrict_list = restrict_list

        if self.dataset_status == HTTPStatus.CREATED.value:
            self.import_collection()
            self.final_report()
        else:
            logger.error(f"{'--':<20}Skip import_files")
            self.irods_client.remove_metadata_state('exporterState', 'do-export')

    def import_collection(self):
        logger.info(f"{'--':<10}Upload files")
        restrict_list = self.restrict_list.split(", ")
        logger.info(f"{'--':<20}restrict_list{restrict_list}")
        self.irods_client.update_metadata_state('exporterState', 'do-export', 'do-export-start')
        self.last_export = 'do-export-start'

        # for data in self.collection.data_objects:
        #     self.import_file(data, "")

        # for col in self.collection.subcollections:
        #     self.import_sub_collection(col, "")

        data = []
        size = 0
        for coll, sub, files in self.collection.walk():
            for file in files:
                data.append(file)
                size += calc_tar_block(file.size) + 512

                irods_hash_decode = self.rulemanager.rule_checksum(file.path)
                logger.info(f"{'--':<30}iRODS {file.name} SHA-256: {irods_hash_decode}")
                self.upload_success.update({file.path: irods_hash_decode})

        blocks, remainder = divmod(size, 10240)
        if remainder == 0:
            size = 10240 * blocks
        elif remainder > 0:
            size = 10240 * blocks + 10240

        self.import_tar_collection(data, size)

        self.irods_client.update_metadata_state('exporterState', self.last_export, 'do-export')
        logger.info(f"{'--':<10} Upload success: {repr(self.upload_success)}")

        if self.deletion:
            self.rulemanager.rule_deletion(self.upload_success)

    def import_tar_collection(self, data, size):
        flag = "false"
        if self.restrict:
            flag = "true"

        streaming_fp = FileStream()
        irods_md5 = hashlib.md5()

        json_data = {"description": "My API test description.",
                     "categories": ["Data"],
                     "restrict": flag
                     }

        m = MultipartEncoder(
            fields={
                'jsonData': json.dumps(json_data),
                'file': (
                    self.tar_name,
                    IteratorAsBinaryFile(
                        size,
                        tar_generator(
                            stream_build_tar(self.tar_name,
                                             self.collection,
                                             data,
                                             streaming_fp,
                                             self.session,
                                             self.upload_success),
                            streaming_fp,
                            tqdm(total=size, unit="bytes", smoothing=0.1, unit_scale=True)
                        ),
                        irods_md5
                    )
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
        logger.info(f"{'--':<30}buffer MD5: {md5_hexdigest}")

        if resp.status_code == HTTPStatus.OK.value:
            md5 = json.loads(resp.content.decode("utf-8"))['data']['files'][0]['dataFile']['md5']
            logger.info(f"{'--':<30}Dataverse MD5: {md5}")
            if md5 == md5_hexdigest:
                logger.info(f"{'--':<30}Dataverse MD5 match: True")
            else:
                logger.error(f"{'--':<30}Dataverse MD5 match: False")
                logger.error(f"{'--':<30}SHA-256 checksum failed")
                logger.error(f"{'--':<30}Upload corrupted: {data}")
        else:
            logger.error(f"{'--':<30}{resp.content.decode('utf-8')}")

    def import_sub_collection(self, coll, name):
        logger.info(f"{'--':<20}{coll.path}")
        sub_name = name + ".." + coll.name
        for data in coll.data_objects:
            self.import_file(data, sub_name)
        for sub in coll.subcollections:
            self.import_sub_collection(sub, sub_name)

    # @profile
    def import_file(self, data, coll_name):
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
                                            irods_sha,
                                            tqdm(total=data.size, unit="bytes", smoothing=0.1, unit_scale=True))
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
