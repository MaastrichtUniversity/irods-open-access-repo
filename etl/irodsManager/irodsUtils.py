import hashlib
import io
import re
import zipfile
import logging
import datetime

from irodsManager.irodsRuleManager import RuleManager

from enum import Enum
from tqdm import tqdm
from io import RawIOBase
from requests_toolbelt.multipart.encoder import CustomBytesIO, encode_with
from requests.utils import super_len
from xml.etree import ElementTree

logger = logging.getLogger('iRODS to Dataverse')
BLOCK_SIZE = 1024 * io.DEFAULT_BUFFER_SIZE

date_iso = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc).isoformat()
date = datetime.datetime.now().date()

debug = False
if logging.getLogger().isEnabledFor(logging.DEBUG):
    debug = True


class IteratorAsBinaryFile(object):
    """Custom bundle iterator for streaming.
    <requests_toolbelt.streaming_iterator>
    """

    def __init__(self, size, iterator, md5, encoding='utf-8'):
        #: The expected size of the upload
        self.size = int(size)
        self.md5 = md5
        if self.size < 0:
            raise ValueError('The size of the upload must be a positive integer')

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

        self.md5.update(s)
        return s


class UnseekableStream(RawIOBase):
    """Custom raw buffer for streaming.
    """

    def __init__(self):
        self._buffer = b''

    def writable(self):
        return True

    def write(self, b):
        if self.closed:
            raise ValueError('Stream was closed!')
        self._buffer += b
        return len(b)

    def get(self):
        chunk = self._buffer
        self._buffer = b''
        return chunk


def archive_generator_faker(func, stream, bar, irods_md5):
    """Go through the zip generator and return the size.

    :param irods_md5: md5 checksum hash
    :param func: archive buffer iterator
    :param stream: raw buffer stream
    :param bar: progress monitor
    :return: int - size
    """

    try:
        size = 0
        if debug:
            with open("debug_archive0.zip", 'wb') as out_fp:
                for i in func:
                    s = stream.get()
                    if len(s) > 0:
                        bar.update(len(s))
                        out_fp.write(s)
                        out_fp.flush()
                        irods_md5.update(s)
                        size += len(s)
            logger.debug("post size " + str(size))
        else:
            for i in func:
                s = stream.get()
                if len(s) > 0:
                    bar.update(len(s))
                    irods_md5.update(s)
                    size += len(s)
        return size
    except StopIteration:
        return b''


def archive_generator(func, stream, bar):
    """Yield the raw buffer for streaming.

    :param func: archive buffer iterator
    :param stream: raw buffer stream
    :param bar: progress monitor
    :return:
    """

    try:
        size = 0
        if debug:
            with open("debug_archive00.zip", 'wb') as out_fp:
                for i in func:
                    s = stream.get()
                    if len(s) > 0:
                        bar.update(len(s))
                        out_fp.write(s)
                        out_fp.flush()
                        size += len(s)
                        yield s
            logger.debug("post size " + str(size))
        else:
            for i in func:
                s = stream.get()
                if len(s) > 0:
                    bar.update(len(s))
                    yield s
    except StopIteration:
        return b''


def zip_collection(irods_client, stream, upload_success,  restrict_list):
    """Create a generator to zip the collection.
    Also calculate the iRODS sha256 chksums .

    :param irods_client: iRODS client manager
    :param UnseekableStream stream: raw buffer stream
    :param list restrict_list: list of files path
    :param dict upload_success: {file_path: hash_key}
    """

    zip_buffer = zipfile.ZipFile(stream, 'w', zipfile.ZIP_DEFLATED)
    yield

    collection = irods_client.coll
    session = irods_client.session
    for coll, sub, files in collection.walk():
        for file in files:
            min_path = file.path.replace("/nlmumc/projects/", "")
            if len(restrict_list) > 0 and (min_path not in restrict_list):
                continue

            irods_sha = hashlib.sha256()
            irods_md5 = hashlib.md5()
            buff = session.data_objects.open(file.path, 'r')
            # Replace space character
            root_folder_name = irods_client.imetadata.title.replace(" ", "_")
            # Remove iRODS project collection path
            zip_file_path = re.sub(r"/nlmumc/projects/P[0-9]{9}/C[0-9]{9}", root_folder_name, file.path)
            # Replace specials characters /: * ? " < > | ; #
            zip_file_path = re.sub(r"[\\:\*\?\"<>\|;#]", "_", zip_file_path)
            zip_info = zipfile.ZipInfo(zip_file_path)
            zip_info.file_size = file.size
            zip_info.compress_type = zipfile.ZIP_DEFLATED
            with zip_buffer.open(zip_info, mode='w') as dest:
                for chunk in iter(lambda: buff.read(BLOCK_SIZE), b''):
                    dest.write(chunk)
                    irods_sha.update(chunk)
                    irods_md5.update(chunk)
                    yield
            buff.close()

            sha_hexdigest = irods_sha.hexdigest()
            md5_hexdigest = irods_md5.hexdigest()
            # index 0 -> sha_hexdigest
            # index 1 -> md5_hexdigest
            hexdigest_list = [sha_hexdigest, md5_hexdigest]
            upload_success.update({file.path: hexdigest_list})

    zip_buffer.close()
    yield


def get_zip_generator(irods_client, upload_success, irods_md5, restrict_list, size) -> IteratorAsBinaryFile:
    """Bundle an iRODS collection into a compressed zip buffer.
    Return the zip buffer iterator.

    :param irods_client: iRODS client manager
    :param dict upload_success: {file_path: hash_key}
    :param list restrict_list: list of file's path to include in the zip
    :param irods_md5: hashlib.md5() object to calculate the md5 checksum
    :param int size: estimated zip size
    :return: zip buffer iterator
    """

    stream = UnseekableStream()
    bar = tqdm(total=size, unit="bytes", smoothing=0.1, unit_scale=True, disable=not debug)
    zip_iterator = zip_collection(irods_client, stream, upload_success, restrict_list)
    iterator = IteratorAsBinaryFile(size, archive_generator(zip_iterator, stream, bar), irods_md5)

    return iterator


def zip_generator_faker(irods_client, upload_success, irods_md5, restrict_list) -> int:
    """Fake the zip creation and estimate the size of the compressed zip buffer.
    Return the estimated size.

    :param irods_client: iRODS client manager
    :param dict upload_success: {file_path: hash_key}
    :param list restrict_list: list of file's path to include in the zip
    :param irods_md5: hashlib.md5() object to calculate the md5 checksum
    :return: estimated zip size
    """

    stream = UnseekableStream()
    bar = tqdm(total=1000, unit="bytes", smoothing=0.1, unit_scale=True, disable=not debug)
    zip_iterator = zip_collection(irods_client, stream, upload_success, restrict_list)
    size_bundle = archive_generator_faker(zip_iterator, stream, bar, irods_md5)

    return size_bundle


def bag_collection(irods_client, stream, upload_success):
    """Create a generator to zip the collection.
    Also request the iRODS sha256 chksums and compare it to the buffer.

    :param irods_client : list of files path
    :param UnseekableStream stream: raw buffer stream
    :param dict upload_success: {file_path: hash_key}
    """

    zip_buffer = zipfile.ZipFile(stream, 'w', compression=zipfile.ZIP_DEFLATED)
    yield

    collection = irods_client.coll
    session = irods_client.session
    imetadata = irods_client.imetadata
    checksum_list = []
    total_bytes = 0
    total_files = 0
    for coll, sub, files in collection.walk():
        for f in files:
            total_files += 1
            buff = session.data_objects.open(f.path, 'r')
            arc_name = f.path.replace(collection.path, f'{collection.name}/data')
            zip_info = zipfile.ZipInfo(arc_name)
            zip_info.file_size = f.size
            zip_info.compress_type = zipfile.ZIP_DEFLATED
            irods_sha256 = hashlib.sha256()
            irods_sha1 = hashlib.sha1()
            with zip_buffer.open(zip_info, mode='w') as dest:
                for chunk in iter(lambda: buff.read(BLOCK_SIZE), b''):
                    dest.write(chunk, )
                    irods_sha256.update(chunk)
                    irods_sha1.update(chunk)
                    total_bytes += len(chunk)
                    yield
            buff.close()

            sha1_hexdigest = irods_sha1.hexdigest()
            sha_hexdigest = irods_sha256.hexdigest()
            upload_success.update({f.path: sha_hexdigest})
            # logger.info(f"{'--':<20}Buffer checksum {f.name}:")
            # logger.info(f"{'--':<30}SHA-256: {sha256_hexdigest}")
            # logger.info(f"{'--':<30}SHA-1: {sha1_hexdigest}")
            checksum_list.append((arc_name.replace(f'{collection.name}/data', 'data'), sha1_hexdigest))
    yield

    tagmanifest = []
    checksum_list = sorted(checksum_list)
    manifest_name = f"{collection.name}/manifest-sha1.txt"
    zip_info = zipfile.ZipInfo(manifest_name)
    zip_info.compress_type = zipfile.ZIP_DEFLATED
    m_size = 0
    with zip_buffer.open(zip_info, mode='w') as manifest:
        md5 = hashlib.md5()
        for filename, digest in checksum_list:
            line = "%s  %s\n" % (digest, filename)
            line = line.encode('utf-8')
            m_size += len(line)
            md5.update(line)
            manifest.write(line)
        tagmanifest.append((md5.hexdigest(), manifest_name))
    yield

    bagit_name = f"{collection.name}/bagit.txt"
    zip_info = zipfile.ZipInfo(bagit_name)
    zip_info.compress_type = zipfile.ZIP_DEFLATED
    m_size = 0
    with zip_buffer.open(zip_info, mode='w') as manifest:
        md5 = hashlib.md5()
        line = """BagIt-Version: 0.97
Tag-File-Character-Encoding: UTF-8
"""
        line = line.encode('utf-8')
        m_size += len(line)
        md5.update(line)
        manifest.write(line)
        tagmanifest.append((md5.hexdigest(), bagit_name))
    yield

    oxum = "%s.%s" % (total_bytes, total_files)
    baginfo_name = f"{collection.name}/bag-info.txt"
    zip_info = zipfile.ZipInfo(baginfo_name)
    zip_info.compress_type = zipfile.ZIP_DEFLATED
    m_size = 0
    with zip_buffer.open(zip_info, mode='w') as manifest:
        md5 = hashlib.md5()
        line = f"""Bagging-Date: {date}
Created: {date_iso}
Payload-Oxum: {oxum}
"""
        line = line.encode('utf-8')
        m_size += len(line)
        md5.update(line)
        manifest.write(line)
        tagmanifest.append((md5.hexdigest(), baginfo_name))
    yield

    with open('resources/dataset.xml') as f:
        name = f"{collection.name}/metadata/dataset.xml"

        tree = ElementTree.parse(f)
        ElementTree.register_namespace('dcterms', "http://purl.org/dc/terms/")
        ElementTree.register_namespace('dc', "http://purl.org/dc/elements/1.1/")
        ElementTree.register_namespace('dcx-dai', "http://easy.dans.knaw.nl/schemas/dcx/dai/")
        ElementTree.register_namespace('ddm', "http://easy.dans.knaw.nl/schemas/md/ddm/")
        ElementTree.register_namespace('xsi', "http://www.w3.org/2001/XMLSchema-instance")
        ElementTree.register_namespace('id-type', "http://easy.dans.knaw.nl/schemas/vocab/identifier-type/")

        root = tree.getroot()

        for title in root.iter('{http://purl.org/dc/elements/1.1/}title'):
            title.text = f"{title.text}: {imetadata.title}"

        for description in root.iter('{http://purl.org/dc/terms/}description'):
            description.text = f"{description.text}: {imetadata.description}"

        doc = ElementTree.tostring(root)

        zip_info = zipfile.ZipInfo(name)
        zip_info.compress_type = zipfile.ZIP_DEFLATED
        m_size = 0
        with zip_buffer.open(zip_info, mode='w') as manifest:
            md5 = hashlib.md5()
            line = doc
            m_size += len(line)
            md5.update(line)
            manifest.write(line)
            tagmanifest.append((md5.hexdigest(), name))
    yield

    with open('resources/files.xml') as f:
        tree = ElementTree.parse(f)
        ElementTree.register_namespace('dcterms', "http://purl.org/dc/terms/")

        root = tree.getroot()
        file = root.find('file')

        for fmt in file.iter('{http://purl.org/dc/terms/}format'):
            pass

        for filename, digest in checksum_list:
            if filename != "data/metadata.xml":
                new_file = ElementTree.SubElement(root, file.tag)
                new_file.attrib = {"filepath": filename.replace(f'{collection.name}/data', 'data')}
                new_format = ElementTree.SubElement(new_file, fmt.tag)
                new_format.text = "text/plain"

        doc = ElementTree.tostring(root) + "\n\n".encode('utf-8')

        name = f"{collection.name}/metadata/files.xml"
        zip_info = zipfile.ZipInfo(name)
        zip_info.compress_type = zipfile.ZIP_DEFLATED
        m_size = 0
        with zip_buffer.open(zip_info, mode='w') as manifest:
            md5 = hashlib.md5()
            # header
            line = '<?xml version="1.0" encoding="UTF-8"?>\n'
            line = line.encode('utf-8')
            m_size += len(line)
            md5.update(line)
            manifest.write(line)
            # data
            line = doc
            m_size += len(line)
            md5.update(line)
            manifest.write(line)

            tagmanifest.append((md5.hexdigest(), name))
    yield

    tagmanifest_name = f"{collection.name}/tagmanifest-md5.txt"
    zip_info = zipfile.ZipInfo(tagmanifest_name)
    zip_info.compress_type = zipfile.ZIP_DEFLATED
    m_size = 0
    with zip_buffer.open(zip_info, mode='w') as manifest:
        for digest, filename in tagmanifest:
            line = "%s  %s\n" % (digest, filename.replace(f'{collection.name}/', ''))
            line = line.encode('utf-8')
            m_size += len(line)
            manifest.write(line)
    yield
    zip_buffer.close()
    yield


def bag_generator_faker(irods_client, upload_success, irods_md5):
    """Bundle an iRODS collection into a compressed zip buffer.
    Return the zip buffer iterator.

    :param irods_client: iRODS collection to zip
    :param dict upload_success: {file_path: hash_key}
    :param irods_md5: hashlib.md5()
    :return: zip buffer iterator
    """

    stream = UnseekableStream()
    bar = tqdm(total=100000000, unit="bytes", smoothing=0.1, unit_scale=True, disable=not debug)
    zip_iterator = bag_collection(irods_client, stream, upload_success)
    size_bundle = archive_generator_faker(zip_iterator, stream, bar, irods_md5)

    return size_bundle


def get_bag_generator(irods_client, upload_success, irods_md5, size) -> IteratorAsBinaryFile:
    """Bundle an iRODS collection into a compressed zip buffer.
    Return the zip buffer iterator.

    :param irods_client: iRODS collection to zip
    :param dict upload_success: {file_path: hash_key}
    :param irods_md5: hashlib.md5()
    :param int size: predicted size
    :return: zip buffer iterator
    """

    logger.info(f"{'--':<20}Bag predicted size: {size}")
    stream = UnseekableStream()
    bar = tqdm(total=size, unit="bytes", smoothing=0.1, unit_scale=True, disable=not debug)
    zip_iterator = bag_collection(irods_client, stream, upload_success)
    iterator = IteratorAsBinaryFile(size, archive_generator(zip_iterator, stream, bar), irods_md5)

    return iterator


class ExporterState(Enum):
    """Enum exporter state
    """

    ATTRIBUTE = 'exporterState'

    IN_QUEUE_FOR_EXPORT = "in-queue-for-export"

    CREATE_EXPORTER = "create-exporter"
    CREATE_DATASET = "create-dataset"
    PREPARE_COLLECTION = "prepare-collection"
    ZIP_COLLECTION = "zip-collection"
    UPLOAD_ZIPPED_COLLECTION = "upload-zipped-collection"
    VALIDATE_UPLOAD = "validate-upload"
    VALIDATE_CHECKSUM = "validate-checksum"

    DATASET_UNKNOWN = "dataset-unknown"
    CREATE_DATASET_FAILED = "create-dataset-failed"
    UPLOAD_FAILED = "upload-failed"
    UPLOAD_CORRUPTED = "upload-corrupted"
    REQUEST_REVIEW_FAILED = "request-review-failed"

    FINALIZE = "finalize"
    EXPORTED = "exported"
    # PREPARE_EXPORT = "prepare-export"
    # DO_EXPORT = "do-export"
    # PREPARE_BAG = "prepare-bag"
    # ZIP_BAG = "zip-bag"
    # UPLOAD_BAG = "upload-bag"
    # INVALID = None
    # REJECTED = None
    # FAILED = None
    # SUBMITTED = None
    # UPLOADED = None
    # FINALIZING = None
    # DRAFT = None
    # ARCHIVED = None


class ExporterClient:

    @staticmethod
    def run_checksum(path):
        return RuleManager.rule_collection_checksum(path)


class irodsMetadata:
    """Store all metadata (AVUs and metadata.xml) from an iRODS Collection
    """

    def __init__(self):
        self.title = None
        self.creator = None
        self.description = None
        self.date = None
        self.pid = None

        self.bytesize = None
        self.numfiles = None

        self.tissue = None
        self.technology = None
        self.organism = None
        self.factors = None
        self.protocol = None
        self.contact = None
        self.articles = None

        self.dataset_json = None
        self.depositor = None
