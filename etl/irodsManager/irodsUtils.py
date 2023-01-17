import base64
import binascii
import datetime
import hashlib
import io
import logging
import re
import zipfile
from enum import Enum
from io import RawIOBase
from typing import Generator

from requests.utils import super_len
from requests_toolbelt.multipart.encoder import CustomBytesIO, encode_with

logger = logging.getLogger("iRODS to Dataverse")
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

    def __init__(self, size, iterator, encoding="utf-8"):
        #: The expected size of the upload
        self.size = int(size)
        # self.md5 = md5
        if self.size < 0:
            raise ValueError("The size of the upload must be a positive integer")

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
            return b""

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
            return b"".join(self.iterator)

        self._load_bytes(size)
        s = self._buffer.read(size)
        if not s:
            self.len = 0

        if size < 0:
            self.len = 0

        # self.md5.update(s)
        return s


class UnseekableStream(RawIOBase):
    """
    Custom raw buffer for streaming.
    The get() method return the available chunk and clean-up/re-initialize the buffer to free up the memory.
    """

    def __init__(self):
        self._buffer = b""

    def writable(self):
        return True

    def write(self, b):
        if self.closed:
            raise ValueError("Stream was closed!")
        self._buffer += b
        return len(b)

    def get(self):
        chunk = self._buffer
        self._buffer = b""
        return chunk


def get_zip_buffer_size(zip_generator, stream_buffer) -> int:
    """
    Exhaust the zip generator to calculate the zip buffer size.
    The buffer size is required to correctly stream upload the zip file.

    Parameters
    ----------
    zip_generator: Generator
        create_zip_buffer_generator
    stream_buffer: UnseekableStream
        The upload buffer

    Returns
    -------
    int
        The zip buffer size
    """
    try:
        zip_buffer_size = 0
        for i in zip_generator:
            stream_chunk = stream_buffer.get()
            if len(stream_chunk) > 0:
                zip_buffer_size += len(stream_chunk)
        return zip_buffer_size
    except StopIteration:
        return 0


def upload_zip_generator(zip_generator, stream_buffer) -> bytes:
    """
    Yield the zip generator chunk from an UnseekableStream buffer to correctly stream upload the zip file.

    Parameters
    ----------
    zip_generator: Generator
        create_zip_buffer_generator
    stream_buffer: UnseekableStream
        The upload buffer

    Returns
    -------
    bytes
        The zip buffer chunk
    """
    try:
        for i in zip_generator:
            stream_chunk = stream_buffer.get()
            if len(stream_chunk) > 0:
                yield stream_chunk
    except StopIteration:
        return b""


def create_zip_buffer_generator(file_obj, stream_buffer, upload_checksums_dict):
    """
    Create a zip file for the single file to upload.

    The zip file creation is done during run-time, but not cached into disk or memory.
    The ZipFile object is initialized with an UnseekableStream stream_buffer.

    Meaning each chunk of the input file is passed to the stream_buffer to create a zip file. But each time a chunk is
    read from the stream_buffer during the upload, the buffer is emptied.
    So it doesn't cache the whole file into memory.

    While creating the zip buffer, each byte chunk is also feed to a SHA256 & a MD5 hash to calculate the checksums.
    (SHA256 for iRODS & MDR5 for Dataverse)

    Parameters
    ----------
    file_obj: irods.data_object.iRODSDataObject
    stream_buffer:
        a file-like object
    upload_checksums_dict: dict
        The dictionary that save the checksums values of each files for validations.

    Yields
    ------
    When the stream_buffer is updated
    """
    zip_buffer = zipfile.ZipFile(stream_buffer, "w", zipfile.ZIP_DEFLATED)
    yield

    # Initialize the checksum hashes
    irods_sha = hashlib.sha256()
    irods_md5 = hashlib.md5()

    file_buffer = file_obj.open("r")

    # Remove iRODS project collection path
    zip_file_path = re.sub(r"/nlmumc/projects/P[0-9]{9}/C[0-9]{9}", "", file_obj.collection.path)
    #  Replace specials characters ( ) [ ] { } $ % & - + @ ~ ' € ! ^
    zip_file_path = re.sub(r"[\\\(\)\[\]\{\}\$\%\&\+@~'€!\^]", "_", zip_file_path)
    # Replace specials characters /: * ? " < > | ; #
    zip_file_path = re.sub(r"[\\:\*\?\"<>\|;#]", "_", zip_file_path)
    zip_file_name = re.sub(r"[\\:\*\?\"<>\|;#]", "_", file_obj.name)
    if zip_file_path == "":
        zip_file_path = zip_file_name
    else:
        zip_file_path = zip_file_path + "/" + zip_file_name

    # Create a ZipInfo object and update its metadata
    zip_info = zipfile.ZipInfo(zip_file_path)
    zip_info.file_size = file_obj.size
    zip_info.compress_type = zipfile.ZIP_DEFLATED

    with zip_buffer.open(zip_info, mode="w") as dest:
        for chunk in iter(lambda: file_buffer.read(BLOCK_SIZE), b""):
            dest.write(chunk)
            irods_sha.update(chunk)
            irods_md5.update(chunk)
            yield
    file_buffer.close()

    sha_hex_digest = irods_sha.hexdigest()
    md5_hex_digest = irods_md5.hexdigest()
    # index 0 -> sha_hex_digest
    # index 1 -> md5_hex_digest
    hex_digest_list = [sha_hex_digest, md5_hex_digest]
    upload_checksums_dict.update({file_obj.path: hex_digest_list})

    zip_buffer.close()
    yield


def get_zip_iterator(file_obj, upload_checksums_dict, zip_buffer_size) -> IteratorAsBinaryFile:
    """
    Create the zip buffer iterator for the file that need to be uploaded.

    Parameters
    ----------
    file_obj: irods.data_object.iRODSDataObject
    upload_checksums_dict: dict
        The dictionary that save the checksums values of each files for validations.
    zip_buffer_size: int
        The total size of the buffer calculated beforehand

    Returns
    -------
    IteratorAsBinaryFile
        file-like object iterator compatible with stream upload.
    """
    stream_buffer = UnseekableStream()
    zip_generator = create_zip_buffer_generator(file_obj, stream_buffer, upload_checksums_dict)
    iterator = IteratorAsBinaryFile(zip_buffer_size, upload_zip_generator(zip_generator, stream_buffer))

    return iterator


def calculate_zip_buffer_size(file_obj, upload_checksums_dict) -> int:
    """
    Calculate the zip buffer total size before the upload.

    Parameters
    ----------
    file_obj: irods.data_object.iRODSDataObject
    upload_checksums_dict: dict
        The dictionary that save the checksums values of each files for validations.

    Returns
    -------
    int
        The total size of the buffer
    """
    stream_buffer = UnseekableStream()
    zip_generator = create_zip_buffer_generator(file_obj, stream_buffer, upload_checksums_dict)
    zip_buffer_size = get_zip_buffer_size(zip_generator, stream_buffer)

    return zip_buffer_size


def get_irods_data_object_checksum_value(file_obj) -> str:
    """
    Request the IRODS data object checksum value.
    iRODS checksums are the base64 encoded sha256 checksum, therefore need to be decoded before returning it.
    So it can be compared during validation.

    Parameters
    ----------
    file_obj: irods.data_object.iRODSDataObject

    Returns
    -------
    str
        SHA256 checksum value
    """
    checksum = file_obj.chksum()
    trim_checksum = checksum.replace("sha2:", "")
    base_hash = base64.b64decode(trim_checksum)

    return binascii.hexlify(base_hash).decode("utf-8")


class ExporterState(Enum):
    """Enum exporter state"""

    ATTRIBUTE = "exporterState"

    IN_QUEUE_FOR_EXPORT = "in-queue-for-export"

    CREATE_EXPORTER = "create-exporter"
    CREATE_DATASET = "create-dataset"
    PREPARE_COLLECTION = "prepare-collection"
    ZIP_COLLECTION = "zip-collection"
    UPLOAD_ZIPPED_COLLECTION = "upload-zipped-collection"
    UPLOADING_FILE = "uploading file - {}"
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


class CollectionAVU:
    """Store metadata AVUs from an iRODS Collection"""

    def __init__(self):
        self.creator = None
        self.pid = None

        self.bytesize = None
        self.numfiles = None
