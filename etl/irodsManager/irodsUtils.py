import base64
import binascii
from enum import Enum


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
