import base64
import binascii
import requests
import os
import logging
from datetime import datetime
from enum import Enum

logger = logging.getLogger("iRODS to Dataverse")


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


def submit_service_desk_ticket(email, description, error_message):
    """
    Submit an automated support request to the Jira Service Desk Cloud instance through
    our help center backend.

    Parameters
    ----------
    email: str
        The email of the user who started the process
    description: str
       Description to be shown in the ticket
    error_message: str
        Error message to display in Jira Service Desk
    """
    # Get the Help Center Backend url
    help_center_backend_base = os.environ.get("HC_BACKEND_URL")
    help_center_request_endpoint = "{}/help_backend/submit_request/automated_process_support".format(
        help_center_backend_base
    )

    error_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    request_json = {
        "email": email,
        "description": description,
        "error_timestamp": error_timestamp,
        "error_message": error_message,
    }

    try:
        response = requests.post(
            help_center_request_endpoint,
            json=request_json,
        )
        if response.ok:
            issue_key = response.json()["issueKey"]
            logger.error(f"{'--':<20}Support ticket '{issue_key}' created after process error")

        else:
            logger.error(f"{'--':<20}Response Help center backend not HTTP OK: '{response.status_code}'")
    except requests.exceptions.RequestException as e:
        logger.error(f"{'--':<20}Exception while requesting Support ticket after process error '{e}'")


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
