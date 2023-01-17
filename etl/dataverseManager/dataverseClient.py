import io
import json
import logging
import ntpath
import os
import re
import time
from http import HTTPStatus

import requests
from requests import Response
from requests.exceptions import ProxyError
from requests_toolbelt.multipart.encoder import MultipartEncoder

from etl.irodsManager.irodsUtils import (
    get_zip_iterator,
    calculate_zip_buffer_size,
    ExporterState as Status,
    get_irods_data_object_checksum_value,
)

logger = logging.getLogger("iRODS to Dataverse")

BLOCK_SIZE = 1024 * io.DEFAULT_BUFFER_SIZE


class DataverseClient:
    """Dataverse client to export datasets and files"""

    def __init__(self, host, token, alias, irods_client, depositor):
        """
        Instantiate a DataverseClient object.

        Parameters
        ----------
        host: str
            The URL of the dataverse host server
        token: str
            The token API
        alias: str
            Alias/ID of the dataverse endpoint where to create datasets & upload files
        irods_client: etl.irodsManager.irodsClient.irodsClient
            An active session irods client
        depositor: str
            The user who started the export process
        """
        self.repository = "Dataverse"
        self.host = host
        self.alias = alias
        self.token = token
        self.depositor = depositor

        self.restrict = False
        self.restrict_list = []

        self.irods_client = irods_client
        self.session = irods_client.session

        self.dataset_deposit_url = None
        self.dataset_pid = None
        self.dataset_url = None

        # str: irods data object path => sha_hex_digest
        # str: dataverse relative file path => md5_hex_digest
        self.upload_checksums_dict = {}

        self.zip_name = irods_client.instance.title.title + ".zip"

    def create_dataset(self, metadata, data_export=False):
        """
        Request a dataset creation with the input metadata.

        Parameters
        ----------
        metadata: dict
            The iRODS collection metadata mapped to match Dataverse metadata.
        data_export: bool
            If false, finalize the export process after the dataset creation.
        """
        logger.info(f"{'--':<10}Dataset - request creation")

        self.irods_client.update_metadata_status(Status.ATTRIBUTE.value, Status.CREATE_DATASET.value)
        url = f"{self.host}/api/dataverses/{self.alias}/datasets/"

        try:
            response = requests.post(
                url,
                data=json.dumps(metadata),
                headers={"Content-type": "application/json", "X-Dataverse-key": self.token},
            )
        except ProxyError:
            logger.error(self.host + " cannot be reached. Create dataset failed")
            self.irods_client.update_metadata_status(Status.ATTRIBUTE.value, Status.CREATE_DATASET_FAILED.value)

            return

        if response.status_code == HTTPStatus.CREATED.value:
            self.dataset_pid = response.json()["data"]["persistentId"]
            self.dataset_url = f"{self.host}/dataset.xhtml?persistentId={self.dataset_pid}&version=DRAFT"
            self.dataset_deposit_url = f"{self.host}/api/datasets/:persistentId/add?persistentId={self.dataset_pid}"
            logger.info(f"{'--':<20}Dataset created with pid: {self.dataset_pid}")
        else:
            logger.error(f"{'--':<20}Create dataset failed")
            logger.error(response.content)
            self.irods_client.update_metadata_status(Status.ATTRIBUTE.value, Status.CREATE_DATASET_FAILED.value)

            return

        if not data_export and response.status_code == HTTPStatus.CREATED.value:
            self.irods_client.update_metadata_status(Status.ATTRIBUTE.value, Status.FINALIZE.value)
            self._final_report()
            self._email_confirmation()
            self._submit_dataset_for_review()

    def export_files(self, restrict=False, restrict_list=""):
        """
        Start to export the iRODS collection files to Dataverse. And then finalize the process, if all files uploads
        were successful.

        Parameters
        ----------
        restrict: bool
            If True, put a restriction/embargo on the exported files in Dataverse.
        restrict_list: str
            CSV string that contains the filtered user files selection for the export.
        """
        self.restrict = restrict
        if len(restrict_list) > 0:
            self.restrict_list = restrict_list

        if self.dataset_deposit_url is None:
            logger.error(f"{'--':<20}Dataset unknown")
            self.irods_client.update_metadata_status(Status.ATTRIBUTE.value, Status.DATASET_UNKNOWN.value)

            return

        self.irods_client.update_metadata_status(Status.ATTRIBUTE.value, Status.PREPARE_COLLECTION.value)
        success = self._upload_files()
        if success:
            self._final_report()
            self._email_confirmation()
            self._submit_dataset_for_review()
        else:
            logger.error(f"{'--':<20}Export failed")
            self.irods_client.update_metadata_status(Status.ATTRIBUTE.value, Status.UPLOAD_CORRUPTED.value)

    def _upload_files(self) -> bool:
        """
        Loop over the collection data objects/files recursively:
            * Check the file was selected for the upload.
            * Request the iRODS data object checksum value
            * Start the file upload
            * Check the iRODS data object checksum value to the file buffer checksum value
            * Check the file buffer checksum value against the Dataverse uploaded file checksum value

        Returns
        -------
        bool
            If true, all uploads were successful.
        """
        logger.info(f"{'--':<10}Upload Files")
        total_uploads = 0
        validated_uploads = 0

        collection = self.irods_client.collection_object
        for coll, sub, files in collection.walk():
            for file_obj in files:
                min_path = file_obj.path.replace("/nlmumc/projects/", "")
                if len(self.restrict_list) > 0 and (min_path not in self.restrict_list):
                    continue
                total_uploads += 1

                logger.info(f"{'--':<15}Start upload file: {file_obj.path}")

                logger.info(f"{'--':<20}Calculate iRODS file checksum")
                decoded_checksum = get_irods_data_object_checksum_value(file_obj)
                logger.debug(f"{'--':<20}File checksum {file_obj.name} - {decoded_checksum}")

                response = self._upload_file(file_obj)

                validated_checksum = self._validate_buffer_checksum(file_obj.path, decoded_checksum)
                validated_upload = self._validate_upload_checksum(response)
                if validated_checksum and validated_upload:
                    validated_uploads += 1

        if total_uploads == validated_uploads:
            return True

        return False

    def _upload_file(self, file_obj) -> Response:
        """
        This method wraps the file object buffer into zip object buffer to "correctly" stream upload the single file.

        Currently, passing the iRODS data object buffer to the repost POST method doesn't correctly stream upload
        the single file to Dataverse. Without using the zip object buffer, the whole file content is loaded into
        the memory.

        Parameters
        ----------
        file_obj: irods.data_object.iRODSDataObject

        Returns
        -------
        Response
            The POST http response
        """
        logger.info(f"{'--':<20}Upload file zipped")
        logger.debug(f"{'--':<20}Upload file zipped {file_obj.name} - {file_obj.size}")

        file_upload_status = Status.UPLOADING_FILE.value.format(file_obj.name)
        self.irods_client.update_metadata_status(Status.ATTRIBUTE.value, file_upload_status)

        zip_buffer_size = calculate_zip_buffer_size(file_obj, self.upload_checksums_dict)
        logger.debug(f"{'--':<20}Upload size_bundle - {zip_buffer_size}")
        logger.debug(f"{'--':<20}Upload upload_checksums_dict - {self.upload_checksums_dict[file_obj.path]}")

        zip_iterator = get_zip_iterator(file_obj, self.upload_checksums_dict, zip_buffer_size)

        file_metadata = {
            "restrict": self.restrict,
        }

        multipart_encoder = MultipartEncoder(
            fields={
                "jsonData": json.dumps(file_metadata),
                "file": (self.zip_name, zip_iterator),
            }
        )
        response = None
        try:
            response = requests.post(
                url=self.dataset_deposit_url,
                data=multipart_encoder,
                headers={
                    "Content-Type": multipart_encoder.content_type,
                    "X-Dataverse-key": self.token,
                },
            )
        except ProxyError:
            logger.error(self.host + " cannot be reached. Upload data failed")
            self.irods_client.update_metadata_status(Status.ATTRIBUTE.value, Status.UPLOAD_FAILED.value)

        return response

    def _validate_buffer_checksum(self, file_path, checksum) -> bool:
        """
        Check that the http response information (status code & checksum) are valid.

        Parameters
        ----------
        file_path: str
            The relative iRODS file path
        checksum: str
            The SHA256 checksum value

        Returns
        -------
        bool
            True, if successful.
        """
        validated = False

        if self.upload_checksums_dict[file_path] == checksum:
            validated = True
            logger.info(f"{'--':<20}iRODS & buffer SHA-256 checksum: validated")
        else:
            logger.error(f"{'--':<20}iRODS & buffer SHA-256 checksum: failed")
            self.irods_client.update_metadata_status(Status.ATTRIBUTE.value, Status.UPLOAD_CORRUPTED.value)

        return validated

    def _validate_upload_checksum(self, response) -> bool:
        """
        Check that the http response information (status code & checksum) are valid.

        Parameters
        ----------
        response: Response
            The POST http response

        Returns
        -------
        bool
            True, if successful.
        """
        validated = False

        if response.status_code != HTTPStatus.OK.value:
            logger.error(f"{'--':<30}{response.content.decode('utf-8')}")

            return validated

        for file_json in response.json()["data"]["files"]:
            # Check if the file is at the root of the collection
            if "directoryLabel" in file_json:
                file_path = f"/{file_json['directoryLabel']}/" f"{file_json['dataFile']['filename']}"
            else:
                file_path = file_json["dataFile"]["filename"]
                logger.info(f"{'--':<20}iRODS & Dataverse MD5 checksum: filename")

            # Dataverse rename '.metadata_versions' sub-folder path by removing the '.'
            # So we need to revert it back to be able to compare the md5 checksums values
            metadata_version_dataverse = "/metadata_versions/"
            metadata_version_irods = "/.metadata_versions/"
            file_path = file_path.replace(metadata_version_dataverse, metadata_version_irods)
            logger.debug(f"{'--':<20}iRODS & Dataverse MD5 checksum: {file_path}")

            # index 1 -> md5_hexdigest
            if file_json["dataFile"]["md5"] == self.upload_checksums_dict[file_path]:
                # count += 1
                validated = True
                logger.info(f"{'--':<20}iRODS & Dataverse MD5 checksum: validated")
            else:
                logger.error(f"{'--':<20}iRODS & Dataverse MD5 checksum: failed")
                self.irods_client.update_metadata_status(Status.ATTRIBUTE.value, Status.UPLOAD_CORRUPTED.value)

        return validated

    def _final_report(self):
        """
        Add the dataset pid to the source iRODS collection as an AVU.
        Also do the final update to the export status AVU.
        """
        logger.info(f"{'--':<10}Report final progress")
        self.irods_client.add_metadata("externalPID", self.dataset_pid, "Dataverse")
        self.irods_client.update_metadata_status(Status.ATTRIBUTE.value, Status.EXPORTED.value)
        time.sleep(5)
        self.irods_client.remove_metadata(Status.ATTRIBUTE.value, f"Dataverse:{Status.EXPORTED.value}")
        logger.info(f"{'--':<10}Export Done")

    def _email_confirmation(self):
        """
        Send a confirmation email to the depositor at the end of the export.
        """
        host = os.environ["DH_MAILER_HOST"]
        user = os.environ["DH_MAILER_USERNAME"]
        pwd = os.environ["DH_MAILER_PASSWORD"]
        from_address = "datahub@maastrichtuniversity.nl"

        endpoint = "http://" + host + "/email/send"

        template_options = {
            "TITLE": self.irods_client.instance.title.title,
            "DESCRIPTION": self.irods_client.instance.description.description,
            "CREATOR": self.irods_client.instance.creator.full_name,
            "DATE": self.irods_client.instance.date.date,
            "BYTESIZE": self.irods_client.collection_avu.bytesize,
            "NUMFILES": self.irods_client.collection_avu.numfiles,
            "PID": self.irods_client.instance.identifier.pid,
            "TIMESTAMP": time.strftime("%d-%m-%Y %H:%M:%S"),
            "DEPOSITOR": self.depositor,
            "REPOSITORY": self.repository,
            "EXTERNAL_PID": self.dataset_pid,
            "DATASET_URL": self.dataset_url,
        }

        data_user = {
            "language": "en",
            "templateName": "OpenAccess_export_confirmation",
            "templateOptions": template_options,
            "emailOptions": {
                "from": from_address,
                "to": self.depositor,
            },
        }

        # Post the e-mail confirmation to the user
        try:
            resp_user = requests.post(endpoint, json=data_user, auth=(user, pwd))
        except ProxyError:
            logger.error(endpoint + " cannot be reached. Send e-mail confirmation failed")

            return

        if resp_user.status_code == HTTPStatus.OK.value:
            logger.info(f"Reporting e-mail confirmation sent to {self.depositor}")
        else:
            logger.error(resp_user.status_code)
            logger.error(resp_user.content)

    def _submit_dataset_for_review(self):
        """
        Send a request to update the dataset status from 'draft' to 'ask for review'.
        """
        logger.info(f"{'--':<10}Dataset - request review")

        url = f"{self.host}/api/datasets/:persistentId/submitForReview?persistentId={self.dataset_pid}"

        try:
            resp = requests.post(
                url,
                headers={"Content-type": "application/json", "X-Dataverse-key": self.token},
            )
            if resp.status_code == HTTPStatus.OK.value:
                logger.info(f"Dataset have been submitted for review: {self.dataset_url}")
            else:
                logger.error(f"{'--':<20}Create dataset failed")
                logger.error(resp.status_code)
                logger.error(resp.content)
        except ProxyError:
            logger.error(self.host + " cannot be reached. Submit for review failed")
            self.irods_client.add_metadata(Status.ATTRIBUTE.value, f"Dataverse:{Status.REQUEST_REVIEW_FAILED}")
