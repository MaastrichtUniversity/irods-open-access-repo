import logging
import base64
import binascii
import unicodedata
import os
import json
import ssl

from irods.rule import Rule
from irods.session import iRODSSession
from irodsrulewrapper.rule import RuleJSONManager

# To avoid potential conflict between irodsrulewrapper.rule.RuleManager with the inline class RuleManager.
# irodsrulewrapper.rule.RuleManager is imported as RuleWrapperManager
from irodsrulewrapper.rule import RuleManager as RuleWrapperManager
from dhpythonirodsutils import formatters

logger = logging.getLogger("iRODS to Dataverse")


class RuleManager:
    """Manager to execute iRods rules"""

    def __init__(self, session, collection):
        """
        :param session: iRODS connection session
        :param collection: collection irodsManager data object
        """
        split = collection.path.split("/")
        self.projectID = split[3]
        self.collectionID = split[4]

        self.session = session
        self.collection = collection

    def rule_open(self):
        logger.info("Rule open")
        RuleWrapperManager(admin_mode=True).open_project_collection(self.projectID, self.collectionID, "rods", "own")

    def rule_close(self):
        logger.info("Rule close")
        RuleWrapperManager(admin_mode=True).close_project_collection(self.projectID, self.collectionID)

    # TODO: Consider to convert the rule_body into an iRODS server side rule
    def rule_checksum(self, path):
        logger.info(f"{'--':<20}Rule checksum")
        self.session.connection_timeout = 1200
        rule_body = (
            "do_checkSum {"
            "{ msiDataObjChksum("
            "'" + path + "',"
            "'forceChksum=', *chkSum);\n"
            "writeLine('stdout', *chkSum);}}"
        )
        rule = Rule(self.session, body=rule_body, output="ruleExecOut")

        irods_hash = self.parse_rule_output(rule.execute()).split("sha2:")[1]
        base_hash = base64.b64decode(irods_hash)
        irods_hash_decode = binascii.hexlify(base_hash).decode("utf-8")
        self.session.connection_timeout = 120

        return irods_hash_decode

    @staticmethod
    def rule_collection_checksum(path):
        logger.info(f"{'--':<10}Query collection checksum")
        project_id = formatters.get_project_id_from_project_collection_path(path)
        collection_id = formatters.get_collection_id_from_project_collection_path(path)
        data = RuleJSONManager(admin_mode=True).get_project_collection_checksum(project_id, collection_id)
        for virtual_path in data:
            base_hash = base64.b64decode(data[virtual_path])
            irods_hash_decode = binascii.hexlify(base_hash).decode("utf-8")
            data[virtual_path] = irods_hash_decode
        return data

    @staticmethod
    def parse_rule_output(out_param_array):
        buff = out_param_array.MsParam_PI[0].inOutStruct.stdoutBuf.buf
        buff = buff.decode("utf-8")
        buf_cleaned = "".join(ch for ch in buff if unicodedata.category(ch)[0] != "C")

        return buf_cleaned
