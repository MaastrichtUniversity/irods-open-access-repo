import logging
import base64
import binascii
import unicodedata
import os
import json
import ssl

from irods.rule import Rule
from irods.session import iRODSSession

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
        rule_body = (
            "do_openProjectCollection {"
            "{openProjectCollection('" + self.projectID + "', '" + self.collectionID + "', 'rods', 'own');}"
            "}"
        )
        open_rule = Rule(self.session, body=rule_body)
        open_rule.execute()

    def rule_close(self):
        logger.info("Rule close")
        rule_body = (
            "do_closeProjectCollection {"
            "{closeProjectCollection('" + self.projectID + "', '" + self.collectionID + "');}}"
        )

        close_rule = Rule(self.session, body=rule_body)
        close_rule.execute()

    # TODO: Consider to convert the rule_body into an iRODS server side rule
    def rule_deletion(self, upload_success):
        logger.info("Rule deletion")

        # Check if all the files have been successfully uploaded before deletion
        if len(upload_success) == len(self.collection.data_objects):
            logger.info(f"{'--':<20}Start deletion")
            for data in self.collection.data_objects:
                if data.name != "metadata.xml":
                    rule_body = (
                        "do_deleteDataObject {"
                        "{ msiDataObjUnlink("
                        "'/nlmumc/projects/" + self.projectID + "/" + self.collectionID + "/" + data.name + "',"
                        "'forceChksum=', *chkSum);\n"
                        "writeLine('stdout', *chkSum);}}"
                    )
                    rule = Rule(self.session, body=rule_body, output="ruleExecOut")
                    out = self.parse_rule_output(rule.execute())
                    if out == "0":
                        logger.info(f"{'--':<30} Delete:\t" + data.name)
                    else:
                        logger.error(f"{'--':<30} File:\t" + out)
            logger.info(f"{'--':<30} End deletion")
        else:
            logger.info("Deletion skipped. collection.files != uploaded.files")

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

    # TODO: Consider to convert the rule_body into an iRODS server side rule
    @staticmethod
    def rule_collection_checksum(path):
        logger.info(f"{'--':<10}Query collection checksum")
        ssl_context = ssl.create_default_context(purpose=ssl.Purpose.SERVER_AUTH, cafile=None, capath=None, cadata=None)
        ssl_settings = {
            "irods_client_server_negotiation": "request_server_negotiation",
            "irods_client_server_policy": os.environ["IRODS_CLIENT_SERVER_POLICY"],
            "irods_encryption_algorithm": "AES-256-CBC",
            "irods_encryption_key_size": 32,
            "irods_encryption_num_hash_rounds": 16,
            "irods_encryption_salt_size": 8,
            "ssl_context": ssl_context,
        }
        session = iRODSSession(
            host=os.environ["IRODS_HOST"],
            port=1247,
            user=os.environ["IRODS_USER"],
            password=os.environ["IRODS_PASS"],
            zone="nlmumc",
            **ssl_settings,
        )
        session.connection_timeout = 1200

        path = path.split("/")
        project = path[3]
        collID = path[4]

        rule_body = f"""do_checkSum(){{
            *project = '{project}';
            *collID = '{collID}';
            *collection = '/nlmumc/projects/*project/*collID';
            *details = "{{}}";

            foreach ( *Row in SELECT DATA_PATH, COLL_NAME WHERE COLL_NAME like "*collection%") {{
                *subName = triml(*Row.DATA_PATH,*collID);

                *name = *collection ++ str(*subName);

                msiDataObjChksum(*name,"forceChksum=", *chkSum);
                *chkSum = triml(*chkSum,"sha2:");

                msiString2KeyValPair("", *titleKvp);
                msiAddKeyVal(*titleKvp, *name, *chkSum);
                msi_json_objops(*details, *titleKvp, "add");
            }}
            writeLine('stdout', *details);
        }}
        """

        rule = Rule(session, body=rule_body, output="ruleExecOut")

        irods_hash = RuleManager.parse_rule_output(rule.execute())
        session.connection_timeout = 120
        session.cleanup()

        data = json.loads(irods_hash)
        for k in data:
            base_hash = base64.b64decode(data[k])
            irods_hash_decode = binascii.hexlify(base_hash).decode("utf-8")
            data[k] = irods_hash_decode

        return data

    @staticmethod
    def parse_rule_output(out_param_array):
        buff = out_param_array.MsParam_PI[0].inOutStruct.stdoutBuf.buf
        buff = buff.decode("utf-8")
        buf_cleaned = "".join(ch for ch in buff if unicodedata.category(ch)[0] != "C")

        return buf_cleaned
