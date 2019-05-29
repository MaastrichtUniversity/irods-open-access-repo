import logging
import base64
import binascii
import unicodedata

from irods.rule import Rule

logger = logging.getLogger('iRODS to Dataverse')


class RuleManager:
    """
           Manager to execute iRods rules
    """

    def __init__(self, path, session, collection):
        """
        :param path: collection path, where to apply the rules
        :param session: iRODS connection session
        :param collection: collection irodsManager data object
        """
        split = path.split("/")
        self.projectID = split[3]
        self.collectionID = split[4]

        self.session = session
        self.collection = collection

    def rule_open(self):
        logger.info("Rule open")
        rule_body = "do_openProjectCollection {" \
                    "{openProjectCollection('" + self.projectID + "', '" + self.collectionID + "', 'rods', 'own');}" \
                                                                                               "}"
        open_rule = Rule(self.session, body=rule_body)
        open_rule.execute()

    def rule_close(self):
        logger.info("Rule close")
        rule_body = "do_closeProjectCollection {" \
                    "{closeProjectCollection('" + self.projectID + "', '" + self.collectionID + "');}}"

        close_rule = Rule(self.session, body=rule_body)
        close_rule.execute()

    def rule_deletion(self, upload_success):
        logger.info("Rule deletion")

        # Check if all the files have been successfully uploaded before deletion
        if len(upload_success) == len(self.collection.data_objects):
            logger.info("--\t\t\t Start deletion")
            for data in self.collection.data_objects:
                if data.name != "metadata.xml":
                    rule_body = "do_deleteDataObject {" \
                                "{ msiDataObjUnlink(" \
                                "'/nlmumc/projects/"+self.projectID+"/"+self.collectionID+"/"+data.name+"'," \
                                "'forceChksum=', *chkSum);\n" \
                                "writeLine('stdout', *chkSum);}}"
                    rule = Rule(self.session, body=rule_body, output="ruleExecOut")
                    out = self.parse_rule_output(rule.execute())
                    if out == "0":
                        logger.info("--\t\t\t Delete:\t" + data.name)
                    else:
                        logger.error("--\t\t\t File:\t" + out)
            logger.info("--\t\t\t End deletion")
        else:
            logger.info("Deletion skipped. collection.files != uploaded.files")

    def rule_checksum(self, path):
        logger.info("--\t\t\t Rule checksum")
        self.session.connection_timeout = 1200
        rule_body = "do_checkSum {" \
                    "{ msiDataObjChksum(" \
                    "'"+path+"'," \
                    "'forceChksum=', *chkSum);\n" \
                    "writeLine('stdout', *chkSum);}}"
        rule = Rule(self.session, body=rule_body, output="ruleExecOut")

        irods_hash = self.parse_rule_output(rule.execute()).split('sha2:')[1]
        base_hash = base64.b64decode(irods_hash)
        irods_hash_decode = binascii.hexlify(base_hash).decode("utf-8")
        self.session.connection_timeout = 120

        return irods_hash_decode

    @staticmethod
    def parse_rule_output(out_param_array):
        buff = out_param_array.MsParam_PI[0].inOutStruct.stdoutBuf.buf
        buff = buff.decode('utf-8')
        buf_cleaned = "".join(ch for ch in buff if unicodedata.category(ch)[0] != "C")

        return buf_cleaned
