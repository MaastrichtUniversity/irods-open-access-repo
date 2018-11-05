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
        :param collection: collection irods data object
        """
        split = path.split("/")
        self.projectID = split[3]
        self.collectionID = split[4]

        self.session = session
        self.collection = collection

    def rule_open(self):
        print("Rule open")
        logger.info("Rule open")

        open_rule = Rule(self.session, "openProjectCollection.r")
        open_rule.params.update({"*project": "\'" + self.projectID + "\'"})
        open_rule.params.update({"*projectCollection": "\'" + self.collectionID + "\'"})

        print(open_rule.params)

        open_rule.execute()

    def rule_close(self):
        print("Rule close")
        logger.info("Rule close")

        open_rule = Rule(self.session, "closeProjectCollection.r")
        open_rule.params.update({"*project": "\'" + self.projectID + "\'"})
        open_rule.params.update({"*projectCollection": "\'" + self.collectionID + "\'"})

        print(open_rule.params)

        open_rule.execute()

    def rule_deletion(self, upload_success):
        print("Rule deletion")
        logger.info("Rule deletion")

        # Check if all the files have been succesfully uploaded before deletion
        if len(upload_success) == len(self.collection.data_objects):
            logger.info("--\t\t\t Start deletion")
            for data in self.collection.data_objects:
                if data.name != "metadata.xml":
                    rule = Rule(self.session, "deleteDataObject.r")
                    rule.params.update({"*project":  "\'"+self.projectID+"\'"})
                    rule.params.update({"*projectCollection": "\'"+self.collectionID+"\'"})
                    rule.params.update({"*fileName": "\'"+data.name+"\'"})
                    out = self.parse_rule_output(rule.execute())
                    if out == "0":
                        print("--\t\t\t Delete:\t" + data.name)
                        logger.info("--\t\t\t File:\t" + data.name)
                    else:
                        print("**\t\t\t Delete:\t" + data.name)
                        logger.error("--\t\t\t File:\t" + data.name)
                        print("**\t\t\t Delete:\t" + out)
                        logger.error("--\t\t\t File:\t" + out)
            logger.info("--\t\t\t End deletion")
        else:
            print("Deletion skipped. collection.files != uploaded.files")
            logger.info("Deletion skipped. collection.files != uploaded.files")

    def rule_checksum(self, name):
        print("--\t\t\t Rule checksum")
        logger.info("--\t\t\t Rule checksum")

        rule = Rule(self.session, "checksums.r")
        rule.params.update({"*project": "\'" + self.projectID + "\'"})
        rule.params.update({"*projectCollection": "\'" + self.collectionID + "\'"})
        rule.params.update({"*fileName": "\'" + name + "\'"})

        irods_hash = self.parse_rule_output(rule.execute()).split('sha2:')[1]
        base_hash = base64.b64decode(irods_hash)
        irods_hash_decode = binascii.hexlify(base_hash).decode("utf-8")

        self.session.cleanup()

        return irods_hash_decode

    @staticmethod
    def parse_rule_output(out_param_array):
        buff = out_param_array.MsParam_PI[0].inOutStruct.stdoutBuf.buf
        buff = buff.decode('utf-8')
        buf_cleaned = "".join(ch for ch in buff if unicodedata.category(ch)[0] != "C")

        return buf_cleaned