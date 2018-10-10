import json
import logging

logger = logging.getLogger('iRODS to Dataverse')


class metadataMapper():

    def __init__(self, title, creator, description, date, pid):
        self.title = title
        self.creator = creator
        self.description = description
        self.date = date
        self.pid = pid
        self.dataset_json = None
        self.md = None

    def read_metadata(self):
        # title = meta_dict.get("title")
        # creator = meta_dict.get("creator")
        # description = meta_dict.get("description")
        # date = meta_dict.get("date")

        # dataset_json = None
        with open('template.json') as f:
            self.dataset_json = json.load(f)

        md = self.dataset_json['datasetVersion']

        # pid = meta_dict.get("PID")
        print("--\t" + self.pid)
        logger.info("--\t" + self.pid)

        pid = self.pid.split("/")
        self.update_pid(md, pid[0], pid[1])

        new = self.add_author(self.creator)
        self.update_fields(md, new)

        new = self.add_description(self.description)
        self.update_fields(md, new)

        new = self.add_date(self.date)
        self.update_fields(md, new)

        new = self.add_title(self.title)
        self.update_fields(md, new)

        new = self.add_subject()
        self.update_fields(md, new)

        new = self.add_contact_email(self.creator)
        self.update_fields(md, new)

        self.dataset_json['datasetVersion'] = md
        logger.info(json.dumps(self.dataset_json, indent=4))

        return self.dataset_json


    def update_pid(self, md, authority, identifier, hdl="hdl"):
        md["protocol"] = hdl
        md["authority"] = authority
        md["identifier"] = identifier

    def update_fields(self, md, new):
        fields = md["metadataBlocks"]["citation"]["fields"]
        fields.append(new)

    def add_author(self, author, affiliation):
        new = {
            "typeName": "author",
            "multiple": True,
            "value": [
                {
                    "authorAffiliation": {
                        "typeName": "authorAffiliation",
                        "multiple": False,
                        "value": affiliation,
                        "typeClass": "primitive"
                    },
                    "authorName": {
                        "typeName": "authorName",
                        "multiple": False,
                        "value": author,
                        "typeClass": "primitive"
                    }
                }
            ],
            "typeClass": "compound"
        }
        return new

    def add_author(self, author):
        new = {
            "typeName": "author",
            "multiple": True,
            "value": [
                {
                    "authorName": {
                        "typeName": "authorName",
                        "multiple": False,
                        "value": author,
                        "typeClass": "primitive"
                    }
                }
            ],
            "typeClass": "compound"
        }
        return new

    def add_title(self, title):
        new = {
            "typeName": "title",
            "multiple": False,
            "value": title,
            "typeClass": "primitive"
        }
        return new

    def add_description(self, description):
        new = {
            "typeName": "dsDescription",
            "multiple": True,
            "value": [
                {
                    "dsDescriptionValue": {
                        "typeName": "dsDescriptionValue",
                        "multiple": False,
                        "value": description,
                        "typeClass": "primitive"
                    }
                }
            ],
            "typeClass": "compound"
        }
        return new

    def add_subject(self):
        new = {
            "typeName": "subject",
            "multiple": True,
            "value": [
                "Medicine, Health and Life Sciences"
            ],
            "typeClass": "controlledVocabulary"
        }
        return new

    def add_date(self, date):
        new = {
            "typeName": "productionDate",
            "multiple": False,
            "value": date,
            "typeClass": "primitive"
        }
        return new

    def add_contact_email(self, email):
        new = {
            "typeName": "datasetContact",
            "multiple": True,
            "value": [
                {
                    "datasetContactEmail": {
                        "typeName": "datasetContactEmail",
                        "multiple": False,
                        "value": email,
                        "typeClass": "primitive"
                    }
                }
            ],
            "typeClass": "compound"
        }
        return new
