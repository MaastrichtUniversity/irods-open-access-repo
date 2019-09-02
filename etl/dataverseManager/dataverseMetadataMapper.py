import json
import logging

logger = logging.getLogger('iRODS to Dataverse')

'''
TODO
self.protocol = None
 '''


class MetadataMapper:
    """Map iRODS metadata to the Open Access Repository metadata format
    """
    def __init__(self, imetadata, depositor):
        self.imetadata = imetadata
        self.dataset_json = None
        self.md = None
        self.imetadata.depositor = depositor

    def read_metadata(self):
        logger.info("--\t Map metadata")

        with open('resources/template.json') as f:
            self.dataset_json = json.load(f)

        self.md = self.dataset_json['datasetVersion']

        pid = self.imetadata.pid.split("/")
        self.update_pid(self.md, pid[0], pid[1])

        author = self.imetadata.creator.split("@")[0]
        self.add_author(author)

        url = f"https://hdl.handle.net/{self.imetadata.pid}"
        self.add_alternative_url(url)

        if self.imetadata.description is None:
            self.add_description("")
        else:
            self.add_description(self.imetadata.description)

        self.add_date(self.imetadata.date)

        self.add_title(self.imetadata.title)

        self.add_subject()

        contacts = []
        contact_email = self.add_contact_email(self.imetadata.creator)
        contacts.append(contact_email)

        for c in self.imetadata.contact:
            if len(c) != 0:
                if c.get("email") is None:
                    c.update({"email": ""})
                if c.get("affiliation") is None:
                    c.update({"affiliation": ""})
                pub = self.add_contact(c.get("firstName") + " " + c.get("lastName"), c.get("email"),
                                       c.get("affiliation"))
                contacts.append(pub)

        self.add_contacts(contacts)

        keywords = []
        if self.imetadata.tissue:
            keyword = self.add_keyword(self.imetadata.tissue.get("name"), self.imetadata.tissue.get("vocabulary"),
                                       self.imetadata.tissue.get("uri"))
            keywords.append(keyword)

        if self.imetadata.technology:
            keyword = self.add_keyword(self.imetadata.technology.get("name"),
                                       self.imetadata.technology.get("vocabulary"),
                                       self.imetadata.technology.get("uri"))
            keywords.append(keyword)

        if self.imetadata.organism:
            keyword = self.add_keyword(self.imetadata.organism.get("name"), self.imetadata.organism.get("vocabulary"),
                                       self.imetadata.organism.get("uri"))
            keywords.append(keyword)

        for f in self.imetadata.factors:
            keyword = self.add_keyword(f, "", "")
            keywords.append(keyword)

        self.add_keywords(keywords)

        publications = []
        for f in self.imetadata.articles:
            info = f.split("/")
            pub = self.add_publication(info[3] + info[4], info[2].strip(".org"), f)
            publications.append(pub)

        self.add_publications(publications)

        self.dataset_json['datasetVersion'] = self.md
        logger.debug(json.dumps(self.dataset_json, indent=4))

        return self.dataset_json

    @staticmethod
    def update_pid(md, authority, identifier, hdl="hdl"):
        md["protocol"] = hdl
        md["authority"] = authority
        md["identifier"] = identifier

    def update_fields(self, new):
        fields = self.md["metadataBlocks"]["citation"]["fields"]
        fields.append(new)

    def add_author(self, author, affiliation="", up=True):
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
        if up:
            self.update_fields(new)
        return new

    def add_title(self, title, up=True):
        new = {
            "typeName": "title",
            "multiple": False,
            "value": title,
            "typeClass": "primitive"
        }
        if up:
            self.update_fields(new)
        return new

    def add_description(self, description, up=True):
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
        if up:
            self.update_fields(new)
        return new

    def add_subject(self, up=True):
        new = {
            "typeName": "subject",
            "multiple": True,
            "value": [
                "Medicine, Health and Life Sciences"
            ],
            "typeClass": "controlledVocabulary"
        }
        if up:
            self.update_fields(new)
        return new

    def add_date(self, date, up=True):
        new = {
            "typeName": "dateOfDeposit",
            "multiple": False,
            "value": date,
            "typeClass": "primitive"
        }
        if up:
            self.update_fields(new)
        return new

    def add_contacts(self, contacts, up=True):
        new = {
            "typeName": "datasetContact",
            "multiple": True,
            "value": contacts,
            "typeClass": "compound"
        }
        if up:
            self.update_fields(new)
        return new

    @staticmethod
    def add_contact_email(email):
        new = {
            "datasetContactEmail": {
                "typeName": "datasetContactEmail",
                "multiple": False,
                "value": email,
                "typeClass": "primitive"
            }
        }
        return new

    @staticmethod
    def add_contact(name, email, affiliation):
        new = {
            "datasetContactAffiliation": {
                "multiple": False,
                "typeClass": "primitive",
                "typeName": "datasetContactAffiliation",
                "value": affiliation
            },
            "datasetContactEmail": {
                "multiple": False,
                "typeClass": "primitive",
                "typeName": "datasetContactEmail",
                "value": email
            },
            "datasetContactName": {
                "multiple": False,
                "typeClass": "primitive",
                "typeName": "datasetContactName",
                "value": name
            }
        }
        return new

    def add_keywords(self, keywords, up=True):
        new = {
            "multiple": True,
            "typeClass": "compound",
            "typeName": "keyword",
            "value": keywords
        }
        if up:
            self.update_fields(new)
        return new

    @staticmethod
    def add_keyword(value, vocabulary, uri):
        new = {
            "keywordValue": {
                "multiple": False,
                "typeClass": "primitive",
                "typeName": "keywordValue",
                "value": value
            },
            "keywordVocabulary": {
                "multiple": False,
                "typeClass": "primitive",
                "typeName": "keywordVocabulary",
                "value": vocabulary
            },
            "keywordVocabularyURI": {
                "multiple": False,
                "typeClass": "primitive",
                "typeName": "keywordVocabularyURI",
                "value": uri
            }
        }
        return new

    def add_alternative_url(self, url, up=True):
        new = {
            "multiple": False,
            "typeClass": "primitive",
            "typeName": "alternativeURL",
            "value": url
        }
        if up:
            self.update_fields(new)
        return new

    def add_publications(self, publications, up=True):
        new = {
            "multiple": True,
            "typeClass": "compound",
            "typeName": "publication",
            "value": publications
        }
        if up:
            self.update_fields(new)
        return new

    @staticmethod
    def add_publication(value, doi, url):
        new = {
            "publicationIDNumber": {
                "multiple": False,
                "typeClass": "primitive",
                "typeName": "publicationIDNumber",
                "value": value
            },
            "publicationIDType": {
                "multiple": False,
                "typeClass": "controlledVocabulary",
                "typeName": "publicationIDType",
                "value": doi
            },
            "publicationURL": {
                "multiple": False,
                "typeClass": "primitive",
                "typeName": "publicationURL",
                "value": url
            }
        }
        return new
