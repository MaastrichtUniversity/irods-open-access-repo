import json
import logging
import urllib.request

logger = logging.getLogger('iRODS to Dataverse')

'''
TODO
self.protocol = None
 '''


class MetadataMapper:
    """Map iRODS metadata to the Open Access Repository metadata format
    """
    def __init__(self, collection_avu, depositor, instance):
        self.collection_avu = collection_avu
        self.dataset_json = None
        self.md = None
        self.depositor = depositor
        self.instance = instance

        self.contributor_type_vocabulary = {}
        self.publication_id_type_vocabulary = {}

    def read_metadata(self):
        self.get_controlled_vocabulary()
        logger.info("--\t Map metadata")

        with open('etl/resources/template.json') as f:
            self.dataset_json = json.load(f)

        self.md = self.dataset_json['datasetVersion']

        self.add_author(self.instance.creator.full_name)

        if self.instance.identifier.pid is None:
            logger.error(f"{'--':<20}PID invalid")
        else:
            url = f"https://hdl.handle.net/{self.instance.identifier.pid}"
            self.add_alternative_url(url)

        if self.instance.description.description is None:
            self.add_description("")
        else:
            self.add_description(self.instance.description.description)

        self.add_date(self.instance.date.date)

        self.add_title(self.instance.title.title)

        self.add_subject()

        depositor = self.depositor.split("@")[0]
        self.add_depositor(depositor)

        contacts = []
        for contact in self.instance.contacts.contacts:
            pub = self.add_contact(contact.full_name, contact.email, contact.affiliation.label)
            contacts.append(pub)
        self.add_contacts(contacts)

        contributors = []
        for contributor in self.instance.contributors.contributors:
            contributor_type = ""
            if contributor.type.label in self.contributor_type_vocabulary:
                contributor_type = self.contributor_type_vocabulary[contributor.type.label]

            pub = self.add_contributor(contributor.full_name, contributor_type)
            contributors.append(pub)
        self.add_contributors(contributors)

        keywords = []
        for subject in self.instance.subjects.subjects:
            if subject.scheme_iri is not None:
                keyword = self.add_keyword(subject.keyword, subject.scheme_iri.rsplit("/", 1)[1], subject.value_uri.uri)
            else:
                keyword = self.add_keyword(subject.keyword, "", "")
            keywords.append(keyword)
        self.add_keywords(keywords)

        publications = []
        for resource in self.instance.related_resources.related_resources:
            value = resource.identifier
            url = ""
            if resource.identifier.startswith("http"):
                url = resource.identifier
                value = resource.identifier.rsplit("/", 1)[1]

            publication_id_type = ""
            if resource.identifier_type.label.lower() in self.publication_id_type_vocabulary:
                publication_id_type = self.publication_id_type_vocabulary[resource.identifier_type.label.lower()]

            pub = self.add_publication(value, publication_id_type, url)
            publications.append(pub)
        self.add_publications(publications)

        self.dataset_json['datasetVersion'] = self.md
        logger.debug(json.dumps(self.dataset_json, indent=4))

        return self.dataset_json

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

    def add_depositor(self, depositor, up=True):
        new = {
            "typeName": "depositor",
            "multiple": False,
            "value": depositor,
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

    def add_contributors(self, contributors, up=True):
        new = {
            "multiple": True,
            "typeClass": "compound",
            "typeName": "contributor",
            "value": contributors
        }
        if up:
            self.update_fields(new)
        return new

    @staticmethod
    def add_contributor(name, role):
        new = {
            "contributorType": {
                "typeName": "contributorType",
                "multiple": False,
                "typeClass": "controlledVocabulary",
                "value": role
            },
            "contributorName": {
                "typeName": "contributorName",
                "multiple": False,
                "typeClass": "primitive",
                "value": name
            }
        }
        return new

    def get_controlled_vocabulary(self):
        schema_url = (
            "https://raw.githubusercontent.com/IQSS/dataverse/develop/scripts/api/data/metadatablocks/citation.tsv"
        )
        with urllib.request.urlopen(schema_url) as url:
            controlled_vocabulary = url.readlines()

        for line in controlled_vocabulary:
            decoded_line = line.decode()
            if "publicationIDType" in decoded_line:
                value = decoded_line.split("\t")[2]
                self.publication_id_type_vocabulary[value.lower()] = value
            elif "contributorType" in decoded_line:
                value = decoded_line.split("\t")[2]
                self.contributor_type_vocabulary[value.lower()] = value
