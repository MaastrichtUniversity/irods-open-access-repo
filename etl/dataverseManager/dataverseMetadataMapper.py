import json
import logging
import urllib.request

logger = logging.getLogger("iRODS to Dataverse")


class MetadataMapper:
    """Map iRODS metadata to the Open Access Repository metadata format"""

    def __init__(self, depositor, instance):
        self.dataset_json = None
        self.md = None
        self.depositor = depositor
        self.instance = instance

        self.contributor_type_vocabulary = {}
        self.publication_id_type_vocabulary = {}

    def read_metadata(self):
        self.get_controlled_vocabulary()
        logger.info("--\t Map metadata")

        with open("etl/resources/template.json") as f:
            self.dataset_json = json.load(f)

        self.md = self.dataset_json["datasetVersion"]

        self.add_author(self.instance.creator.full_name)

        if self.instance.identifier.pid is None:
            logger.error(f"{'--':<20}PID invalid")
        else:
            self.add_alternative_url(self.instance.identifier.pid)

        if self.instance.description.description is None:
            self.add_description("")
        else:
            self.add_description(self.instance.description.description)

        self.add_date(self.instance.date.date)

        self.add_title(self.instance.title.title)

        self.add_subject()

        self.add_depositor(self.depositor.split("@")[0])

        self.map_contacts()
        self.map_contributors()
        self.map_keywords()
        self.map_publications()
        self.map_resource_type()

        self.dataset_json["datasetVersion"] = self.md
        logger.debug(json.dumps(self.dataset_json, indent=4))

        return self.dataset_json

    def update_fields(self, new):
        fields = self.md["metadataBlocks"]["citation"]["fields"]
        fields.append(new)

    def map_contacts(self):
        contacts = []
        for contact in self.instance.contacts.contacts:
            contacts.append(self.add_contact(contact.full_name, contact.email, contact.affiliation.label))
        self.add_contacts(contacts)

    def map_contributors(self):
        contributors = []
        for contributor in self.instance.contributors.contributors:
            # Map contributor_type to Dataverse controlled vocabulary for contributor type
            contributor_type = ""
            if contributor.type.label in self.contributor_type_vocabulary:
                contributor_type = self.contributor_type_vocabulary[contributor.type.label]

            contributors.append(self.add_contributor(contributor.full_name, contributor_type))
        self.add_contributors(contributors)

    def map_keywords(self):
        keywords = []
        for subject in self.instance.subjects.subjects:
            # Case where a subject has no free-text or ontology value
            if subject.scheme_iri is None and subject.keyword is None:
                continue
            if subject.scheme_iri is not None:
                keyword = self.add_keyword(subject.keyword, subject.scheme_iri.rsplit("/", 1)[1], subject.value_uri.uri)
            else:
                keyword = self.add_keyword(subject.keyword, "", "")
            keywords.append(keyword)
        self.add_keywords(keywords)

    def map_publications(self):
        publications = []
        for resource in self.instance.related_resources.related_resources:
            # Case where a related_resource has no identifier
            if resource.identifier is None:
                continue

            # Check if the resource identifier is a URI or not
            value = resource.identifier
            url = ""
            if value and value.startswith("http"):
                url = value
                value = value.rsplit("/", 1)[1]

            # Map publication_id_type to Dataverse controlled vocabulary for publication id type
            publication_id_type = ""
            if resource.identifier_type.label.lower() in self.publication_id_type_vocabulary:
                publication_id_type = self.publication_id_type_vocabulary[resource.identifier_type.label.lower()]

            publications.append(self.add_publication(value, publication_id_type, url))
        self.add_publications(publications)

    def map_resource_type(self):
        if self.instance.resource_type.type_detail is not None:
            self.add_kind_of_data([self.instance.resource_type.type_detail])

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
                        "typeClass": "primitive",
                    },
                    "authorName": {
                        "typeName": "authorName",
                        "multiple": False,
                        "value": author,
                        "typeClass": "primitive",
                    },
                }
            ],
            "typeClass": "compound",
        }
        if up:
            self.update_fields(new)
        return new

    def add_title(self, title, up=True):
        new = {"typeName": "title", "multiple": False, "value": title, "typeClass": "primitive"}
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
                        "typeClass": "primitive",
                    }
                }
            ],
            "typeClass": "compound",
        }
        if up:
            self.update_fields(new)
        return new

    def add_subject(self, up=True):
        new = {
            "typeName": "subject",
            "multiple": True,
            "value": ["Medicine, Health and Life Sciences"],
            "typeClass": "controlledVocabulary",
        }
        if up:
            self.update_fields(new)
        return new

    def add_date(self, date, up=True):
        new = {"typeName": "dateOfDeposit", "multiple": False, "value": date, "typeClass": "primitive"}
        if up:
            self.update_fields(new)
        return new

    def add_kind_of_data(self, resource_type, up=True):
        new = {"typeName": "kindOfData", "multiple": True, "typeClass": "primitive", "value": resource_type}
        if up:
            self.update_fields(new)
        return new

    def add_depositor(self, depositor, up=True):
        new = {"typeName": "depositor", "multiple": False, "value": depositor, "typeClass": "primitive"}
        if up:
            self.update_fields(new)
        return new

    def add_contacts(self, contacts, up=True):
        new = {"typeName": "datasetContact", "multiple": True, "value": contacts, "typeClass": "compound"}
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
                "typeClass": "primitive",
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
                "value": affiliation,
            },
            "datasetContactEmail": {
                "multiple": False,
                "typeClass": "primitive",
                "typeName": "datasetContactEmail",
                "value": email,
            },
            "datasetContactName": {
                "multiple": False,
                "typeClass": "primitive",
                "typeName": "datasetContactName",
                "value": name,
            },
        }
        return new

    def add_keywords(self, keywords, up=True):
        new = {"multiple": True, "typeClass": "compound", "typeName": "keyword", "value": keywords}
        if up:
            self.update_fields(new)
        return new

    @staticmethod
    def add_keyword(value, vocabulary, uri):
        new = {
            "keywordValue": {"multiple": False, "typeClass": "primitive", "typeName": "keywordValue", "value": value},
            "keywordVocabulary": {
                "multiple": False,
                "typeClass": "primitive",
                "typeName": "keywordVocabulary",
                "value": vocabulary,
            },
            "keywordVocabularyURI": {
                "multiple": False,
                "typeClass": "primitive",
                "typeName": "keywordVocabularyURI",
                "value": uri,
            },
        }
        return new

    def add_alternative_url(self, url, up=True):
        new = {"multiple": False, "typeClass": "primitive", "typeName": "alternativeURL", "value": url}
        if up:
            self.update_fields(new)
        return new

    def add_publications(self, publications, up=True):
        new = {"multiple": True, "typeClass": "compound", "typeName": "publication", "value": publications}
        if up:
            self.update_fields(new)
        return new

    @staticmethod
    def add_publication(value, publication_id_type, url):
        new = {
            "publicationIDNumber": {
                "multiple": False,
                "typeClass": "primitive",
                "typeName": "publicationIDNumber",
                "value": value,
            },
            "publicationURL": {"multiple": False, "typeClass": "primitive", "typeName": "publicationURL", "value": url},
        }

        if publication_id_type:
            new["publicationIDType"] = {
                "multiple": False,
                "typeClass": "controlledVocabulary",
                "typeName": "publicationIDType",
                "value": publication_id_type,
            }
        return new

    def add_contributors(self, contributors, up=True):
        new = {"multiple": True, "typeClass": "compound", "typeName": "contributor", "value": contributors}
        if up:
            self.update_fields(new)
        return new

    @staticmethod
    def add_contributor(name, contributor_type):
        new = {
            "contributorName": {
                "typeName": "contributorName",
                "multiple": False,
                "typeClass": "primitive",
                "value": name,
            }
        }
        if contributor_type:
            new["contributorType"] = {
                "typeName": "contributorType",
                "multiple": False,
                "typeClass": "controlledVocabulary",
                "value": contributor_type,
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
