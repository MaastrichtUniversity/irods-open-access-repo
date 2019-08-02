import logging
logger = logging.getLogger('iRODS to Dataverse')


class ZenodoMetadataMapper:

    def __init__(self, imetadata):
        self.imetadata = imetadata
        self.dataset_json = None
        self.md = None

    def read_metadata(self):
        logger.info("--\t Map metadata")
        if len(self.imetadata.description) < 3:
            self.imetadata.description = "Shorter than minimum length 3"
        if len(self.imetadata.title) < 3:
            self.imetadata.title = self.imetadata.title + "_size"
        metadata = {}
        metadata.update({'title': self.imetadata.title})
        metadata.update({'upload_type': 'dataset'})
        metadata.update({'description': self.imetadata.description})
        metadata.update({'access_right': 'closed'})
        metadata.update({'creators': [{'name': self.imetadata.creator}]})
        metadata.update({'related_identifiers': self.add_related_identifiers()})
        metadata.update({'references': self.add_references()})
        metadata.update({'subjects': self.add_subjects()})
        # metadata.update({'contributors': self.add_contributors()})
        metadata.update({'keywords': self.add_keywords()})

        data = {'metadata': metadata}
        return data

    def add_title(self):
        title = {'title': self.imetadata.title}
        # {'status': 400, 'message': 'Validation error.',
        #  'errors': [{'field': 'metadata.title', 'message': 'Shorter than minimum length 3.'}]}
        return title

    def add_description(self):
        if len(self.imetadata.description) < 3:
            self.imetadata.description = "Shorter than minimum length 3"
        description = {'description': self.imetadata.description}
        return description

    def add_creator(self):
        creator = {'creators': [{'name': self.imetadata.creator, 'affiliation': 'DataHub'}]}
        return creator

    def add_keywords(self):
        keywords = self.imetadata.factors
        return keywords

    def add_related_identifiers(self):
        pid = [{'relation': 'isAlternateIdentifier', 'identifier': self.imetadata.pid}]
        return pid

    def add_references(self):
        references = self.imetadata.articles
        return references

    def add_subjects(self):
        subjects = []
        if self.imetadata.tissue:
            subject = {"term": self.imetadata.tissue.get("name"),
                       "identifier": self.imetadata.tissue.get("uri"),
                       "scheme": "url"
                       }
            subjects.append(subject)

        if self.imetadata.technology:
            subject = {"term": self.imetadata.technology.get("name"),
                       "identifier": self.imetadata.technology.get("uri"),
                       "scheme": "url"
                       }
            subjects.append(subject)

        if self.imetadata.organism:
            subject = {"term": self.imetadata.organism.get("name"),
                       "identifier": self.imetadata.organism.get("uri"),
                       "scheme": "url"
                       }
            subjects.append(subject)
        return subjects

    def add_contributors(self):
        contacts = []
        for contact in self.imetadata.contact:
            logger.info(contact)
            if not contact:
                contacts.append({'name': '',
                                 'affiliation': '',
                                 'type': 'ContactPerson'})
            else:
                contacts.append({
                    'name': contact.get("firstName") + " " + contact.get("lastName"),
                    'affiliation': contact.get("affiliation"),
                    'type': 'ContactPerson'
                })
        return contacts
