import logging

logger = logging.getLogger('iRODS to Dataverse')


class irodsMetadata():

    def __init__(self):
        self.title = None
        self.creator = None
        self.description = None
        self.date = None
        self.pid = None

        self.byteSize = None
        self.numFiles = None

        self.tissue = None
        self.technology = None
        self.organism = None
        self.factors = None
        self.protocol = None
        self.contact = None
        self.articles = None

        self.dataset_json = None
