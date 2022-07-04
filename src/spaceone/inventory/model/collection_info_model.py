from mongoengine import *


class CollectionInfo(EmbeddedDocument):
    collectors = ListField(StringField(max_length=40))
    service_accounts = ListField(StringField(max_length=40))
    secrets = ListField(StringField(max_length=40))

    def to_dict(self):
        return self.to_mongo()
