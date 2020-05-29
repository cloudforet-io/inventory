from mongoengine import *


class ReferenceResource(EmbeddedDocument):
    resource_id = StringField(default=None, null=True)
    external_link = StringField(default=None, null=True)

    def to_dict(self):
        return self.to_mongo()
