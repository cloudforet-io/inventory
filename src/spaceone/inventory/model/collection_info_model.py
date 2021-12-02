from mongoengine import *


class ChangeHistory(EmbeddedDocument):
    key = StringField()
    job_id = StringField(max_length=40, default=None, null=True)
    diff = DictField()
    updated_by = StringField(max_length=40)
    updated_at = DateTimeField()


class CollectionInfo(EmbeddedDocument):
    collectors = ListField(StringField(max_length=40))
    service_accounts = ListField(StringField(max_length=40))
    secrets = ListField(StringField(max_length=40))
    change_history = ListField(EmbeddedDocumentField(ChangeHistory))
    pinned_keys = ListField(StringField())

    def to_dict(self):
        return self.to_mongo()
