from mongoengine import *


class ChangeHistory(EmbeddedDocument):
    key = StringField()
    job_id = StringField(max_length=40, default=None, null=True)
    diff = StringField(default=None, null=True)
    updated_by = StringField(max_length=40)
    updated_at = StringField()


class CollectionInfo(EmbeddedDocument):
    state = StringField(max_length=20, default='MANUAL', choices=('MANUAL', 'ACTIVE', 'DISCONNECTED'))
    collectors = ListField(StringField(max_length=40))
    service_accounts = ListField(StringField(max_length=40))
    secrets = ListField(StringField(max_length=40))
    change_history = ListField(EmbeddedDocumentField(ChangeHistory))
    pinned_keys = ListField(StringField())

    def to_dict(self):
        return self.to_mongo()
