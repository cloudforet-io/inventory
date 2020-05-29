from mongoengine import *


class UpdateHistory(EmbeddedDocument):
    key = StringField()
    updated_by = StringField(max_length=40)
    updated_at = IntField()
    service_account_id = StringField(max_length=40, default=None, null=True)
    secret_id = StringField(max_length=40, default=None, null=True)


class CollectionInfo(EmbeddedDocument):
    state = StringField(max_length=20, default='MANUAL', choices=('MANUAL', 'ACTIVE', 'DISCONNECTED'))
    collectors = ListField(StringField(max_length=40))
    service_accounts = ListField(StringField(max_length=40))
    secrets = ListField(StringField(max_length=40))
    update_history = ListField(EmbeddedDocumentField(UpdateHistory))
    pinned_keys = ListField(StringField())

    def to_dict(self):
        return self.to_mongo()
