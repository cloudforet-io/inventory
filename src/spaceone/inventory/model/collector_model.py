from mongoengine import *

from spaceone.core.model.mongo_model import MongoModel


class PluginInfo(EmbeddedDocument):
    plugin_id = StringField(max_length=255)
    version = StringField(max_length=255)
    options = DictField()
    metadata = DictField()
    upgrade_mode = StringField(max_length=20, default='AUTO', choices=('AUTO', 'MANUAL'))

    def to_dict(self):
        return dict(self.to_mongo())


class SecretFilter(EmbeddedDocument):
    state = StringField(max_length=20, default='ENABLED', choices=('ENABLED', 'DISABLED'))
    secrets = ListField(StringField(max_length=40), defualt=None, null=True)
    service_accounts = ListField(StringField(max_length=40), default=None, null=True)
    schemas = ListField(StringField(max_length=40), default=None, null=True)
    exclude_secrets = ListField(StringField(max_length=40), defualt=None, null=True)
    exclude_service_accounts = ListField(StringField(max_length=40), default=None, null=True)
    exclude_schemas = ListField(StringField(max_length=40), default=None, null=True)

class Scheduled(EmbeddedDocument):
    state = StringField(max_length=20, default='ENABLED', choices=('ENABLED', 'DISABLED'))
    hours = ListField(default=None, null=True)


class Collector(MongoModel):
    collector_id = StringField(max_length=40, generate_id='collector', unique=True)
    name = StringField(max_length=255)
    provider = StringField(max_length=40, default=None, null=True)
    capability = DictField()
    plugin_info = EmbeddedDocumentField(PluginInfo, default=None, null=True)
    schedule = EmbeddedDocumentField(Scheduled, default=None, null=False)
    secret_filter = EmbeddedDocumentField(SecretFilter, default=None, null=True)
    priority = IntField(min_value=0, default=10, max_value=99)
    tags = DictField()
    domain_id = StringField(max_length=255)
    created_at = DateTimeField(auto_now_add=True)
    last_collected_at = DateTimeField()

    meta = {
        'updatable_fields': [
            'name',
            'plugin_info',
            'schedule',
            'secret_filter',
            'tags',
            'last_collected_at'
        ],
        'minimal_fields': [
            'collector_id',
            'name',
            'provider',
            'capability',
            'plugin_info'
        ],
        'change_query_keys': {
            'plugin_id': 'plugin_info.plugin_id'
        },
        'ordering': [
            'name'
        ],
        'indexes': [
            'provider',
            'priority',
            'domain_id',
        ]
    }
