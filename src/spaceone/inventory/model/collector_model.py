from mongoengine import *

from spaceone.core.model.mongo_model import MongoModel


class PluginInfo(EmbeddedDocument):
    plugin_id = StringField(max_length=40)
    version = StringField(max_length=255)
    options = DictField()
    metadata = DictField()
    secret_id = StringField(max_length=40, null=True)
    secret_group_id = StringField(max_length=40, null=True)
    provider = StringField(max_length=40, null=True)
    service_account_id = StringField(max_length=40, null=True)


class CollectorTag(EmbeddedDocument):
    key = StringField(max_length=255)
    value = StringField(max_length=255)


class Collector(MongoModel):
    collector_id = StringField(max_length=40, generate_id='collector', unique=True)
    name = StringField(max_length=255)
    state = StringField(max_length=20, default='ENABLED', choices=('ENABLED', 'DISABLED'))
    provider = StringField(max_length=40)
    is_public = BooleanField(default=True)
    capability = DictField()
    plugin_info = EmbeddedDocumentField(PluginInfo, default=None, null=True)
    priority = IntField(min_value=0, default=10, max_value=99)
    tags = ListField(EmbeddedDocumentField(CollectorTag))
    project_id = StringField(max_length=40)
    domain_id = StringField(max_length=255)
    created_at = DateTimeField(auto_now_add=True)
    last_collected_at = DateTimeField()

    meta = {
        'updatable_fields': [
            'name',
            'state',
            'plugin_info',
            'priority',
            'tags',
            'last_collected_at'
        ],
        'minimal_fields': [
            'collector_id',
            'name',
            'state',
            'provider',
            'capability',
            'plugin_info',
            'is_public',
            'project_id'

        ],
        'change_query_keys': {
            'plugin_id': 'plugin_info.plugin_id'
        },
        'ordering': [
            'name'
        ],
        'indexes': [
            'collector_id',
            'state',
            'provider',
            'priority',
            'project_id',
            'domain_id',
            ('tags.key', 'tags.value')
        ]
    }


class Scheduled(EmbeddedDocument):
    cron = StringField(max_length=1024, default=None, null=True)
    interval = IntField(min_value=1, max_value=3600, default=None, null=True)
    minutes = ListField(defualt=None, null=True)
    hours = ListField(default=None, null=True)


class Schedule(MongoModel):
    schedule_id = StringField(max_length=40, generate_id='sched', unique=True)
    name = StringField(max_length=255)
    collector = ReferenceField('Collector', reverse_delete_rule=CASCADE)
    collector_id = StringField(max_length=40)
    schedule = EmbeddedDocumentField(Scheduled, default=None, null=False)
    filters = DictField()
    collect_mode = StringField(max_length=8, default='ALL', choice=('ALL', 'CREATE', 'UPDATE'))
    created_at = DateTimeField(auto_now_add=True)
    last_scheduled_at = DateTimeField()
    domain_id = StringField(max_length=255)

    meta = {
        'updatable_fields': [
            'name',
            'collector_id',
            'collect_mode',
            'schedule',
            'last_scheduled_at'
        ],
        'minimal_fields': [
            'schedule_id',
            'name',
            'collect_mode',
            'schedule',
            'collector',
            'collector_id'
        ],
        'change_query_keys': {
            'collector_id': 'collector.collector_id'
        },
        'reference_query_keys': {
            'collector': Collector
        },
        'ordering': [
            'name'
        ],
        'indexes': [
            'schedule_id',
            'collector',
            'collector_id',
            'domain_id'
        ]
    }
