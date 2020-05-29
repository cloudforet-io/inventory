from mongoengine import *

from spaceone.core.model.mongo_model import MongoModel


class PluginInfo(EmbeddedDocument):
    plugin_id = StringField(max_length=40)
    version = StringField(max_length=255)
    options = DictField()
    secret_id = StringField(max_length=40, null=True)
    secret_group_id = StringField(max_length=40, null=True)
    provider = StringField(max_length=40)


class Collector(MongoModel):
    collector_id = StringField(max_length=40, generate_id='collector', unique=True)
    name = StringField(max_length=255)
    provider = StringField(max_length=40)
    capability = DictField()
    plugin_info = EmbeddedDocumentField(PluginInfo, default=None, null=True)
    state = StringField(max_length=20, default='ENABLED', choices=('ENABLED', 'DISABLED'))
    priority = IntField(min_value=0, default=10, max_value=99)
    tags = DictField()
    domain_id = StringField(max_length=255)
    created_at = DateTimeField(auto_now_add=True)
    last_collected_at = DateTimeField()

    meta = {
        'db_alias': 'default',
        'updatable_fields': [
            'name',
            'plugin_info',
            'state',
            'priority',
            'tags',
            'last_collected_at'
        ],
        'exact_fields': [
            'collector_id',
            'name',
            'priority',
            'domain_id',
        ],
        'minimal_fields': [
            'collector_id',
            'name',
            'plugin_info',
            'state',
            'provider',
            'capability'

        ],
        'change_query_keys': {
            'plugin_id': 'plugin_info.plugin_id'
        },
        'ordering': [
            'name'
        ],
        'indexes': [
            'collector_id'
        ]
    }


class Scheduled(EmbeddedDocument):
    cron = StringField(max_length=1024)
    interval = IntField(min_value=1, max_value=60)
    minutes = ListField()
    hours = ListField()


class Schedule(MongoModel):
    schedule_id = StringField(max_length=40, generate_id='sched', unique=True)
    name = StringField(max_length=255)
    collector = ReferenceField('Collector', reverse_delete_rule=CASCADE)
    schedule = EmbeddedDocumentField(Scheduled, default=None, null=False)
    filters = DictField()
    collect_mode = StringField(max_length=8, default='ALL', choice=('ALL', 'CREATE', 'UPDATE'))
    created_at = DateTimeField(auto_now_add=True)
    last_scheduled_at = DateTimeField()
    domain_id = StringField(max_length=255)

    meta = {
        'db_alias': 'default',
        'updatable_fields': [
            'name',
            'collect_mode',
            'schedule',
            'last_scheduled_at'
        ],
        'exact_fields': [
            'schedule_id',
            'name',
            'domain_id',
        ],
        'minimal_fields': [
            'schedule_id',
            'name',
            'collect_mode',
            'schedule',
            'collector'
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
            'schedule_id'
        ]
    }
