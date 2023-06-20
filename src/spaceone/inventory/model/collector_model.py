from mongoengine import *

from spaceone.core.model.mongo_model import MongoModel


class PluginInfo(EmbeddedDocument):
    plugin_id = StringField(max_length=255)
    version = StringField(max_length=255)
    options = DictField()
    metadata = DictField()
    upgrade_mode = StringField(max_length=20, default='AUTO', choices=('AUTO', 'MANUAL'))
    # secret_id = StringField(max_length=40, default=None, null=True)             # Deprecated
    # secret_group_id = StringField(max_length=40, default=None, null=True)       # Deprecated
    # service_account_id = StringField(max_length=40, default=None, null=True)    # Deprecated
    # provider = StringField(max_length=40, default=None, null=True)              # Deprecated

    def to_dict(self):
        return dict(self.to_mongo())


class SecretFilter(EmbeddedDocument):
    state = StringField(max_length=20, default='ENABLED', choices=('ENABLED', 'DISABLED'))
    secrets = ListField(StringField(max_length=40), defualt=None, null=True)
    service_accounts = ListField(StringField(max_length=40), default=None, null=True)
    schemas = ListField(StringField(max_length=40), default=None, null=True)


class Scheduled(EmbeddedDocument):
    state = StringField(max_length=20, default='ENABLED', choices=('ENABLED', 'DISABLED'))
    # cron = StringField(max_length=1024, default=None, null=True)                # Deprecated
    # interval = IntField(min_value=1, max_value=3600, default=None, null=True)   # Deprecated
    # minutes = ListField(default=None, null=True)                                # Deprecated
    hours = ListField(default=None, null=True)


class Collector(MongoModel):
    collector_id = StringField(max_length=40, generate_id='collector', unique=True)
    name = StringField(max_length=255)
    # state = StringField(max_length=20, default='ENABLED', choices=('ENABLED', 'DISABLED'))  # Deprecated
    provider = StringField(max_length=40, default=None, null=True)
    capability = DictField()
    # is_public = BooleanField(default=True)                                                  # Deprecated
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


"""
Deprecated
"""
# class Schedule(MongoModel):
#     schedule_id = StringField(max_length=40, generate_id='sched', unique=True)
#     name = StringField(max_length=255)
#     collector = ReferenceField('Collector', reverse_delete_rule=CASCADE)
#     collector_id = StringField(max_length=40)
#     schedule = EmbeddedDocumentField(Scheduled, default=None, null=False)
#     filters = DictField()
#     collect_mode = StringField(max_length=8, default='ALL', choice=('ALL', 'CREATE', 'UPDATE'))
#     domain_id = StringField(max_length=255)
#     created_at = DateTimeField(auto_now_add=True)
#     last_scheduled_at = DateTimeField()
#
#     meta = {
#         'updatable_fields': [
#             'name',
#             'collector_id',
#             'collect_mode',
#             'schedule',
#             'last_scheduled_at'
#         ],
#         'minimal_fields': [
#             'schedule_id',
#             'name',
#             'collect_mode',
#             'schedule',
#             'collector',
#             'collector_id'
#         ],
#         'change_query_keys': {
#             'collector_id': 'collector.collector_id'
#         },
#         'reference_query_keys': {
#             'collector': Collector
#         },
#         'ordering': [
#             'name'
#         ],
#         'indexes': [
#             'schedule_id',
#             'collector',
#             'collector_id',
#             'domain_id'
#         ]
#     }
