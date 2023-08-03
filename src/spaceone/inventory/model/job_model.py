from mongoengine import *

from spaceone.core.model.mongo_model import MongoModel
from spaceone.inventory.model.collector_model import Collector


class Error(EmbeddedDocument):
    error_code = StringField(max_length=128)
    message = StringField(max_length=2048)
    additional = DictField()


class Job(MongoModel):
    job_id = StringField(max_length=40, generate_id='job', unique=True)
    status = StringField(max_length=20, default='IN_PROGRESS',
                         choices=('CANCELED', 'IN_PROGRESS', 'FAILURE', 'SUCCESS', 'ERROR'))
    # filters = DictField()                                                                     # Deprecated
    total_tasks = IntField(min_value=0, max_value=65000, default=0)
    remained_tasks = IntField(max_value=65000, default=0)
    success_tasks = IntField(min_value=0, max_value=65000, default=0)
    failure_tasks = IntField(min_value=0, max_value=65000, default=0)
    # errors = ListField(EmbeddedDocumentField(Error, default=None, null=True))                 # Deprecated
    collector = ReferenceField('Collector', reverse_delete_rule=CASCADE)
    collector_id = StringField(max_length=40)
    secret_id = StringField(max_length=40, null=True, default=None)
    plugin_id = StringField(max_length=40)
    # project_id = StringField(max_length=40, default=None, null=True)                          # Deprecated
    projects = ListField(StringField(max_length=40), default=[])
    domain_id = StringField(max_length=255)
    created_at = DateTimeField(auto_now_add=True)
    updated_at = DateTimeField(auto_now=True)
    finished_at = DateTimeField(default=None, null=True)
    mark_error = IntField(min_value=0, default=0)

    meta = {
        'updatable_fields': [
            'status',
            'total_tasks',
            'remained_tasks',
            'success_tasks',
            'failure_tasks',
            # 'errors',
            'collector_id',
            'projects',
            'finished_at',
            'mark_error',
        ],
        'minimal_fields': [
            'job_id',
            'status',
            'created_at',
            'finished_at',
        ],
        'change_query_keys': {
            'user_projects': 'projects'
        },
        'reference_query_keys': {
            'collector': Collector
        },
        'ordering': [
            '-created_at'
        ],
        'indexes': [
            # 'job_id',
            'status',
            # 'collector',
            'collector_id',
            # 'project_id',
            # 'projects',
            'domain_id',
            'created_at',
            'finished_at'
        ]
    }

