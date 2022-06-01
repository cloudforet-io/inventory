from mongoengine import *
from datetime import datetime
from spaceone.core.model.mongo_model import MongoModel


class Error(EmbeddedDocument):
    error_code = StringField()
    message = StringField()
    additional = DictField()


class JobTask(MongoModel):
    job_task_id = StringField(max_length=40, generate_id='job_task', unique=True)
    status = StringField(max_length=20, default='PENDING',
                         choices=('PENDING', 'CANCELED', 'IN_PROGRESS', 'SUCCESS', 'FAILURE'))
    created_count = IntField(default=0)
    updated_count = IntField(default=0)
    deleted_count = IntField(default=0)
    disconnected_count = IntField(default=0)
    failure_count = IntField(default=0)
    total_count = IntField(default=0)
    errors = ListField(EmbeddedDocumentField(Error, default=None, null=True))
    job_id = StringField(max_length=40)
    secret_id = StringField(max_length=40)
    collector_id = StringField(max_length=40, default=None, null=True)
    provider = StringField(max_length=40, default=None, null=True)
    service_account_id = StringField(max_length=40, default=None, null=True)
    project_id = StringField(max_length=255, default=None, null=True)
    domain_id = StringField(max_length=255)
    created_at = DateTimeField(auto_now_add=True)
    started_at = DateTimeField(default=None, null=True)
    finished_at = DateTimeField(default=None, null=True)

    meta = {
        'updatable_fields': [
            'status',
            'secret_id',
            'provider',
            'service_account_id',
            'project_id',
            'created_count',
            'updated_count',
            'deleted_count',
            'disconnected_count',
            'failure_count',
            'errors',
            'started_at',
            'finished_at'
        ],
        'minimal_fields': [
            'job_task_id',
            'status',
            'created_count',
            'updated_count',
            'deleted_count',
            'disconnected_count',
            'failure_count',
            'job_id',
            'created_at',
            'started_at',
            'finished_at',
        ],
        'change_query_keys': {
            'user_projects': 'project_id'
        },
        'ordering': [
            '-created_at'
        ],
        'indexes': [
            # 'job_task_id',
            'status',
            'job_id',
            'secret_id',
            'collector_id',
            'provider',
            'service_account_id',
            'project_id',
            'domain_id',
            'created_at',
            'finished_at'
        ]
    }
