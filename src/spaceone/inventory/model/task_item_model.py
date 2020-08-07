from mongoengine import *
from datetime import datetime
from spaceone.core.model.mongo_model import MongoModel
from spaceone.inventory.model.job_model import Job
from spaceone.inventory.model.job_task_model import JobTask

class Error(EmbeddedDocument):
    error_code = StringField(max_length=128)
    message = StringField(max_length=2048)
    additional = DictField()

class TaskItem(MongoModel):
    resource_id = StringField(max_length=40)
    resource_type = StringField(max_length=40)
    references = ListField(StringField(max_length=512))
    job_id = StringField(max_length=40)
    job_task_id = StringField(max_length=40)
    state = StringField(max_length=20,
                        choices=('CREATE', 'UPDATE', 'DELETE', 'FAILURE'))
    error = EmbeddedDocumentField(Error, default=None, null=True)
    project_id = StringField(max_length=255)
    domain_id = StringField(max_length=255)

    meta = {
        'updatable_fields': [
            'resource_id',
            'resource_type',
            'job_id',
            'job_task_id',
            'state',
            'error'
        ],
        'exact_fields': [
            'resource_id'
        ],
        'minimal_fields': [
            'resource_id',
            'resource_type',
            'state'
        ],
        'change_query_keys': {
        },
        'reference_query_keys': {
        },
        'ordering': [
            'job_id'
        ],
        'indexes': [
            'job_task_id',
            'job_id',
            'resource_id',
            'domain_id'
        ]
    }

