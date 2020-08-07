from mongoengine import *
from datetime import datetime
from spaceone.core.model.mongo_model import MongoModel
from spaceone.inventory.model.job_model import Job

class SecretInfo(EmbeddedDocument):
    secret_id = StringField(max_length=40)
    name = StringField(max_length=255)
    provider = StringField(max_length=40)
    service_account_id = StringField(max_length=40)
    project_id = StringField(max_length=40)

class Error(EmbeddedDocument):
    error_code = StringField(max_length=128)
    message = StringField(max_length=2048)
    additional = DictField()

class JobTask(MongoModel):
    job_task_id = StringField(max_length=40, generate_id='job_task', unique=True)
    state = StringField(max_length=20, default='PENDING',
                        choices=('PENDING', 'IN_PROGRESS', 'SUCCESS', 'FAILURE'))
    job = ReferenceField('Job', reverse_delete_rule=NULLIFY)
    secret_info = EmbeddedDocumentField(SecretInfo, default=None, null=True)
    errors = ListField(EmbeddedDocumentField(Error, default=None, null=True))
    created_at = DateTimeField(auto_now_add=True)
    started_at = DateTimeField()
    finished_at = DateTimeField()
    project_id = StringField(max_length=255)
    domain_id = StringField(max_length=255)

    meta = {
        'updatable_fields': [
            'state',
            'secret_info',
            'errors',
            'started_at',
            'finished_at'
        ],
        'exact_fields': [
            'state'
        ],
        'minimal_fields': [
            'job_task_id',
            'state'
        ],
        'change_query_keys': {
            'job': 'job.job_id'
        },
        'reference_query_keys': {
            'job': Job
        },
        'ordering': [
            'name'
        ],
        'indexes': [
            'job_task_id',
            'state',
            'domain_id'
        ]
    }

    def update_started_at(self):
        self.update({
            'started_at': datetime.utcnow()
        })

    def update_finished_at(self):
        self.update({
            'finished_at': datetime.utcnow()
        })
