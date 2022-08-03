from mongoengine import *

from spaceone.core.model.mongo_model import MongoModel
from spaceone.inventory.model.cloud_service_model import CloudService


class RecordDiff(EmbeddedDocument):
    key = StringField(required=True)
    before = DynamicField(default=None, null=True)
    after = DynamicField(default=None, null=True)
    type = StringField(max_length=20, choices=('ADDED', 'CHANGED', 'DELETED'), required=True)

    def to_dict(self):
        return dict(self.to_mongo())


class Record(MongoModel):
    record_id = StringField(max_length=40, generate_id='record', unique=True)
    cloud_service_id = StringField(max_length=40, required=True)
    action = StringField(max_length=20, choices=('CREATE', 'UPDATE', 'DELETE'), required=True)
    diff = ListField(EmbeddedDocumentField(RecordDiff), default=[])
    diff_count = IntField(default=0)
    user_id = StringField(max_length=255, default=None, null=True)
    collector_id = StringField(max_length=40, default=None, null=True)
    job_id = StringField(max_length=40, default=None, null=True)
    updated_by = StringField(max_length=40, choices=('COLLECTOR', 'USER'), default=None, null=True)
    project_id = StringField(max_length=255, default=None, null=True)
    domain_id = StringField(max_length=40)
    created_at = DateTimeField(auto_now=True)

    meta = {
        'minimal_fields': [
            'record_id',
            'cloud_service_id',
            'action',
            'diff_count',
            'user_id',
            'collector_id',
            'job_id',
            'updated_by',
            'created_at',
            'project_id'
        ],
        'change_query_keys': {
            'user_projects': 'project_id'
        },
        'ordering': [
            '-created_at'
        ],
        'indexes': [
            'cloud_service_id',
            'action',
            'user_id',
            'collector_id',
            'job_id',
            'updated_by',
            'project_id',
            'domain_id',
            'created_at',
            'diff.key'
        ]
    }
