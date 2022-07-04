from mongoengine import *

from spaceone.core.model.mongo_model import MongoModel
from spaceone.inventory.model.cloud_service_model import CloudService


class Record(MongoModel):
    record_id = StringField(max_length=40, generate_id='record', unique=True)
    cloud_service_id = StringField(max_length=40, required=True)
    action = StringField(max_length=20, choices=('CREATE', 'UPDATE', 'DELETE'), required=True)
    diff = ListField(DictField(required=True), default=[])
    user_id = StringField(max_length=255, default=None, null=True)
    collector_id = StringField(max_length=40, default=None, null=True)
    job_id = StringField(max_length=40, default=None, null=True)
    cloud_service = ReferenceField('CloudService', reverse_delete_rule=CASCADE)
    domain_id = StringField(max_length=40)
    created_at = DateTimeField(auto_now=True)

    meta = {
        'minimal_fields': [
            'record_id',
            'cloud_service_id',
            'action',
            'user_id',
            'collector_id',
            'job_id',
            'updated_at'
        ],
        'change_query_keys': {
            'user_projects': 'cloud_service.project_id'
        },
        'reference_query_keys': {
            'cloud_service': {
                'model': CloudService,
                'foreign_key': 'project_id'
            }
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
            'domain_id',
            'created_at'
        ]
    }
