from mongoengine import *

from spaceone.core.model.mongo_model import MongoModel
from spaceone.inventory.model.cloud_service_model import CloudService


class Note(MongoModel):
    note_id = StringField(max_length=40, generate_id='note', unique=True)
    note = StringField()
    record_id = StringField(max_length=40)
    cloud_service_id = StringField(max_length=40)
    created_by = StringField(max_length=40)
    cloud_service = ReferenceField('CloudService', reverse_delete_rule=CASCADE)
    domain_id = StringField(max_length=40)
    created_at = DateTimeField(auto_now_add=True)

    meta = {
        'updatable_fields': [
            'note'
        ],
        'minimal_fields': [
            'note_id',
            'note',
            'record_id',
            'cloud_service_id',
            'created_by'
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
            'record_id',
            'cloud_service_id',
            'created_by',
            'cloud_service',
            'domain_id'
        ]
    }
