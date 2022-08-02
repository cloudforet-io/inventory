from mongoengine import *

from spaceone.core.model.mongo_model import MongoModel


class Note(MongoModel):
    note_id = StringField(max_length=40, generate_id='note', unique=True)
    note = StringField()
    record_id = StringField(max_length=40)
    cloud_service_id = StringField(max_length=40)
    created_by = StringField(max_length=40)
    project_id = StringField(max_length=255, default=None, null=True)
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
            'created_by',
            'project_id'
        ],
        'change_query_keys': {
            'user_projects': 'project_id'
        },
        'ordering': [
            '-created_at'
        ],
        'indexes': [
            'record_id',
            'cloud_service_id',
            'created_by',
            'project_id',
            'domain_id'
        ]
    }
