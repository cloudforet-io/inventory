from mongoengine import *

from spaceone.core.model.mongo_model import MongoModel


class NetworkType(MongoModel):
    network_type_id = StringField(max_length=40, generate_id='ntype', unique=True)
    name = StringField(max_length=255)
    tags = DictField()
    domain_id = StringField(max_length=255)
    created_at = DateTimeField(auto_now_add=True)

    meta = {
        'updatable_fields': [
            'name',
            'tags'
        ],
        'exact_fields': [
            'network_type_id',
        ],
        'minimal_fields': [
            'network_type_id',
            'name'
        ],
        'ordering': [
            'name'
        ],
        'indexes': [
            'network_type_id',
            'domain_id'
        ]
    }
