from mongoengine import *
from spaceone.core.model.mongo_model import MongoModel


class CollectionState(MongoModel):
    collector_id = StringField(max_length=40)
    job_id = StringField(max_length=40)
    resource_id = StringField(max_length=40)
    disconnected_count = IntField(default=0)
    collector = ReferenceField('Collector', default=None, null=True, reverse_delete_rule=CASCADE)
    server = ReferenceField('Server', default=None, null=True, reverse_delete_rule=CASCADE)
    cloud_service = ReferenceField('CloudService', default=None, null=True, reverse_delete_rule=CASCADE)
    cloud_service_type = ReferenceField('CloudServiceType', default=None, null=True, reverse_delete_rule=CASCADE)
    region = ReferenceField('Region', default=None, null=True, reverse_delete_rule=CASCADE)
    domain_id = StringField(max_length=40)
    updated_at = DateTimeField(auto_now=True)

    meta = {
        'updatable_fields': [
            'job_id',
            'disconnected_count',
            'updated_at'
        ],
        'indexes': [
            'collector_id',
            'job_id',
            'resource_id',
            'disconnected_count',
            'collector',
            'server',
            'cloud_service',
            'cloud_service_type',
            'region',
            'domain_id'
        ]
    }

