from mongoengine import *
from spaceone.core.model.mongo_model import MongoModel


class Resource(EmbeddedDocument):
    resource_type = StringField()
    filter = ListField(DictField())
    keyword = StringField(default=None, null=True)


class ResourceGroup(MongoModel):
    resource_group_id = StringField(max_length=40, generate_id='rsc-grp', unique=True)
    name = StringField(max_length=255)
    resources = ListField(EmbeddedDocumentField(Resource))
    options = DictField()
    tags = DictField()
    project_id = StringField(max_length=255)
    domain_id = StringField(max_length=255)
    created_at = DateTimeField(auto_now_add=True)

    meta = {
        'updatable_fields': [
            'name',
            'resources',
            'project_id',
            'options',
            'tags'
        ],
        'minimal_fields': [
            'resource_group_id',
            'name',
            'project_id'
        ],
        'change_query_keys': {
            'user_projects': 'project_id'
        },
        'ordering': [
            'name'
        ],
        'indexes': [
            # 'resource_group_id',
            'resources.resource_type',
            'project_id',
            'domain_id',
        ]
    }
