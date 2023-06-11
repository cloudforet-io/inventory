from mongoengine import *

from spaceone.core.model.mongo_model import MongoModel
from spaceone.inventory.model.cloud_service_type_model import CloudServiceType
from spaceone.inventory.model.region_model import Region


class CloudServiceStats(MongoModel):
    query_set_id = StringField(max_length=40, required=True)
    key = StringField(max_length=255, required=True)
    value = FloatField(default=0)
    unit = StringField(max_length=50, default='Count')
    provider = StringField(max_length=255)
    cloud_service_group = StringField(max_length=255)
    cloud_service_type = StringField(max_length=255)
    ref_cloud_service_type = StringField(max_length=255)
    region_code = StringField(max_length=255, default=None, null=True)
    ref_region = StringField(max_length=255, default=None, null=True)
    account = StringField(max_length=255, default=None, null=True)
    additional_info = DictField()
    project_id = StringField(max_length=255, default=None, null=True)
    domain_id = StringField(max_length=40)
    timestamp = IntField(required=True)
    created_at = DateTimeField(required=True)
    created_year = StringField(max_length=20)
    created_month = StringField(max_length=20)
    created_date = StringField(max_length=20)

    meta = {
        'updatable_fields': [],
        'minimal_fields': [
            'key',
            'value',
            'unit',
            'provider',
            'cloud_service_group',
            'cloud_service_type',
            'project_id',
            'created_at'
        ],
        'change_query_keys': {
            'user_projects': 'project_id'
        },
        'reference_query_keys': {
            'ref_cloud_service_type': {
                'model': CloudServiceType,
                'foreign_key': 'ref_cloud_service_type'
            },
            'ref_region': {
                'model': Region,
                'foreign_key': 'ref_region'
            }
        },
        'ordering': [
            '-created_at'
        ],
        'indexes': [
            {
                "fields": ['domain_id', '-created_date', 'project_id', 'provider', 'cloud_service_group',
                           'cloud_service_type', 'key', 'value'],
                "name": "COMPOUND_INDEX_FOR_SEARCH_1"
            },
            {
                "fields": ['domain_id', '-created_date', 'project_id', 'ref_cloud_service_type', 'key', 'value'],
                "name": "COMPOUND_INDEX_FOR_SEARCH_2"
            },
            {
                "fields": ['domain_id', 'query_set_id', 'timestamp'],
                "name": "COMPOUND_INDEX_FOR_DELETE"
            }
        ]
    }


class MonthlyCloudServiceStats(MongoModel):
    query_set_id = StringField(max_length=40, required=True)
    key = StringField(max_length=255, required=True)
    value = FloatField(default=0)
    unit = StringField(max_length=50, default='Count')
    provider = StringField(max_length=255)
    cloud_service_group = StringField(max_length=255)
    cloud_service_type = StringField(max_length=255)
    ref_cloud_service_type = StringField(max_length=255)
    region_code = StringField(max_length=255, default=None, null=True)
    account = StringField(max_length=255, default=None, null=True)
    additional_info = DictField()
    project_id = StringField(max_length=255, default=None, null=True)
    domain_id = StringField(max_length=40)
    timestamp = IntField(required=True)
    created_year = StringField(max_length=20)
    created_month = StringField(max_length=20)

    meta = {
        'updatable_fields': [],
        'change_query_keys': {
            'user_projects': 'project_id'
        },
        'reference_query_keys': {
            'ref_cloud_service_type': {
                'model': CloudServiceType,
                'foreign_key': 'ref_cloud_service_type'
            },
            'ref_region': {
                'model': Region,
                'foreign_key': 'ref_region'
            }
        },
        'ordering': [
            '-created_at'
        ],
        'indexes': [
            {
                "fields": ['domain_id', '-created_month', 'project_id', 'provider', 'cloud_service_group',
                           'cloud_service_type', 'key', 'value'],
                "name": "COMPOUND_INDEX_FOR_SEARCH_1"
            },
            {
                "fields": ['domain_id', '-created_month', 'project_id', 'ref_cloud_service_type', 'key', 'value'],
                "name": "COMPOUND_INDEX_FOR_SEARCH_2"
            },
            {
                "fields": ['domain_id', 'query_set_id', 'timestamp'],
                "name": "COMPOUND_INDEX_FOR_DELETE"
            }
        ]
    }


class CloudServiceStatsQueryHistory(MongoModel):
    query_hash = StringField(max_length=255)
    query_options = DictField(default={})
    domain_id = StringField(max_length=40)
    granularity = StringField(max_length=40, default=None, null=True)
    start = DateField(defulat=None, null=True)
    end = DateField(default=None, null=True)
    updated_at = DateTimeField(auto_now=True)

    meta = {
        'updatable_fields': [
            'start',
            'end',
            'updated_at'
        ],
        'indexes': [
            {
                "fields": ['domain_id', 'query_hash'],
                "name": "COMPOUND_INDEX_FOR_SEARCH"
            },
        ]
    }