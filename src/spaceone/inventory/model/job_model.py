import datetime
from mongoengine import *

from spaceone.core.model.mongo_model import MongoModel
from spaceone.inventory.model.collector_model import Collector


class Job(MongoModel):
    job_id = StringField(max_length=40, generate_id='job', unique=True)
    state = StringField(max_length=20, default='CREATED',
                choices=('CREATED', 'CANCELED', 'IN_PROGRESS', 'FINISHED', 'FAILURE', 'TIMEOUT'))
    filters = DictField()
    #results = DictField()
    collect_mode = StringField(max_length=20, default='ALL',
                choices=('ALL', 'CREATE', 'UPDATE'))
    remained_tasks = IntField(min_value=0, max_value=65000, default=0)          # Number of remained from collector, 0 means No remained_task
    created_count = IntField(default=0)
    updated_count = IntField(default=0)
    statistics = DictField()
    collector = ReferenceField('Collector', reverse_delete_rule=NULLIFY)
    domain_id = StringField(max_length=255)
    created_at = DateTimeField(auto_now_add=True)
    finished_at = DateTimeField()
    last_updated_at = DateTimeField()

    meta = {
        'db_alias': 'default',
        'updatable_fields': [
            'state',
            'results',
            'remained_tasks',
            'created_count',
            'updated_count',
            'collected_resources',
            'finished_at',
            'last_updated_at',
        ],
        'exact_fields': [
            'job_id',
        ],
        'minimal_fields': [
            'job_id',
            'state',
            'created_at',
            'finished_at',
            'collect_mode',
        ],
        'change_query_keys': {
            'collector_id': 'collector.collector_id'
        },
        'reference_query_keys': {
            'collector': Collector
        },
        'ordering': [
            'domain_id'
        ],
        'indexes': [
            'job_id'
        ],
        'aggregate': {
            'lookup': {
                'collector': {
                    'from': 'collector'
                }
            }
        }
    }
 
    def update_collected_at(self, stat):
        stat.update({'finished_at': datetime.datetime.utcnow()})
        return self.update(stat)

