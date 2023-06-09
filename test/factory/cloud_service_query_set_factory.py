import factory
from spaceone.core import utils

from spaceone.inventory.model.cloud_service_query_set_model import CloudServiceQuerySet


class CloudServiceQuerySetFactory(factory.mongoengine.MongoEngineFactory):
    class Meta:
        model = CloudServiceQuerySet

    query_set_id = factory.LazyAttribute(lambda o: utils.generate_id('query-set'))
    name = factory.LazyAttribute(lambda o: utils.random_string())
    query_options = {
        'group_by': ['data.severity'],
        'fields': {
            'fail_finding_count': {
                'key': 'data.stats.findings.fail',
                'operator': 'sum'
            },
            'pass_finding_count': {
                'key': 'data.stats.findings.pass',
                'operator': 'sum'
            }
        }
    }
    query_type = 'MANAGED'
    unit = {
        'fail_finding_count': 'Count',
        'pass_finding_count': 'Count'
    }
    provider = 'aws'
    cloud_service_group = 'Prowler'
    cloud_service_type = 'CIS-1.5'
    tags = {
        'foo': 'bar'
    }
    domain_id = utils.generate_id('domain')
