import factory

from spaceone.core import utils
from spaceone.inventory.model.collector_model import Collector, PluginInfo


class PluginInfoFactory(factory.mongoengine.MongoEngineFactory):

    class Meta:
        model = PluginInfo

    plugin_id = 'plugin-aws-inven-collector'
    provider = 'aws'
    upgrade_mode = 'AUTO'
    version = '1.0'
    options = {}
    metadata = {
        'filter_format': [],
        'supported_features': ['garbage_collection'],
        'supported_resource_type':[
            'inventory.CloudService',
            'inventory.CloudServiceType',
            'inventory.Region'
        ],
        'supported_schedules': ['hours']
    }


class CollectorFactory(factory.mongoengine.MongoEngineFactory):

    class Meta:
        model = Collector

    plugin_info = factory.SubFactory(PluginInfoFactory)
    collector_id = factory.LazyAttribute(lambda o: utils.generate_id('collector'))
    name = factory.LazyAttribute(lambda o: utils.random_string())
    state = 'ENABLED'
    provider = 'aws'
    is_public = True
    capability = {'supported_schema': ['aws_access_key']}
    priority = 10
    tags = {'xxx': 'yyy'}
    project_id = factory.LazyAttribute(lambda o: utils.generate_id('project'))
    domain_id = utils.generate_id('domain')
    created_at = factory.Faker('date_time')
    last_collected_at = factory.Faker('date_time')
