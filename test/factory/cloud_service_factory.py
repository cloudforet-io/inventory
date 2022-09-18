import factory
from spaceone.core import utils

from spaceone.inventory.model import CloudService, CloudServiceTag


class CloudServiceTagFactory(factory.mongoengine.MongoEngineFactory):
    class Meta:
        model = CloudServiceTag

    cloud_service_id = factory.LazyAttribute(lambda o: utils.generate_id('cloud-svc'))
    k = utils.generate_id()
    v = utils.generate_id()
    provider = 'aws'
    project_id = ''
    domain_id = utils.generate_id('domain')
    created_at = factory.Faker('date_time')


class CloudServiceFactory(factory.mongoengine.MongoEngineFactory):
    class Meta:
        model = CloudService

    cloud_service_id = factory.LazyAttribute(lambda o: utils.generate_id('cloud-svc'))
    name = factory.LazyAttribute(lambda o: utils.random_string())
    account = '123456789012'
    cloud_service_group = 'EC2'
    cloud_service_type = 'Instance'
    provider = 'aws'
    data = {}
    tags = factory.List([
        factory.SubFactory(CloudServiceTagFactory) for _ in range(5)
    ])
    domain_id = utils.generate_id('domain')
    region_code = "ap-northeast-2"
    reference = {
        "resource_id": "resource-xxxx",
        "external_link": "https://aaa.bbb.ccc/"
    }
