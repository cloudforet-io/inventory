import factory
from spaceone.core import utils

from spaceone.inventory.model import CloudService


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
    domain_id = utils.generate_id('domain')
    region_code = "ap-northeast-2"
    reference = {
        "resource_id": "resource-xxxx",
        "external_link": "https://aaa.bbb.ccc/"
    }
