import unittest
from unittest.mock import patch
from mongoengine import connect, disconnect

from spaceone.core.unittest.result import print_data
from spaceone.core.unittest.runner import RichTestRunner
from spaceone.core import config
from spaceone.core import utils
from spaceone.core.model.mongo_model import MongoModel
from spaceone.inventory.service.collector_service import CollectorService
from spaceone.inventory.model.collector_model import Collector
from spaceone.inventory.manager.plugin_manager import PluginManager
from spaceone.inventory.manager.collector_plugin_manager import CollectorPluginManager
from spaceone.inventory.manager.repository_manager import RepositoryManager
from spaceone.inventory.manager.collector_rule_manager import CollectorRuleManager
from spaceone.inventory.info.collector_info import *
from test.factory.collector_factory import CollectorFactory


class TestCollectorService(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        config.init_conf(package='spaceone.inventory')
        connect('test', host='mongomock://localhost')

        cls.domain_id = utils.generate_id('domain')
        cls.metadata = {
            'verb': 'create',
            'collector_id': utils.generate_id('collector'),
            'job_id': utils.generate_id('job'),
            'plugin_id': utils.generate_id('plugin'),
            'resource': utils.generate_id('resource'),
            'secret.service_account_id': utils.generate_id('sa'),
        }

        # cls.collector_vo = CollectorFactory(domain_id=cls.domain_id)

        super().setUpClass()

    @classmethod
    def tearDownClass(cls) -> None:
        super().tearDownClass()
        disconnect()

    @patch.object(MongoModel, 'connect', return_value=None)
    def tearDown(self, *args) -> None:
        print()
        print('(tearDown) ==> Delete collector rule')

    @patch.object(PluginManager, '__init__', return_value=None)
    @patch.object(PluginManager, 'get_endpoint', return_value=('grpc://plugin.spaceone.dev:50051', '1.0.0'))
    @patch.object(RepositoryManager, '__init__', return_value=None)
    @patch.object(RepositoryManager, 'get_plugin', return_value={})
    @patch.object(CollectorPluginManager, '__init__', return_value=None)
    @patch.object(CollectorPluginManager, 'init_plugin', return_value={})
    @patch.object(CollectorRuleManager, '__init__', return_value=None)
    @patch.object(CollectorRuleManager, 'create_collector_rule', return_value=None)
    @patch.object(MongoModel, 'connect', return_value=None)
    def test_create_collector(self, *args):
        params = {
            'name': 'Test Collector',
            'plugin_info': {
                'plugin_id': utils.generate_id('plugin'),
                'version': '1.0',
                'options': {},
                'metadata': {},
                'secret_filter': {},
            },
            'tags': {
                utils.random_string(): utils.random_string()
            },
            'schedule': {
                'hours': [1, 2, 3]
            },
            'domain_id': self.domain_id
        }

        collector_svc = CollectorService(metadata=self.metadata)
        collector_vo = collector_svc.create(params.copy())
        print(CollectorInfo(collector_vo))
        print_data(collector_vo.to_dict(), 'test_create_collector')

    @patch.object(PluginManager, '__init__', return_value=None)
    @patch.object(MongoModel, 'connect', return_value=None)
    def test_update_collector_rule(self, *args):
        collector_rule_vo = CollectorRuleFactory(domain_id=self.domain_id,
                                                 collector_id=self.collector_vo.collector_id,
                                                 collector=self.collector_vo)

        name = 'Update name'
        params = {
            'name': name,
            'collector_rule_id': collector_rule_vo.collector_rule_id,
            'tags': {
                utils.random_string(): utils.random_string()
            },
            'domain_id': self.domain_id
        }

        self.metadata.update({'verb': 'update'})
        collector_rule_svc = CollectorRuleService(metadata=self.metadata)
        update_collector_rule_vo = collector_rule_svc.update(params.copy())

        print_data(update_collector_rule_vo.to_dict(), 'test_update_collector_rule')
        print(CollectorRuleInfo(update_collector_rule_vo))

        self.assertEqual(name, update_collector_rule_vo.name)
        self.assertEqual(params['tags'], update_collector_rule_vo.tags)
        self.assertEqual(params['domain_id'], update_collector_rule_vo.domain_id)

    @patch.object(PluginManager, '__init__', return_value=None)
    @patch.object(MongoModel, 'connect', return_value=None)
    def test_delete_collector_rule(self, *args):
        collector_rule_vo = CollectorRuleFactory(domain_id=self.domain_id)
        params = {
            'collector_rule_id': collector_rule_vo.collector_rule_id,
            'domain_id': self.domain_id
        }

        self.metadata.update({'verb': 'delete'})
        collector_rule_svc = CollectorRuleService(metadata=self.metadata)
        result = collector_rule_svc.delete(params)

        self.assertIsNone(result)

    @patch.object(PluginManager, '__init__', return_value=None)
    @patch.object(MongoModel, 'connect', return_value=None)
    def test_get_collector_rule(self, *args):
        collector_rule_vo = CollectorRuleFactory(domain_id=self.domain_id)
        params = {
            'collector_rule_id': collector_rule_vo.collector_rule_id,
            'domain_id': self.domain_id
        }

        self.metadata.update({'verb': 'get'})
        collector_rule_svc = CollectorRuleService(metadata=self.metadata)
        get_collector_rule_vo = collector_rule_svc.get(params)

        print_data(get_collector_rule_vo.to_dict(), 'test_get_collector_rule')
        CollectorRuleInfo(get_collector_rule_vo)

        self.assertIsInstance(get_collector_rule_vo, CollectorRule)

    @patch.object(PluginManager, '__init__', return_value=None)
    @patch.object(MongoModel, 'connect', return_value=None)
    def test_list_collector_rules(self, *args):
        collector_rules_vos = CollectorRuleFactory.build_batch(10, domain_id=self.domain_id)
        list(map(lambda vo: vo.save(), collector_rules_vos))

        params = {
            'domain_id': self.domain_id
        }

        self.metadata.update({'verb': 'list'})
        collector_rule_svc = CollectorRuleService(metadata=self.metadata)
        collector_rule_vos, total_count = collector_rule_svc.list(params)
        CollectorRulesInfo(collector_rule_vos, total_count)

        self.assertEqual(len(collector_rule_vos), 10)
        self.assertIsInstance(collector_rule_vos[0], CollectorRule)
        self.assertEqual(total_count, 10)


if __name__ == "__main__":
    unittest.main(testRunner=RichTestRunner)
