from spaceone.core.manager import BaseManager
from spaceone.core.connector.space_connector import SpaceConnector


class IdentityManager(BaseManager):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.identity_conn: SpaceConnector = self.locator.get_connector('SpaceConnector', service='identity')

    def get_user(self, user_id, domain_id):
        return self.identity_conn.dispatch('User.get', {'user_id': user_id, 'domain_id': domain_id})

    def list_users(self, query, domain_id):
        return self.identity_conn.dispatch('User.list', {'query': query, 'domain_id': domain_id})

    def get_project(self, project_id, domain_id):
        return self.identity_conn.dispatch('Project.get', {'project_id': project_id, 'domain_id': domain_id})

    def list_projects(self, query, domain_id):
        return self.identity_conn.dispatch('Project.list', {'query': query, 'domain_id': domain_id})

    def list_project_groups(self, query, domain_id):
        return self.identity_conn.dispatch('ProjectGroup.list', {'query': query, 'domain_id': domain_id})

    def get_project_group(self, project_group_id, domain_id):
        return self.identity_conn.dispatch('ProjectGroup.get', {'project_group_id': project_group_id,
                                                                'domain_id': domain_id})

    def list_projects_in_project_group(self, project_group_id, domain_id, recursive=False, query=None):
        request = {
            'project_group_id': project_group_id,
            'domain_id': domain_id,
            'recursive': recursive
        }

        if query:
            request['query'] = query

        return self.identity_conn.dispatch('ProjectGroup.list_projects', request)

    def list_domains(self, query):
        return self.identity_conn.dispatch('Domain.list', {'query': query})
