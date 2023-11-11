from spaceone.inventory.plugin.collector.model.field import *
from spaceone.inventory.plugin.collector.model.dynamic_view import *


class MetadataGenerator:

    def __init__(self, metadata: dict):
        self.metadata = metadata

    def generate_metadata(self) -> dict:

        search_meta, table_meta, tabs_meta = self._separate_metadata()

        metadata = {
            'view': {
                'search': self._generate_search(search_meta),
                'table': self._generate_table(table_meta),
                'sub_data': self._generate_tabs(tabs_meta),
            }
        }

        return metadata

    def _separate_metadata(self):
        old_metadata_keys = list(self.metadata.keys())

        search_meta = self.metadata.get('search', {})
        table_meta = self.metadata.get('table', {})

        tabs_meta = {}
        for key in old_metadata_keys:
            if key.startswith('tabs.'):
                prefix, index = key.split('.', 1)
                tabs_meta[index] = self.metadata[key]

        tabs_meta = sorted(tabs_meta.items())
        return search_meta, table_meta, tabs_meta

    def _generate_search(self, search_meta: dict) -> list:
        return self._generate_fields(search_meta['fields'])

    def _generate_table(self, table_meta: dict) -> dict:
        table_metadata = self._generate_default_dynamic_view('Main Table', 'query-search-table')

        if 'sort' in table_meta:
            table_metadata['options']['default_sort'] = self._generate_sort(table_meta)

        if 'fields' in table_meta:
            table_metadata['options']['fields'] = self._generate_fields(table_meta['fields'])
        return {'layout': MainTableDynamicView(**table_metadata).dict()}

    def _generate_tabs(self, tabs_meta: list) -> dict:
        new_tabs_metadata = []

        for tab in tabs_meta:
            index, tab_meta = tab

            # generate multi dynamic view
            if 'items' in tab_meta:
                dynamic_view = self._generate_default_dynamic_view(
                    name=tab_meta['name'],
                    view_type=tab_meta['type']
                )

                inner_dynamic_views = []
                for inner_tab_meta in tab_meta['items']:
                    inner_dynamic_view = self._generate_default_dynamic_view(
                        name=inner_tab_meta['name'],
                        view_type=inner_tab_meta['type'],
                    )

                    if 'sort' in inner_tab_meta:
                        inner_dynamic_view['options']['default_sort'] = self._generate_sort(inner_tab_meta['sort'])
                    if 'fields' in inner_tab_meta:
                        inner_dynamic_view['options']['fields'] = self._generate_fields(inner_tab_meta['fields'])

                    inner_dynamic_views.append(inner_dynamic_view)
                    dynamic_view['options']['layouts'] = inner_dynamic_views
                new_tabs_metadata.append(dynamic_view)

            # generate single dynamic view
            elif 'fields' in tab_meta:
                dynamic_view = self._generate_default_dynamic_view(
                    name=tab_meta['name'],
                    view_type='list')
                dynamic_view['options']['layouts'] = []
                dynamic_view['options']['layouts'].append(
                    self._generate_default_dynamic_view(
                        name='',
                        view_type=tab_meta['type']
                    )
                )
                dynamic_view['options']['layouts'][0]['options']['fields'] = self._generate_fields(tab_meta['fields'])
                new_tabs_metadata.append(dynamic_view)
        return {'layouts': new_tabs_metadata}

    @staticmethod
    def _generate_default_dynamic_view(name, view_type, options=None):
        if options is None:
            options = {}

        return {
            'name': name,
            'type': view_type,
            'options': options
        }

    @staticmethod
    def _generate_sort(table: dict) -> dict:
        sort_field = table['sort']

        sort_option = {
            'key': sort_field['key'],
        }

        if 'desc' in sort_field:
            sort_option['desc'] = sort_field['desc']

        return Sort(**sort_option).dict()

    def _generate_fields(self, fields: list) -> list:
        gen_fields = []
        for field in fields:
            if 'type' not in field:
                gen_fields.append(self._generate_text_field(field))

            elif field['type'] == 'text':
                gen_fields.append(self._generate_text_field(field))
        return gen_fields

    @staticmethod
    def _generate_text_field(field: dict):
        if 'key' not in field:
            key = [key for key in field.keys() if key != 'type'][0]
            field['name'] = key
            field['key'] = field[key]
            del field[key]

        if 'label' not in field:
            field['type'] = 'text'

        return TextField(**field).dict()
