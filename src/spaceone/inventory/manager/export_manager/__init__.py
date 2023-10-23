import tempfile
import logging
from datetime import datetime
import pandas as pd

from spaceone.core.manager import BaseManager
from spaceone.inventory.manager.file_manager import FileManager

_LOGGER = logging.getLogger(__name__)


class ExportManager(BaseManager):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._file_format = kwargs.get('file_format', 'EXCEL')

        file_name = kwargs.get('file_name', 'export')
        now = datetime.utcnow()

        if self._file_format == 'EXCEL':
            self._file_name = f'{file_name}_{now.strftime("%Y%m%d")}.xlsx'
        else:
            self._file_name = f'{file_name}_{now.strftime("%Y%m%d")}.zip'

        self._file_dir = None
        self._file_path = None

    def export(self, export_options, domain_id, **kwargs):
        with tempfile.TemporaryDirectory() as temp_dir:
            self._file_dir = temp_dir
            self._file_path = f'{self._file_dir}/{self._file_name}'
            self.make_file(export_options)
            return self.upload_file(domain_id)

    def make_file(self, export_options):
        idx = 0
        for export_option in export_options:
            if self._file_format == 'EXCEL':
                self._make_excel_file(idx, export_option['name'], export_option['results'])
            else:
                self._make_csv_file(idx, export_option['name'], export_option['results'])

            idx += 1

    def _make_excel_file(self, idx, name, results):
        df = pd.DataFrame(results)
        sheet_name = name.replace(' ', '')[:30]

        if idx == 0:
            with pd.ExcelWriter(self._file_path, mode='w', engine='openpyxl') as writer:
                df.to_excel(writer, sheet_name=sheet_name, index=False)
        else:
            with pd.ExcelWriter(self._file_path, mode='a', engine='openpyxl') as writer:
                df.to_excel(writer, sheet_name=sheet_name, index=False)

    def _make_csv_file(self, idx, name, results):
        pass

    def upload_file(self, domain_id):
        file_mgr: FileManager = self.locator.get_manager(FileManager)
        file_info = file_mgr.add_file({
            'name': self._file_name,
            'domain_id': domain_id
        })

        file_mgr.upload_file(self._file_path, file_info['upload_url'], file_info['upload_options'])
        download_file_info = file_mgr.get_download_url(file_info['file_id'], domain_id)

        return {
            'download_url': download_file_info['download_url']
        }
