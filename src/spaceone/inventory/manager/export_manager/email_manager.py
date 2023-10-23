import tempfile
import logging
from spaceone.core import config
from spaceone.inventory.manager.export_manager import ExportManager

_LOGGER = logging.getLogger(__name__)


class EmailManager(ExportManager):

    def export(self, export_options, domain_id):
        with tempfile.TemporaryDirectory() as temp_dir:
            self._file_dir = temp_dir
            self._file_path = f'{self._file_dir}/{self._file_name}'
            self.make_file(export_options)
            return self.upload_file(domain_id)
