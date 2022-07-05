from spaceone.api.inventory.v1 import note_pb2, note_pb2_grpc
from spaceone.core.pygrpc import BaseAPI


class Note(BaseAPI, note_pb2_grpc.NoteServicer):

    pb2 = note_pb2
    pb2_grpc = note_pb2_grpc

    def create(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('NoteService', metadata) as note_service:
            return self.locator.get_info('NoteInfo', note_service.create(params))

    def update(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('NoteService', metadata) as note_service:
            return self.locator.get_info('NoteInfo', note_service.update(params))

    def delete(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('NoteService', metadata) as note_service:
            note_service.delete(params)
            return self.locator.get_info('EmptyInfo')

    def get(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('NoteService', metadata) as note_service:
            return self.locator.get_info('NoteInfo', note_service.get(params))

    def list(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('NoteService', metadata) as note_service:
            note_vos, total_count = note_service.list(params)
            return self.locator.get_info('NotesInfo',
                                         note_vos,
                                         total_count,
                                         minimal=self.get_minimal(params))

    def stat(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('NoteService', metadata) as note_service:
            return self.locator.get_info('StatisticsInfo', note_service.stat(params))
