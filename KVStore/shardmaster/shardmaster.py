import logging

import grpc

from KVStore.tests.utils import KEYS_LOWER_THRESHOLD, KEYS_UPPER_THRESHOLD
from KVStore.protos.kv_store_pb2 import RedistributeRequest, ServerRequest
from KVStore.protos.kv_store_pb2_grpc import KVStoreStub
from KVStore.protos.kv_store_shardmaster_pb2_grpc import ShardMasterServicer
from KVStore.protos.kv_store_shardmaster_pb2 import *

logger = logging.getLogger(__name__)


class ShardMasterService:
    def join(self, server: str):
        pass

    def leave(self, server: str):
        pass

    def _rearrange(self, server: str, keys_per_server: int):
        pass

    def query(self, key: int) -> str:
        pass

    def join_replica(self, server: str) -> Role:
        pass

    def query_replica(self, key: int, op: Operation) -> str:
        pass


class KeyRange:
    def __init__(self, minimum, maximum, stub=None):
        self._min = minimum
        self._max = maximum
        self._stub = stub

    @property
    def min(self):
        return self._min

    @property
    def max(self):
        return self._max

    @property
    def stub(self):
        return self._stub

    @min.setter
    def min(self, m):
        self._min = m

    @max.setter
    def max(self, m):
        self._max = m

    @stub.setter
    def stub(self, s):
        self._stub = s


class ShardMasterSimpleService(ShardMasterService):
    def __init__(self):
        self._servers = dict()

    def join(self, server: str):
        num = len(self._servers)
        channel = grpc.insecure_channel(server)
        stub = KVStoreStub(channel)
        if num == 0:
            self._servers[server] = KeyRange(KEYS_LOWER_THRESHOLD, KEYS_UPPER_THRESHOLD, stub)
        else:
            keys_per_server = KEYS_UPPER_THRESHOLD // num + 1
            self._servers[server] = KeyRange(keys_per_server * num, KEYS_UPPER_THRESHOLD, stub)
            self._rearrange(server, keys_per_server)

    def _get_servers(self, side: str, server: str) -> int:
        keys = list(self._servers.keys())
        index = keys.index(server)
        if side.upper() == "LEFT":
            return index
        else:
            return len(keys) - index - 1

    def leave(self, server: str):
        # supposing at least one server left
        num = len(self._servers) - 1
        keys_per_server = KEYS_UPPER_THRESHOLD // num
        # self._rearrange(server, keys_per_server)
        left = self._get_servers("LEFT", server)
        right = self._get_servers("RIGHT", server)
        remaining_keys = KEYS_UPPER_THRESHOLD - (keys_per_server * num)
        '''Generar lista de servidores, saber posicion del que hay que eliminar'''
        keys = list(self._servers.keys())
        index = keys.index(server)
        remove = keys.pop(index)
        rearrange = {}
        keys_left_rearrange = 0
        for i, key in enumerate(keys):
            if remaining_keys != 0:
                rearrange[key] = keys_per_server + 1
                remaining_keys -= 1
            else:
                rearrange[key] = keys_per_server
            if i < index:
                keys_left_rearrange += rearrange[key]
        keys_left = self._servers[keys[index - 1]].max

        # first distribution for left and right servers

        self._servers[remove].stub.Redistribute(
            RedistributeRequest(destination_server=keys[index - 1], lower_val=self._servers[remove].min,
                                upper_val=keys_left_rearrange - keys_left + self._servers[remove].min))
        self._servers[remove].stub.Redistribute(
            RedistributeRequest(destination_server=keys[index], lower_val=keys_left_rearrange - keys_left
                                                                          + self._servers[remove].min,
                                upper_val=self._servers[remove].max))

        for key in reversed(keys[:index]):
            pass

    def _rearrange(self, server: str, keys_per_server: int):
        keys = list(self._servers.keys())
        for i, key in enumerate(keys[:-1]):
            # can be threaded
            self._servers[key].min = (keys_per_server + 1) * i
            new_max = keys_per_server * (i + 1)
            self._servers[key].stub.Redistribute(RedistributeRequest(destination_server=keys[i + 1], lower_val=new_max,
                                                                     upper_val=self._servers[key].max))
            self._servers[key].max = new_max

    def query(self, key: int) -> str:
        num = len(self._servers)
        keys_per_server = KEYS_UPPER_THRESHOLD // num
        num_server = key // keys_per_server
        keys = list(self._servers.keys())
        return keys[num_server]


class ShardMasterReplicasService(ShardMasterSimpleService):
    def __init__(self, number_of_shards: int):
        super().__init__()
        """
        To fill with your code
        """

    def leave(self, server: str):
        """
        To fill with your code
        """

    def join_replica(self, server: str) -> Role:
        """
        To fill with your code
        """

    def query_replica(self, key: int, op: Operation) -> str:
        """
        To fill with your code
        """


class ShardMasterServicer(ShardMasterServicer):
    def __init__(self, shard_master_service: ShardMasterService):
        self.shard_master_service = shard_master_service
        """
        To fill with your code
        """

    def Join(self, request: JoinRequest, context) -> google_dot_protobuf_dot_empty__pb2.Empty:
        server = request.server
        self.shard_master_service.join(server)
        return google_dot_protobuf_dot_empty__pb2.Empty()

    def Leave(self, request: LeaveRequest, context) -> google_dot_protobuf_dot_empty__pb2.Empty:
        server = request.server
        self.shard_master_service.leave(server)
        return google_dot_protobuf_dot_empty__pb2.Empty()

    def Query(self, request: QueryRequest, context) -> QueryResponse:
        key = request.key
        response = self.shard_master_service.query(key)
        query_response = QueryResponse()
        if response is not None:
            query_response.server = response
        return query_response

    def JoinReplica(self, request: JoinRequest, context) -> JoinReplicaResponse:
        """
        To fill with your code
        """

    def QueryReplica(self, request: QueryReplicaRequest, context) -> QueryResponse:
        """
        To fill with your code
        """
