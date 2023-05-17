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


    def _rearrange(self, server: str, keys_per_server: int):
        keys = list(self._servers.keys())
        for i, key in enumerate(keys[:-1]):
            # can be threaded
            self._servers[key].min = keys_per_server * i
            new_max = keys_per_server * (i + 1)
            self._servers[key].stub.Redistribute(destination_server=keys[i + 1], lower_val=new_max,
                                                 upper_val=self._servers[key].max)
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
        """
        To fill with your code
        """

    def Leave(self, request: LeaveRequest, context) -> google_dot_protobuf_dot_empty__pb2.Empty:
        """
        To fill with your code
        """

    def Query(self, request: QueryRequest, context) -> QueryResponse:
        """
        To fill with your code
        """

    def JoinReplica(self, request: JoinRequest, context) -> JoinReplicaResponse:
        """
        To fill with your code
        """

    def QueryReplica(self, request: QueryReplicaRequest, context) -> QueryResponse:
        """
        To fill with your code
        """
