import logging
import threading

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

    def __str__(self):
        return self.__dict__.__str__()

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
        self._lock = threading.Lock()

    def join(self, server: str):
        num = len(self._servers)
        channel = grpc.insecure_channel(server)
        stub = KVStoreStub(channel)
        if num == 0:
            self._servers[server] = KeyRange(KEYS_LOWER_THRESHOLD, KEYS_UPPER_THRESHOLD, stub)
        else:
            keys_per_server = (KEYS_UPPER_THRESHOLD + 1) // (num + 1)
            self._servers[server] = KeyRange((keys_per_server * num) + 1, KEYS_UPPER_THRESHOLD, stub)
            self._rearrange(server, keys_per_server)

    def _get_servers(self, side: str, server: str) -> int:
        keys = list(self._servers)
        logger.info(keys)
        index = keys.index(server)
        if side.upper() == "LEFT":
            return index
        else:
            return len(keys) - index - 1

    def _redistribute(self, server: str, direction: str, r: KeyRange):
        servers_list = list(self._servers)
        index = servers_list.index(server)
        keys_per_server = KEYS_UPPER_THRESHOLD // (len(self._servers) - 1)
        if direction.upper() == "LEFT":
            for i in reversed(range(1, index + 1)):
                logger.info(f"LEFT: redistributing {r.min} to {r.max}.")
                self._servers[server].stub.Redistribute(
                    RedistributeRequest(destination_server=servers_list[i - 1], lower_val=r.min,
                                        upper_val=r.max))
                logger.info("redistributed")
                self._servers[servers_list[i - 1]].max = r.max
                r.max = (keys_per_server * i) - 1
                r.min = self._servers[servers_list[i - 1]].min
                self._servers[servers_list[i - 1]].min = keys_per_server * i
                server = servers_list[i - 1]

        elif direction.upper() == "RIGHT":
            logger.info(f"RIGHT: redistributing {r.min} to {r.max}.")
            for i in range(index, len(self._servers)-1):
                self._servers[server].stub.Redistribute(
                    RedistributeRequest(destination_server=servers_list[i + 1], lower_val=r.min,
                                        upper_val=r.max))
                r.max = self._servers[servers_list[i + 1]].max
                r.min = keys_per_server * (i + 1) + 1
                self._servers[servers_list[i + 1]].max = r.min - 1
                self._servers[servers_list[i + 1]].min = i if i == 0 else keys_per_server * i + 1
                server = servers_list[i + 1]

    def leave(self, server: str):
        # supposing at least one server left
        self._lock.acquire()
        if len(self._servers) > 1:
            logger.info(f"I'm server number {list(self._servers).index(server)} of {len(self._servers)} servers and I leave.")
            keys_to_redistribute = self._servers[server]
            num_left = self._get_servers("LEFT", server)
            num_right = self._get_servers("RIGHT", server)
            logger.info(f"Left servers: {num_left}, Right servers: {num_right}")
            last_kps = KEYS_UPPER_THRESHOLD // len(self._servers)
            keys_per_server = KEYS_UPPER_THRESHOLD // (len(self._servers) - 1)
            division = keys_to_redistribute.min + (num_left * (keys_per_server - last_kps))
            if num_left != 0 and num_right != 0:
                keys_left = KeyRange(keys_to_redistribute.min, division)
                keys_right = KeyRange(division + 1, keys_to_redistribute.max)
                logger.info(f"Redistribute {keys_left} to the left and {keys_right} to the right.")
                self._redistribute(server, "LEFT", keys_left)
                self._redistribute(server, "RIGHT", keys_right)
                # remove server
            elif num_right == 0 and num_left != 0:
                logger.info(f"Redistribute {keys_to_redistribute} to the left.")
                self._redistribute(server, "LEFT", keys_to_redistribute)
            elif num_right != 0 and num_left == 0:
                logger.info(f"Redistribute {keys_to_redistribute} to the right.")
                self._redistribute(server, "RIGHT", keys_to_redistribute)
            logger.info("Keys reallocated.")
            self._servers.pop(server)
            self._lock.release()
            logger.info("Removed server.")
        else:
            logger.info("Can't remove the only server in the system.")

    def _rearrange(self, server: str, keys_per_server: int):
        keys = list(self._servers.keys())
        for i, key in enumerate(keys[:-1]):
            # can be threaded
            self._servers[key].min = i if i == 0 else keys_per_server * i + 1
            new_max = keys_per_server * (i + 1)
            logger.info(f"join: {len(self._servers)}")
            logger.info(self._servers[key].max)
            self._servers[key].stub.Redistribute(RedistributeRequest(destination_server=keys[i + 1], lower_val=new_max + 1,
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
        server = request.server
        role = self.shard_master_service.join_replica(server)
        return JoinReplicaResponse(role)

    def QueryReplica(self, request: QueryReplicaRequest, context) -> QueryResponse:
        key = request.key
        op = request.operation
        response = self.shard_master_service.query_replica(key, op)
        query_response = QueryResponse()
        if response is not None:
            query_response.server = response
        return query_response
