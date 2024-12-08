import asyncio
import grpc

from grpclib.client import Channel
from grpclib.server import Server
from grpclib.utils import graceful_exit

from proto import raft_pb2
from proto import raft_pb2_grpc

from kvstore import KVStore

import time
import random

from enum import Enum
from threading import Thread, Lock

class State(Enum):
    FOLLOWER = 1
    CANDIDATE = 2
    LEADER = 3

class RaftNode:
    def __init__(self, node_id, nodes):
        self.id = node_id
        self.nodes = nodes
        self.state = State.Follower
        self.current_term = 0
        self.voted_for = None
        self.log = []
        self.commit_index = 0
        self.last_applied = 0
        self.next_index = {node: 1 for node in nodes}
        self.match_index = {node: 0 for node in nodes}
        self.request_vote_timeout = random.uniform(150, 300) / 1000  # Convert to seconds
        self.election_timeout = self.request_vote_timeout + 0.1
        self.heartbeat_interval = 0.05  # 50ms
        self.last_heartbeat = time.time()
        self.kv_store = KVStore()
        self.lock = Lock()

    async def run(self):
        while True:
            if self.state == State.Follower:
                await self.follower_loop()
            if self.state == State.Candidate:
                await self.candidate_loop()
            if self.state == State.Leader:
                await self.leader_loop()

    async def follower_loop(self):
        while self.state == State.Follower:
            if time.time() - self.last_heartbeat > self.election_timeout:
                self.state = State.Candidate
            await asyncio.sleep(0.1)

    async def candidate_loop(self):
        new_term = self.current_term + 1
        self.voted_for = self.id
        votes = 1
        
        vote_tasks = []
        for node in self.nodes:
            if node == self.id:
                continue

            request = raft_pb2.RequestVoteRequest(
                term=new_term,
                candidate_id=self.id,
                last_log_index=len(self.log),
                last_log_term=self.log[-1].term if self.log else 0
            )
            vote_tasks.append(self.send_vote_request(node, request))

        try:
            responses = await asyncio.gather(*vote_tasks, return_exceptions=True)
            for response in responses:
                if isinstance(response, Exception):
                    continue  # Skip failed requests
                if response and response.vote_granted:
                    votes += 1
            
            if votes > len(self.nodes) // 2:
                self.state = State.Leader
                self.current_term += 1
            else:
                self.state = State.Follower
        except asyncio.TimeoutError:
            self.state = State.Follower

    async def leader_loop(self):
        while self.state == State.Leader:
            append_tasks = []
            for node in self.nodes:
                if node != self.id:
                    append_tasks.append(self.send_append_entries(node))
            
            await asyncio.gather(*append_tasks, return_exceptions=True)
            await asyncio.sleep(self.heartbeat_interval)

    async def send_vote_request(self, node, request):
        try:
            host, port = node.split(':')
            channel = Channel(host, int(port))
            stub = raft_pb2_grpc.RaftConsensusStub(channel)
            return await asyncio.wait_for(stub.RequestVote(request), timeout=self.election_timeout)
        except asyncio.TimeoutError:
            print(f"Vote request to {node} timed out")
            return None
        except grpc.RpcError as e:
            print(f"Failed to send vote request to {node}: {e}")
            return None

    async def send_append_entries(self, node):
        next_idx = self.next_index[node]
        prev_log_index = next_idx - 1
        prev_log_term = self.log[prev_log_index].term if prev_log_index > 0 else 0
        entries = self.log[next_idx:]

        request = raft_pb2.AppendEntriesRequest(
            term=self.current_term,
            leader_id=self.id,
            prev_log_index=prev_log_index,
            prev_log_term=prev_log_term,
            entries=entries,
            leader_commit=self.commit_index
        )

        try:
            host, port = node.split(':')
            channel = Channel(host, int(port))
            stub = raft_pb2_grpc.RaftConsensusStub(channel)

            response = await asyncio.wait_for(stub.AppendEntries(request), timeout=self.heartbeat_interval)
            if response.success:
                self.next_index[node] = len(self.log)
                self.match_index[node] = len(self.log) - 1
            else:
                self.next_index[node] = max(1, self.next_index[node] - 1)
        except asyncio.TimeoutError:
            print(f"AppendEntries request to {node} timed out")
        except grpc.RpcError as e:
            print(f"Failed to send AppendEntries to {node}: {e}")

    def get(self, key):
        return self.kv_store.get(key)

    def put(self, key, value):
        idx = len(self.log)

        command = raft_pb2.Command(
            command_type=raft_pb2.Command.CommandType.Update,
            key=key,
            value=value
        )
        entry = raft_pb2.LogEntry(
            term=self.current_term, 
            index = idx, 
            command=command
        )
        self.log.append(entry)
        return True

    def delete(self, key):
        idx = len(self.log)

        command = raft_pb2.Command(
            command_type=raft_pb2.Command.CommandType.Delete,
            key=key
        )
        entry = raft_pb2.LogEntry(
            term=self.current_term, 
            index = idx, 
            command=command
        )
        self.log.append(entry)
        return True

    def apply_log(self):
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            entry = self.log[self.last_applied]
            self.kv_store.apply(entry)

class RaftService(raft_pb2_grpc.RaftConsensusServicer):
    def __init__(self, node):
        self.node = node

    def log_is_greater(self, other_term, other_index):
        if other_term > self.current_term:
            return True
        if other_term < self.current_term:
            return False
        return other_index >= len(self.log)

    async def RequestVote(self, stream):
        request = await stream.recv_message()

        response = raft_pb2.RequestVoteResponse(term=self.node.current_term, vote_granted=False)

        #ensure leader is up to date
        if self.log_is_greater(request.last_log_term, request.last_log_index):
            if self.voted_for is None or request.term > self.current_term or s.voted_for == request.candidate_id:
                self.current_term = request.term
                self.voted_for = request.candidate_id
                self.last_heartbeat = time.time()

                response.term = self.current_term
                response.vote_granted = True

        await stream.send_message(response)

    async def work_append_entries(self, request):
        response = raft_pb2.AppendEntriesResponse(term=self.node.current_term, success=False)

        if request.term < self.current_term:
            return response

        if request.prev_log_index >= len(self.log) or self.log[request.prev_log_index].term != request.prev_log_term:
            #mistake
            return response

        for i, entry in request.entries:
            cur = request.prev_log_index + i + 1

            if cur < len(self.log):
                if self.log[cur].term != request.term:
                    self.log = self.log[:cur]
                else:
                    continue

            self.log.append(entry)


        return response 

    async def AppendEntries(self, stream):
        request = await stream.recv_message()
        response = await self.work_append_entries(request)
        await stream.send_message(response)
