import asyncio
import grpc

from grpclib.utils import graceful_exit

from proto import raft_pb2
from proto import raft_pb2_grpc

from kvstore import KVStore

import time
import random

from enum import Enum
from threading import Thread, Lock

class State(Enum):
    Follower = 1
    Candidate = 2
    Leader = 3

class RaftNode:
    def __init__(self, node_id, nodes):
        self.id = node_id
        self.nodes = nodes
        self.state = State.Follower
        self.current_term = 0
        self.voted_for = {}
        self.log = []
        self.commit_index = 0
        self.last_applied = 0
        self.next_index = {node: 1 for node in nodes}
        self.match_index = {node: 0 for node in nodes}
        self.request_vote_timeout = random.uniform(150, 300) / 100 # Convert to seconds
        self.election_timeout = self.request_vote_timeout + 0.1
        print(self.election_timeout)
        self.heartbeat_interval = 0.5
        self.last_heartbeat = time.time()
        self.kv_store = KVStore()
        self.lock = Lock()

    async def run(self):
        while True:
            if self.state == State.Follower:
                print("FOLLOWER")
                await self.follower_loop()
            if self.state == State.Candidate:
                print("CANDIDATE")
                await self.candidate_loop()
            if self.state == State.Leader:
                print("LEADER")
                await self.leader_loop()

    async def follower_loop(self):
        print("FOLLOWER LOOP")
        while self.state == State.Follower:
            if time.time() - self.last_heartbeat > self.election_timeout:
                self.state = State.Candidate
            await asyncio.sleep(0.1)

    async def candidate_loop(self):
        print("STARTING ELECTION")
        self.current_term += 1
        self.voted_for = self.id
        votes = 1
        
        vote_tasks = []
        for i, node in enumerate(self.nodes):
            if i == self.id:
                continue

            request = raft_pb2.RequestVoteRequest(
                term=self.current_term,
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
                    print("ACCEPTED")
                    votes += 1

            #we gave up our vote for a better leader
            if self.state == State.Follower:
                return

            if votes > len(self.nodes) // 2:
                self.state = State.Leader
            else:
                print("NOT ENOUGH VOTES", votes, len(self.nodes))
                self.state = State.Follower
                self.last_heartbeat = time.time()
        except asyncio.TimeoutError:
            self.state = State.Follower
            self.last_heartbeat = time.time()

    async def leader_loop(self):
        while self.state == State.Leader:
            print("LEADER LOOP")
            append_tasks = []
            for i, node in enumerate(self.nodes):
                if i == self.id:
                    continue
                append_tasks.append(self.send_append_entries(node))

            await asyncio.gather(*append_tasks)
            await asyncio.sleep(self.heartbeat_interval)

    async def send_vote_request(self, node, request):
        try:
            print("HERE SENDING", node)
            async with grpc.aio.insecure_channel(node) as channel:
                stub = raft_pb2_grpc.RaftConsensusStub(channel)
                result = await stub.RequestVote(request, timeout=self.election_timeout)
                return result
        except asyncio.TimeoutError:
            print(f"Vote request to {node} timed out")
        except grpc.RpcError as e:
            #print(f"Failed to send vote request to {node}: {e}")
            pass
        return None

    async def send_append_entries(self, node):
        next_idx = self.next_index[node]
        prev_log_index = next_idx - 1
        prev_log_term = 0
        entries = []
        if prev_log_index < len(self.log):
            prev_log_term = self.log[prev_log_index].term
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
            async with grpc.aio.insecure_channel(node) as channel:
                stub = raft_pb2_grpc.RaftConsensusStub(channel)

                response = await stub.AppendEntries(request, timeout=self.heartbeat_interval)

                if response and response.success:
                    self.next_index[node] = max(1, len(self.log))
                    self.match_index[node] = len(self.log) - 1
                else:
                    self.next_index[node] = max(1, self.next_index[node] - 1)

        except asyncio.TimeoutError:
            print(f"AppendEntries request to {node} timed out")
        except grpc.RpcError as e:
            #print(f"Failed to send AppendEntries to {node}: {e}")
            pass

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
        super().__init__()
        self.node = node

    def compare_logs(self, other_term, other_log_term, other_log_index):
        log_term = 0
        if self.node.log:
            log_term = self.node.log[-1].term

        if self.node.current_term > other_term:
            return -1
        if self.node.current_term < other_term:
            return 1

        if log_term > other_log_term:
            return -1
        if log_term < other_log_term:
            return 1

        log_len = len(self.node.log)
        if log_len > other_log_index:
            return -1
        if log_len < other_log_index:
            return 1

        return 0

    async def RequestVote(self, request, context):
        print("ACCEPTED VOTE REQUEST")

        response = raft_pb2.RequestVoteResponse(term=self.node.current_term, vote_granted=False)

        comp = self.compare_logs(request.term, request.last_log_term, request.last_log_index)

        if comp == -1:
            return response

        #we might drop our own vote for ourselves, but not other votes
        if comp == 1 and self.node.state != State.Follower:
            self.node.State = Follower
            self.node.last_heartbeat = time.time()
            if request.term in self.node.voted_for:
                self.node.voted_for.pop(request.term)

        cur_vote = self.node.voted_for.get(request.term, request.candidate_id)
        if cur_vote == request.candidate_id:
            self.node.current_term = request.term
            self.node.voted_for[request.term] = request.candidate_id
            self.node.last_heartbeat = time.time()

            response.term = self.node.current_term
            response.vote_granted = True
            print("GRANTED VOTE")

        return response

    async def AppendEntries(self, request, context):
        print("ACCEPTED HEARTBEAT")
        response = raft_pb2.AppendEntriesResponse(term=self.node.current_term, success=False)

        if request.term < self.node.current_term:
            return response
        if request.term > self.node.current_term:
            self.node.current_term = request.term

        if not request.entries:
            response.success = True
            self.node.last_heartbeat = time.time()
            return response 

        if request.prev_log_index >= len(self.node.log) or self.node.log[request.prev_log_index].term != request.prev_log_term:
            #mistake
            return response

        for i, entry in request.entries:
            cur = request.prev_log_index + i + 1

            if cur < len(self.node.log):
                if self.node.log[cur].term != request.term:
                    self.node.log = self.node.log[:cur]
                else:
                    continue

            self.node.log.append(entry)

        response.success = True
        self.node.last_heartbeat = time.time()
        return response 
