import asyncio
import grpc
import traceback

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
        self.update_period = 2
        self.heartbeat_interval = 0.5
        self.last_heartbeat = time.time()
        self.kv_store = KVStore()
        self.lock = Lock()

    def apply_log(self):
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            entry = self.log[self.last_applied - 1]
            print(f"APPLYING {self.last_applied} {entry}")
            self.kv_store.apply(entry.command)

    def commit_indices(self):
        #to be used only for leader
        while self.commit_index < len(self.log):
            calc = 1 #count self

            for i, node in enumerate(self.nodes):
                if i == self.id:
                    continue
                if self.match_index[node] > self.commit_index:
                    calc += 1

            if calc > len(self.nodes) // 2:
                self.commit_index += 1
            else:
                break

    async def update_loop(self):
        try:
            while True:
                if self.state == State.Leader:
                    self.commit_indices()
                self.apply_log()
                await asyncio.sleep(self.update_period)
        except Exception:
            print(traceback.format_exc())

    async def run(self):
        self.update_task = asyncio.create_task(self.update_loop())

        while True:
            if self.state == State.Follower:
                await self.follower_loop()
            if self.state == State.Candidate:
                await self.candidate_loop()
            if self.state == State.Leader:
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
                    votes += 1

            #we gave up our vote for a better leader
            if self.state == State.Follower:
                return

            if votes > len(self.nodes) // 2:
                print("BECAME LEADER")
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
            append_tasks = []
            for i, node in enumerate(self.nodes):
                if i == self.id:
                    continue
                append_tasks.append(self.send_append_entries(node))

            await asyncio.gather(*append_tasks)
            await asyncio.sleep(self.heartbeat_interval)

    async def send_vote_request(self, node, request):
        try:
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
        if prev_log_index <= len(self.log) and prev_log_index > 0:
            prev_log_term = self.log[prev_log_index - 1].term
            entries = self.log[prev_log_index:]
        elif prev_log_index == 0:
            entries = self.log[:]

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
                    self.next_index[node] = len(self.log) + 1
                    self.match_index[node] = len(self.log)
                else:
                    self.next_index[node] = max(1, self.next_index[node] - 1)

        except asyncio.TimeoutError:
            print(f"AppendEntries request to {node} timed out")
        except grpc.RpcError as e:
            #print(f"Failed to send AppendEntries to {node}: {e}")
            pass

    def get(self, key):
        return self.kv_store.get(key)

    async def create(self, key, value):
        idx = len(self.log)

        command = raft_pb2.Command(
            command_type=raft_pb2.Command.CommandType.Create,
            key=key,
            value=value
        )
        entry = raft_pb2.LogEntry(
            term=self.current_term, 
            index = idx, 
            command=command
        )

        return await self.add_entry(entry)

    async def put(self, key, value):
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

        return await self.add_entry(entry)

    async def compare_swap(self, key, old_value, value):
        idx = len(self.log)

        command = raft_pb2.Command(
            command_type=raft_pb2.Command.CommandType.CompareSwap,
            key=key,
            value=value,
            old_value=old_value
        )
        entry = raft_pb2.LogEntry(
            term=self.current_term, 
            index = idx, 
            command=command
        )

        return await self.add_entry(entry)

    async def delete(self, key):
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

        return await self.add_entry(entry)


    async def add_entry(self, entry):
        self.log.append(entry)
        needed_index = len(self.log)

        while True:
            if self.state != State.Leader:
                return False
            if self.commit_index >= needed_index:
                return True
            await asyncio.sleep(self.update_period)

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
        print("INCOMING VOTE REQUEST", request)

        response = raft_pb2.RequestVoteResponse(term=self.node.current_term, vote_granted=False)

        comp = self.compare_logs(request.term, request.last_log_term, request.last_log_index)

        if comp == -1:
            return response

        #we might drop our own vote for ourselves, but not other votes
        if comp == 1 and self.node.state != State.Follower:
            self.node.State = Follower
            self.node.current_term = request.term
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
            print(f"GRANTED VOTE TO {request.candidate_id}")

        return response

    async def AppendEntries(self, request, context):
        response = raft_pb2.AppendEntriesResponse(term=self.node.current_term, success=False)

        if request.term < self.node.current_term:
            return response
        if request.term > self.node.current_term:
            self.node.current_term = request.term
            #cancel out leadership if a superior leader has appeared
            self.node.state = State.Follower

        if request.prev_log_index > len(self.node.log):
            #mistake, no such prev_log_index
            return response

        if request.prev_log_index > 0 and self.node.log[request.prev_log_index - 1].term != request.prev_log_term:
            #mistake, logs from older terms
            return response

        for i, entry in enumerate(request.entries):
            cur = request.prev_log_index + i

            if cur < len(self.node.log):
                if self.node.log[cur].term != entry.term:
                    print(f"CUTTING LOGS TO {cur}")
                    self.node.log = self.node.log[:cur]
                else:
                    continue

            print(f"APPENDING at {cur}")
            self.node.log.append(entry)

        if request.leader_commit > self.node.commit_index:
            self.node.commit_index = min(request.leader_commit, len(self.node.log))
            print(f"Propagating commit to {self.node.commit_index}")

        response.success = True
        self.node.last_heartbeat = time.time()
        return response 
