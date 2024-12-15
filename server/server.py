from quart import Quart, request, jsonify
from concurrent import futures

from raft_node import RaftNode
from raft_node import RaftService
from raft_node import State

from proto import raft_pb2_grpc

import grpc
import asyncio

import os

app = Quart(__name__)
global raft_node
global nodes
global server_id

nodes = ['0.0.0.0:50051', '0.0.0.0:50052', '0.0.0.0:50053']
server_id = int(os.environ["SERV_ID"])
raft_node = RaftNode(server_id, nodes)

@app.route('/kv', methods=['POST'])
async def create_key():
    global raft_node

    data = await request.get_json()
    key = data['key']
    value = data['value']

    print(f"METHOD {request.method} key {key} {value}")
    if raft_node.state != State.Leader:
        return jsonify({'success': False})

    result = await raft_node.create(key, value)

    return jsonify({'success': result})

@app.route('/kv/<key>', methods=['GET', 'PUT', 'DELETE', 'PATCH'])
async def handle_key(key):
    print(f"METHOD {request.method} key {key}")

    global raft_node

    if request.method == 'GET':
        value = raft_node.get(key)
        if value is None:
            return jsonify({'success' : False})
        return jsonify({'success': True, 'value' : value})

    if raft_node.state != State.Leader:
        return jsonify({'success': False})

    if request.method == 'PUT':
        data = await request.get_json()
        value = data['value']

        result = await raft_node.put(key, value)
        return jsonify({'success': result})

    if request.method == 'DELETE':
        result = await raft_node.delete(key)
        return jsonify({'success': result}) 

    if request.method == 'PATCH':
        data = await request.get_json()
        value = data['value']
        old_value = data['old_value']

        result = await raft_node.compare_swap(key, old_value, value)
        return jsonify({'success': result}) 

async def serve():
    global nodes
    global server_id
    global raft_node

    grpc_address = nodes[server_id]

    server = grpc.aio.server(futures.ThreadPoolExecutor(max_workers=1))
    raft_pb2_grpc.add_RaftConsensusServicer_to_server(RaftService(raft_node), server)
    server.add_insecure_port(grpc_address)

    await server.start()
    await server.wait_for_termination()

@app.before_serving
async def startup():
    app.grpc_task = asyncio.create_task(serve())
    global raft_node
    app.node_task = asyncio.create_task(raft_node.run())

if __name__ == '__main__':
    import uvicorn 

    port = int(nodes[server_id].split(':')[1]) - 50000
    print(port)
    uvicorn.run(app, host="0.0.0.0", port=port)
