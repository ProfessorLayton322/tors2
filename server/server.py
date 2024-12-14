from quart import Quart, request, jsonify
from concurrent import futures

from raft_node import RaftNode
from raft_node import RaftService

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
    data = await request.get_json()
    key = data['key']
    value = data['value']
    print(f"POST {key} {value}")
    #success = await raft_node.put(key, value)
    return jsonify({'success': True})

@app.route('/kv/<key>', methods=['GET', 'PUT', 'DELETE'])
async def handle_key(key):
    if request.method == 'GET':
        #value = await raft_node.get(key)
        print(f"GET {key}")
        return jsonify({'key': key, 'value': "something"})
    elif request.method == 'PUT':
        data = await request.get_json()
        value = data['value']
        #success = await raft_node.put(key, value)
        print(f"PUT {key}")
        return jsonify({'success': True})  # Changed from 'success' to True for this example
    elif request.method == 'DELETE':
        #success = await raft_node.delete(key)
        print(f"DELETE {key}")
        return jsonify({'success': True})  # Changed from 'success' to True for this example


async def serve():
    global nodes
    global server_id
    global raft_node

    grpc_address = nodes[server_id]

    server = grpc.aio.server(futures.ThreadPoolExecutor(max_workers=2))
    raft_pb2_grpc.add_RaftConsensusServicer_to_server(RaftService(raft_node), server)
    server.add_insecure_port(grpc_address)

    await server.start()
    await server.wait_for_termination()

@app.before_serving
async def startup():
    app.grpc_task = asyncio.create_task(serve())
    global raft_node
    app.node_task = asyncio.create_task(raft_node.run())

'''
async def main():

    _ = await asyncio.gather(
        serve(raft_node, server.split(':')[1]),
        raft_node.run()
    )
'''

if __name__ == '__main__':
    import uvicorn 

    port = int(nodes[server_id].split(':')[1]) - 50000
    print(port)
    uvicorn.run(app, host="0.0.0.0", port=port)
