from flask import Flask, request, jsonify
from concurrent import futures

from raft_node import RaftNode
from raft_node import RaftService

from proto import raft_pb2_grpc

import grpc
import asyncio

import os

app = Flask(__name__)

@app.route('/kv', methods=['POST'])
def create_key():
    key = request.json['key']
    value = request.json['value']
    print(f"POST {key} {value}")
    #success = raft_node.put(key, value)
    return jsonify({'success': True})

@app.route('/kv/<key>', methods=['GET', 'PUT', 'DELETE'])
def handle_key(key):
    if request.method == 'GET':
        #value = raft_node.get(key)
        print(f"GET {key}")
        return jsonify({'key': key, 'value': "something"})
    elif request.method == 'PUT':
        value = request.json['value']
        #success = raft_node.put(key, value)
        print(f"PUT {key}")
        return jsonify({'success': success})
    elif request.method == 'DELETE':
        #success = raft_node.delete(key)
        print(f"DELETE {key}")
        return jsonify({'success': success})

async def serve(node, port):
    server = grpc.aio.server(futures.ThreadPoolExecutor(max_workers=2))
    raft_pb2_grpc.add_RaftConsensusServicer_to_server(RaftService(node), server)
    server.add_insecure_port("0.0.0.0:" + port)
    await server.start()
    await server.wait_for_termination()

async def main():
    #from waitress import serve
    #serve(app, host='0.0.0.0', port=5000)

    nodes = ['0.0.0.0:50051', '0.0.0.0:50052', '0.0.0.0:50053']
    server_id = int(os.environ["SERV_ID"])
    server = nodes[server_id]

    global raft_node
    raft_node = RaftNode(server_id, nodes)

    _ = await asyncio.gather(
        serve(raft_node, server.split(':')[1]),
        raft_node.run()
    )

if __name__ == '__main__':
    nodes = ['0.0.0.0:50051', '0.0.0.0:50052', '0.0.0.0:50053']
    server_id = int(os.environ["SERV_ID"])
    server = nodes[server_id]

    global raft_node
    raft_node = RaftNode(server_id, nodes)

    asyncio.run(main())
