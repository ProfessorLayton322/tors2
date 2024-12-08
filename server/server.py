from flask import Flask, request, jsonify
from raft_node import RaftNode
from raft_node import RaftService
from grpclib.server import Server

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

async def serve(node):
    server = Server([RaftService(node)])
    await server.start(node.id.split(':')[0], int(node.id.split(':')[1]))
    await server.wait_closed()

async def main():
    #from waitress import serve
    #serve(app, host='0.0.0.0', port=5000)

    nodes = ['localhost:50051', 'localhost:50052', 'localhost:50053']
    server_id = int(os.environ["SERV_ID"])
    server = nodes[server_id]

    global raft_node
    raft_node = RaftNode(server, nodes)

    _ = await asyncio.gather(
        serve(raft_node),
        raft_node.run()
    )

if __name__ == '__main__':
    asyncio.run(main())
