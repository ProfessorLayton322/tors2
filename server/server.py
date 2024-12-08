from flask import Flask, request, jsonify

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

if __name__ == '__main__':
    from waitress import serve
    serve(app, host='0.0.0.0', port=5000)
