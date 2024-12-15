#!/bin/bash

if [ $# -ne 2 ]; then
    echo "Usage: $0 <server_id> <key>"
    exit 1
fi

server_id=$1
key=$2

if ! [[ "$server_id" =~ ^[0-9]+$ ]]; then
    echo "Error: server_id must be a non-negative integer"
    exit 1
fi

port=$((51 + server_id))

curl -X GET "http://localhost:$port/kv/$key"

echo
