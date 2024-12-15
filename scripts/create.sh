#!/bin/bash

if [ $# -ne 3 ]; then
    echo "Usage: $0 <server_id> <key> <value>"
    exit 1
fi

server_id=$1
key=$2
value=$3

if ! [[ "$server_id" =~ ^[0-9]+$ ]]; then
    echo "Error: server_id must be a non-negative integer"
    exit 1
fi

port=$((51 + server_id))

curl -X POST -H "Content-Type: application/json" -d "{\"value\":\"$value\", \"key\":\"$key\"}" "http://localhost:$port/kv"

echo
