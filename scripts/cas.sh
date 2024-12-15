#!/bin/bash

if [ $# -ne 4 ]; then
    echo "Usage: $0 <server_id> <key> <value> <old_value>"
    exit 1
fi

server_id=$1
key=$2
value=$3
old_value=$4

if ! [[ "$server_id" =~ ^[0-9]+$ ]]; then
    echo "Error: server_id must be a non-negative integer"
    exit 1
fi

port=$((51 + server_id))

curl -X PATCH -H "Content-Type: application/json" -d "{\"value\":\"$value\", \"old_value\":\"$old_value\"}" "http://localhost:$port/kv/$key"

echo
