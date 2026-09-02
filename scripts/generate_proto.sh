#!/usr/bin/env bash
# Generate Python gRPC stubs from .proto files
# Run from the repository root: bash scripts/generate_proto.sh

set -e

PROTO_DIR="proto"
OUT_DIR="generated"

mkdir -p "$OUT_DIR"

python -m grpc_tools.protoc \
    -I "$PROTO_DIR" \
    --python_out="$OUT_DIR" \
    --grpc_python_out="$OUT_DIR" \
    "$PROTO_DIR"/chat.proto \
    "$PROTO_DIR"/raft.proto \
    "$PROTO_DIR"/llm.proto

echo "Proto stubs generated in $OUT_DIR/"
ls "$OUT_DIR"

