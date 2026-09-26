FROM python:3.11-slim

WORKDIR /app

# Install dependencies (includes grpcio-tools for proto compilation)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy all source code
COPY proto/     ./proto/
COPY app/       ./app/
COPY storage/   ./storage/
COPY raft/      ./raft/
COPY client/    ./client/
COPY llm/       ./llm/
COPY scripts/   ./scripts/
COPY tests/     ./tests/

# Compile protobuf definitions → generated/
RUN mkdir -p generated && \
    python -m grpc_tools.protoc \
        -I ./proto \
        --python_out=./generated \
        --grpc_python_out=./generated \
        ./proto/chat.proto \
        ./proto/raft.proto \
        ./proto/llm.proto

# Persistent data directory (mounted via Docker volume)
RUN mkdir -p /data/files

ENV PYTHONPATH=/app:/app/generated

# Default: run the app server
CMD ["python", "app/server.py"]

