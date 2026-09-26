FROM python:3.11-slim

WORKDIR /app

# Install dependencies (includes grpcio-tools for proto compilation)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy all source code & assets
COPY proto/     ./proto/
COPY app/       ./app/
COPY storage/   ./storage/
COPY raft/      ./raft/
COPY client/    ./client/
COPY web/       ./web/
COPY llm/       ./llm/
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

ENV PYTHONPATH=/app:/app/generated

# Run static unit tests at build time to verify integrity
RUN python -m unittest discover -v tests

# Persistent data directory (mounted via Docker volume)
RUN mkdir -p /data/files

EXPOSE 50051 8000

# Default: run the app server
CMD ["python", "app/server.py"]
