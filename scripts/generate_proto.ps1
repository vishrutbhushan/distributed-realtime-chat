# Generate Python gRPC stubs from .proto files
# Run from the repository root: .\scripts\generate_proto.ps1

$ProtoDir = "proto"
$OutDir   = "generated"

New-Item -ItemType Directory -Force -Path $OutDir | Out-Null

python -m grpc_tools.protoc `
    -I $ProtoDir `
    --python_out=$OutDir `
    --grpc_python_out=$OutDir `
    $ProtoDir\chat.proto `
    $ProtoDir\raft.proto `
    $ProtoDir\llm.proto

Write-Host "Proto stubs generated in $OutDir/"
Get-ChildItem $OutDir

