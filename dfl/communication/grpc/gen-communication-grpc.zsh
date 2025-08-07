#!/usr/bin/env sh

set -e

cd "$(dirname "$0")/../../../"

python -m grpc_tools.protoc -I . --python_out . --pyi_out . --grpc_python_out . ./dfl/communication/grpc/communication.proto