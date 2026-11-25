#!/usr/bin/env bash
# A live 5-node cluster you can break from the browser: http://localhost:8080
#   scripts/playground.sh [--nodes 5] [--port 8080] [--first-client-port 6380]
set -euo pipefail
cd "$(dirname "$0")/.."
./gradlew installDist -q
exec java -cp "build/install/raftis/lib/*" com.raftis.playground.Playground "$@"
