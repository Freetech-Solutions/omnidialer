#!/usr/bin/env sh
# Legacy entrypoint → ./run_tests.sh (estilo django/run_tests.sh).
# Docker 24.x (CI dind) soporta hasta API 1.43; 1.44 requiere daemon >= 25.
export DOCKER_API_VERSION=1.43
cd "$(dirname "$0")"
exec ./run_tests.sh "$@"
