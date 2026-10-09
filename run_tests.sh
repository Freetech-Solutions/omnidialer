#!/bin/bash
# Ejecutar tests del worker handle-campaign (Postgres + Redis + Gearman).
#
# Uso:
#   ./run_tests.sh --up                         # solo levanta dependencias
#   ./run_tests.sh                              # tests.py + handler.tests_event_stream
#   ./run_tests.sh tests.PredictiveConstantsTests
#   ./run_tests.sh handler.tests_event_stream
#   ./run_tests.sh --down                       # baja el stack de test
#
# El runner usa docker-compose-test.yml: el código de
# workers/handle-campaign/src se monta en /app, así los cambios se ven
# sin reconstruir la imagen (salvo deps nuevas → BUILD_IMAGE=true).

set -euo pipefail

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

COMPOSE_FILE="docker-compose-test.yml"
ENV_FILE=".env-tests"
IMAGE_NAME="${IMAGE_NAME:-omnidialer_worker_test}"
WORKER_SRC="./workers/handle-campaign/src"

if docker compose version &> /dev/null; then
    COMPOSE_CMD="docker compose"
elif command -v docker-compose &> /dev/null; then
    COMPOSE_CMD="docker-compose"
else
    echo -e "${RED}Error: docker compose no está instalado${NC}"
    exit 1
fi

compose() {
    $COMPOSE_CMD --env-file "$ENV_FILE" -f "$COMPOSE_FILE" "$@"
}

compose_up() {
    echo -e "${GREEN}Levantando Postgres, Redis y Gearman de test...${NC}"
    compose up -d postgresql redis job_server
    echo -e "${GREEN}Esperando Postgres...${NC}"
    # PGPORT=5434 dentro del contenedor (ver docker-compose-test.yml)
    for _ in $(seq 1 30); do
        if compose exec -T postgresql pg_isready -U omnidialer -p 5434 &>/dev/null; then
            echo -e "${GREEN}Dependencias listas.${NC}"
            return 0
        fi
        sleep 1
    done
    echo -e "${RED}Timeout esperando Postgres${NC}"
    return 1
}

compose_down() {
    echo -e "${YELLOW}Deteniendo stack de test dialer...${NC}"
    compose down
}

ensure_image() {
    if [ "${BUILD_IMAGE:-false}" = "true" ] || ! docker image inspect "$IMAGE_NAME" &> /dev/null; then
        echo -e "${YELLOW}Construyendo imagen Docker (primera vez o BUILD_IMAGE=true)...${NC}"
        compose build worker
    fi
}

run_tests_in_container() {
    ensure_image
    compose_up
    echo -e "${GREEN}Ejecutando tests en contenedor (cwd: /app, src montado)...${NC}"
    # Default: suite principal + event_stream (evita test_call_generate.py, que no es unittest).
    if [ $# -gt 0 ]; then
        compose run --rm --no-deps worker \
            python -m unittest "$@"
    else
        compose run --rm --no-deps worker \
            python -m unittest tests handler.tests_event_stream -v
    fi
}

case "${1:-}" in
    --up)
        compose_up
        ;;
    --down)
        compose_down
        ;;
    --help|-h)
        sed -n '2,14p' "$0"
        ;;
    *)
        if [ ! -f "$ENV_FILE" ]; then
            echo -e "${RED}Falta ${ENV_FILE} en $(pwd)${NC}"
            exit 1
        fi
        if [ ! -d "$WORKER_SRC" ]; then
            echo -e "${RED}No se encuentra ${WORKER_SRC}. Ejecutá desde components-git-repo/dialer${NC}"
            exit 1
        fi
        echo -e "${GREEN}** [OMniDialer handle-campaign] Tests en local **${NC}"
        set +e
        run_tests_in_container "$@"
        EXIT_CODE=$?
        set -e
        if [ $EXIT_CODE -eq 0 ]; then
            echo -e "${GREEN}Tests OK${NC}"
        else
            echo -e "${RED}Tests fallaron (exit ${EXIT_CODE})${NC}"
        fi
        exit $EXIT_CODE
        ;;
esac
