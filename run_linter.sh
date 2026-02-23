#!/bin/bash
# Script para ejecutar flake8 linter en un contenedor Docker para Dialer

set -euo pipefail

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

IMAGE_NAME="${IMAGE_NAME:-python:3.9-alpine}"
CONTAINER_NAME="dialer-linter-$(date +%s)"

echo -e "${GREEN}** [OMniLeads Dialer] Ejecutando Flake8 Linter en Contenedor **${NC}"

# Verificar si Docker está disponible
if ! command -v docker &> /dev/null; then
    echo -e "${RED}Error: Docker no está instalado o no está en el PATH${NC}"
    exit 1
fi

# Verificar que estamos en el directorio correcto
if [ ! -f ".flake8" ]; then
    echo -e "${RED}Error: No se encontró el archivo .flake8 en el directorio actual${NC}"
    echo -e "${YELLOW}Por favor, ejecuta este script desde el directorio raíz de dialer${NC}"
    exit 1
fi

# Verificar si la imagen existe
if ! docker image inspect "$IMAGE_NAME" &> /dev/null; then
    echo -e "${YELLOW}La imagen $IMAGE_NAME no existe localmente. Se descargará automáticamente.${NC}"
fi

echo -e "${BLUE}Ejecutando flake8 en contenedor...${NC}"

# Opciones adicionales de flake8
FLAKE8_OPTS="${FLAKE8_OPTS:---statistics --count}"
PIP_INSTALL_OPTS="${PIP_INSTALL_OPTS:---disable-pip-version-check --no-cache-dir --retries 10 --timeout 60}"

docker run --rm \
    --name "$CONTAINER_NAME" \
    -e HTTP_PROXY \
    -e HTTPS_PROXY \
    -e NO_PROXY \
    -e http_proxy \
    -e https_proxy \
    -e no_proxy \
    -e PIP_INDEX_URL \
    -e PIP_EXTRA_INDEX_URL \
    -e PIP_TRUSTED_HOST \
    -v "$(pwd):/opt/dialer:ro" \
    -w /opt/dialer \
    "$IMAGE_NAME" \
    sh -c "
        set -eu && \
        echo 'Instalando flake8 en el contenedor...' && \
        python -m pip install $PIP_INSTALL_OPTS flake8 && \
        echo 'Ejecutando flake8...' && \
        flake8 --config=.flake8 . $FLAKE8_OPTS
    "

EXIT_CODE=$?

if [ $EXIT_CODE -eq 0 ]; then
    echo -e "${GREEN}✅ Linter ejecutado exitosamente - No se encontraron errores${NC}"
else
    echo -e "${RED}❌ Linter encontró errores (código de salida: $EXIT_CODE)${NC}"
fi

exit $EXIT_CODE
