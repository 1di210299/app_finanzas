#!/bin/bash

GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${BLUE}🛑 Deteniendo Data Pipeline...${NC}"

docker-compose down

echo -e "${GREEN}Data Pipeline detenido correctamente${NC}"
