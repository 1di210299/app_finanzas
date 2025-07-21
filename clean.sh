#!/bin/bash

GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${BLUE}🧹 Limpiando Data Pipeline...${NC}"

echo -e "${YELLOW}⚠ Esto eliminará todos los contenedores, volúmenes y datos${NC}"
read -p "¿Estás seguro? (y/N): " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    docker-compose down -v --remove-orphans
    docker system prune -f
    echo -e "${GREEN}Limpieza completada${NC}"
else
    echo "Operación cancelada"
fi
