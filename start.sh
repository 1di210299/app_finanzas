#!/bin/bash

# Colores
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${BLUE}Iniciando Data Pipeline...${NC}"

# Verificar Docker
if ! docker info &> /dev/null; then
    echo -e "${YELLOW}⚠ Docker no está ejecutándose. Iniciando Docker Desktop...${NC}"
    open -a Docker
    echo "⏳ Esperando a que Docker se inicie..."
    while ! docker info &> /dev/null; do
        sleep 2
    done
fi

# Generar Fernet Key si es necesario
if grep -q "PLACEHOLDER_FERNET_KEY" docker-compose.yml; then
    echo -e "${BLUE}🔑 Generando Fernet Key para Airflow...${NC}"
    FERNET_KEY=$(python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())")
    sed -i '' "s/PLACEHOLDER_FERNET_KEY/$FERNET_KEY/g" docker-compose.yml
    echo -e "${GREEN}Fernet Key configurada${NC}"
fi

echo -e "${BLUE}🐳 Iniciando contenedores...${NC}"
docker-compose up -d

echo -e "${BLUE}⏳ Esperando que los servicios estén listos...${NC}"
echo "   ⏳ PostgreSQL iniciando..."
docker-compose exec -T postgres pg_isready -U airflow > /dev/null 2>&1
while [ $? -ne 0 ]; do
    sleep 2
    docker-compose exec -T postgres pg_isready -U airflow > /dev/null 2>&1
done
echo -e "${GREEN}   PostgreSQL listo${NC}"

echo "   ⏳ Redis iniciando..."
docker-compose exec -T redis redis-cli ping > /dev/null 2>&1
while [ $? -ne 0 ]; do
    sleep 2
    docker-compose exec -T redis redis-cli ping > /dev/null 2>&1
done
echo -e "${GREEN}   Redis listo${NC}"

echo "   ⏳ Airflow iniciando..."
sleep 20

echo -e "${BLUE}👤 Creando usuario admin en Airflow...${NC}"
docker-compose exec -T airflow-webserver airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin123 2>/dev/null || true

echo ""
echo -e "${GREEN}🎉 ¡Data Pipeline iniciado correctamente!${NC}"
echo ""
echo -e "${BLUE}📍 URLs de acceso:${NC}"
echo -e "   Airflow UI: ${YELLOW}http://localhost:8080${NC} (admin/admin123)"
echo -e "   Spark UI:   ${YELLOW}http://localhost:8081${NC}"
echo -e "   🗄 PostgreSQL: ${YELLOW}localhost:5432${NC} (airflow/airflow123)"
echo ""
echo -e "${BLUE}Para activar el DAG:${NC}"
echo "   1. Ir a http://localhost:8080"
echo "   2. Login con admin/admin123"
echo "   3. Buscar 'data_pipeline_etl_demo'"
echo "   4. Activar el toggle del DAG"
echo "   5. Hacer clic en 'Trigger DAG' para ejecutar"
echo ""
echo -e "${GREEN}¡Listo para procesar datos a escala!${NC}"
