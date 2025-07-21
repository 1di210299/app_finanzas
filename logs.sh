#!/bin/bash

GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${BLUE}Logs del Data Pipeline${NC}"
echo ""
echo "Selecciona el servicio:"
echo "1) Airflow Webserver"
echo "2) Airflow Scheduler"
echo "3) Spark Master"
echo "4) PostgreSQL"
echo "5) Todos los servicios"

read -p "Opción (1-5): " choice

case $choice in
    1)
        docker-compose logs -f airflow-webserver
        ;;
    2)
        docker-compose logs -f airflow-scheduler
        ;;
    3)
        docker-compose logs -f spark-master
        ;;
    4)
        docker-compose logs -f postgres
        ;;
    5)
        docker-compose logs -f
        ;;
    *)
        echo "Opción inválida"
        ;;
esac
