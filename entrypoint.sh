#!/bin/bash

# Script de inicialización de Airflow
set -e

# Esperar a que PostgreSQL esté disponible
echo "Esperando a PostgreSQL..."
while ! pg_isready -h postgres -p 5432 -U airflow; do
    echo "PostgreSQL no está listo - esperando..."
    sleep 1
done

echo "PostgreSQL está listo!"

# Intentar conectar y verificar si la base de datos está inicializada
if ! airflow db check-migrations; then
    echo "Inicializando base de datos de Airflow..."
    airflow db init
    
    echo "Creando usuario admin de Airflow..."
    airflow users create \
        --username admin \
        --firstname Admin \
        --lastname User \
        --role Admin \
        --email admin@example.com \
        --password admin123 || true  # No fallar si ya existe
else
    echo "Base de datos de Airflow ya está inicializada."
fi

# Ejecutar el comando original
exec "$@"
