#!/bin/bash
# Script para ejecutar validación completa con entorno virtual

echo "🚀 INICIANDO VALIDACIÓN COMPLETA DEL PIPELINE ETL"
echo "=================================================="

# Activar entorno virtual
echo "📦 Activando entorno virtual Python 3.11..."
source .venv/bin/activate

# Verificar Python
echo "🐍 Versión de Python: $(python --version)"
echo "📍 Ubicación de Python: $(which python)"

# Verificar dependencias instaladas
echo ""
echo "📋 Verificando dependencias críticas..."
python -c "
try:
    import pandas as pd
    print('   ✅ pandas:', pd.__version__)
except ImportError:
    print('   ❌ pandas: No instalado')

try:
    import psycopg2
    print('   ✅ psycopg2: Instalado')
except ImportError:
    print('   ❌ psycopg2: No instalado')
    
try:
    import pyspark
    print('   ✅ pyspark:', pyspark.__version__)
except ImportError:
    print('   ❌ pyspark: No instalado')

try:
    import redis
    print('   ✅ redis: Instalado')
except ImportError:
    print('   ❌ redis: No instalado')
"

echo ""
echo "⚡ Ejecutando validación completa..."
python validate_complete.py
