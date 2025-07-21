# 🚀 Data Pipeline Escalable - macOS Edition

Pipeline de datos empresarial completo con Apache Airflow, Spark y PostgreSQL.

## ⚡ Inicio Rápido

```bash
# Iniciar el pipeline completo
./start.sh

# Ver logs en tiempo real
./logs.sh

# Detener el pipeline
./stop.sh

# Limpiar todo (incluye datos)
./clean.sh
```

## 🌐 URLs de Acceso

| Servicio | URL | Credenciales |
|----------|-----|--------------|
| 🎛️ **Airflow UI** | http://localhost:8080 | admin/admin123 |
| ⚡ **Spark UI** | http://localhost:8081 | - |
| 🗄️ **PostgreSQL** | localhost:5432 | airflow/airflow123 |

## 📊 DAG Demo Incluido

El pipeline incluye un DAG de demostración que:

✅ **Valida datos** de múltiples fuentes (CSV, JSON)  
✅ **Verifica conexiones** a base de datos  
✅ **Ejecuta checks** de calidad  
✅ **Genera reportes** automáticos  

### Activar el DAG:
1. Ir a http://localhost:8080
2. Login con `admin`/`admin123`
3. Buscar `data_pipeline_etl_demo`
4. Activar el toggle del DAG
5. Hacer clic en "Trigger DAG"

## 🏗️ Arquitectura

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Data Sources  │───▶│   Airflow DAG   │───▶│ Data Warehouse  │
│   (CSV/JSON)    │    │   Orchestrator  │    │   PostgreSQL    │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                │
                                ▼
                       ┌─────────────────┐
                       │  Spark Engine   │
                       │  Data Processor │
                       └─────────────────┘
```

## 📁 Estructura del Proyecto

```
data-pipeline/
├── 🐳 docker-compose.yml     # Orquestación de servicios
├── 📦 Dockerfile.airflow     # Container Airflow personalizado
├── 📋 requirements.txt       # Dependencias Python
├── 🚀 start.sh              # Script de inicio
├── 🛑 stop.sh               # Script de parada
├── 🧹 clean.sh              # Script de limpieza
├── 📊 logs.sh               # Visor de logs
├── data/                    # 📂 Datos
│   ├── raw/                # Datos fuente
│   ├── processed/          # Datos procesados
│   └── logs/               # Logs del sistema
├── dags/                    # 🎯 DAGs de Airflow
├── spark/                   # ⚡ Jobs de Spark
├── sql/                     # 🗄️ Scripts SQL
├── config/                  # ⚙️ Configuraciones
└── monitoring/              # 📈 Monitoreo
```

## 🔧 Comandos Útiles

### Gestión de Servicios
```bash
# Ver estado de contenedores
docker-compose ps

# Reiniciar un servicio específico
docker-compose restart airflow-webserver

# Ejecutar comandos en contenedores
docker-compose exec postgres psql -U airflow -d data_warehouse
docker-compose exec airflow-webserver airflow dags list
```

### Desarrollo
```bash
# Reconstruir imágenes
docker-compose build --no-cache

# Ver logs de un servicio específico
docker-compose logs -f airflow-scheduler

# Acceso directo a PostgreSQL
docker-compose exec postgres psql -U airflow -d data_warehouse
```

## 🎯 Próximos Pasos

1. **Personalizar datos**: Agregar tus CSV/JSON en `data/raw/`
2. **Crear DAGs**: Desarrollar pipelines en `dags/`
3. **Agregar transformaciones**: Usar Spark en `spark/`
4. **Configurar alertas**: Setup en `monitoring/`
5. **Escalar**: Agregar más workers y particionamiento

## 🚨 Troubleshooting

### Docker no responde
```bash
# Reiniciar Docker Desktop
killall Docker && open -a Docker
```

### Puerto en uso
```bash
# Ver qué está usando el puerto 8080
lsof -i :8080

# Cambiar puerto en docker-compose.yml
# "8080:8080" → "8090:8080"
```

### Fernet Key error
```bash
# Regenerar Fernet Key
python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

## 🎉 ¡Listo para Big Data!

Este pipeline está diseñado para escalar desde datasets pequeños hasta terabytes de información. 

**¿Questions?** Revisa los logs con `./logs.sh` o abre un issue.

---
*Hecho con ❤️ para macOS - Optimizado para Apple Silicon y Intel*
