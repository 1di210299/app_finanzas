# Pipeline de Datos Escalable y Confiable Pipeline de Datos Escalable y Confiable

## Sobre Este Proyecto

Este proyecto implementa una solución completa de pipeline de datos empresarial que desarrollé como parte de una evaluación técnica. El objetivo era crear un sistema escalable y confiable que pudiera manejar grandes volúmenes de datos utilizando tecnologías modernas de la industria.

### ¿Qué problema resuelve?

En mi experiencia trabajando con datos, he visto cómo las empresas luchan con:
- **Volúmenes crecientes de datos** que saturan sistemas legacy
- **Procesos manuales** propensos a errores humanos
- **Falta de visibilidad** sobre el estado de los pipelines
- **Escalabilidad limitada** cuando el negocio crece

Esta solución aborda estos desafíos implementando un pipeline robusto que puede crecer con las necesidades del negocio.

## Arquitectura del Sistema

He diseñado una arquitectura de microservicios que separa claramente las responsabilidades:

```
Data Sources → Airflow (Orchestration) → Spark (Processing) → PostgreSQL (Storage)
     ↓              ↓                       ↓                    ↓
  CSV/JSON      Scheduling            Distributed           Data Warehouse
   Files        Monitoring            Computing              Analytics
```

### Tecnologías Principales

- **Apache Airflow 2.7.2**: Para orquestación y monitoreo de workflows
- **Apache Spark 3.4**: Procesamiento distribuido de grandes volúmenes
- **PostgreSQL 14**: Base de datos confiable para almacenamiento final
- **Docker Compose**: Orquestación de servicios y deployment
- **Redis**: Message broker para comunicación entre servicios

## Inicio Rápido

### Requisitos Previos
- Docker Desktop instalado y ejecutándose
- Al menos 8GB de RAM disponible
- Puertos 5432, 6379, 7077, 8080, 8081 libres

### Instalación en 3 Pasos

```bash
# 1. Iniciar todos los servicios
./start.sh

# 2. Verificar que todo funcione
python validate_simple.py

# 3. Acceder a la interfaz web
open http://localhost:8080
```

### Scripts de Utilidad

```bash
# Ver logs en tiempo real
./logs.sh

# Ejecutar validación completa
./run_validation.sh

# Detener servicios
./stop.sh

# Limpieza completa (cuidado: borra datos)
./clean.sh
```

## Interfaces Web

Una vez que el sistema esté ejecutándose:

| Servicio | URL | Credenciales | Propósito |
|----------|-----|--------------|-----------|
| **Airflow Web UI** | http://localhost:8080 | admin/admin123 | Monitoreo y control del pipeline |
| **Spark Master UI** | http://localhost:8081 | - | Estado del cluster de procesamiento |
| **PostgreSQL** | localhost:5432 | airflow/airflow123 | Base de datos (usar cliente SQL) |

## Pipeline de Datos Implementado

### Flujo de Procesamiento

El DAG `data_pipeline_etl` ejecuta estas etapas secuencialmente:

1. **Data Ingestion**: Lee archivos CSV y JSON desde el directorio `/data/raw/`
2. **Data Cleaning**: Valida formatos, elimina duplicados, maneja valores nulos
3. **Data Transformation**: Calcula métricas agregadas y KPIs de negocio
4. **Data Loading**: Inserta datos procesados en PostgreSQL
5. **Quality Checks**: Valida integridad y consistencia de datos
6. **Daily Summary**: Genera reportes con métricas del día
7. **Notifications**: Envía alertas sobre el estado del procesamiento

### Cómo Ejecutar el Pipeline

1. **Accede a Airflow**: http://localhost:8080 (admin/admin123)
2. **Encuentra el DAG**: Busca `data_pipeline_etl` en la lista
3. **Actívalo**: Cambia el toggle de OFF a ON
4. **Ejecuta**: Haz clic en "Trigger DAG" para iniciar manualmente
5. **Monitorea**: Utiliza la vista Graph para ver el progreso en tiempo real

## Estructura del Proyecto

```
├── README.md                      # Este archivo
├── docker-compose.yml            # Configuración de servicios
├── Dockerfile.airflow            # Imagen customizada de Airflow
├── requirements.txt              # Dependencias Python
├── start.sh / stop.sh / clean.sh # Scripts de manejo del entorno
├── dags/
│   └── data_pipeline_dag.py     # Definición del pipeline principal
├── spark/
│   ├── data_ingestion.py        # Módulo de lectura de datos
│   ├── data_cleaning.py         # Módulo de limpieza y validación
│   ├── data_transformation.py   # Módulo de transformaciones
│   └── data_loader.py           # Módulo de carga a PostgreSQL
├── data/
│   ├── raw/                     # Datos fuente (CSV, JSON)
│   ├── processed/               # Resultados del procesamiento
│   └── logs/                    # Logs del pipeline
├── monitoring/
│   ├── data_quality_monitor.py # Monitor de calidad de datos
│   └── alert_system.py         # Sistema de alertas
└── sql/
    └── create_tables.sql        # Scripts de base de datos
```

## Datos de Prueba Incluidos

Para facilitar las pruebas, incluí datasets en tres tamaños:

### Datasets Small (~20 registros cada uno)
- `sales_data_small.csv`: Datos de ventas básicos
- `customer_data_small.json`: Información de clientes
- `products_data_small.csv`: Catálogo de productos

### Datasets Medium (~100 registros cada uno)
- `sales_data_medium.csv`: Volumen intermedio de ventas
- `customer_data_medium.json`: Base ampliada de clientes
- `products_data_medium.csv`: Catálogo extendido

### Datasets Large (~5000 registros cada uno)
- `sales_data_large.csv`: Simulación de alto volumen
- `customer_data_large.json`: Base de datos empresarial
- `products_data_large.csv`: Catálogo completo

Esto permite probar el comportamiento del pipeline con diferentes cargas de trabajo.

## Validación y Troubleshooting

### Scripts de Validación

He incluido varios scripts para verificar que todo funcione correctamente:

```bash
# Validación básica - verifica que servicios estén up
python validate_simple.py

# Validación completa - ejecuta el pipeline end-to-end
python validate_complete.py

# Validación de producción - prueba con datasets grandes
python validate_production.py

# Script automatizado que ejecuta todas las validaciones
./run_validation.sh
```

### Problemas Comunes y Soluciones

**"Port already in use"**
```bash
# Verificar qué está usando el puerto
lsof -i :8080

# Si es necesario, cambiar puertos en docker-compose.yml
```

**"Container health check failed"**
```bash
# Ver logs específicos del servicio que falla
docker-compose logs airflow-webserver

# Reiniciar servicios problemáticos
docker-compose restart airflow-webserver
```

**"Out of memory errors"**
```bash
# Verificar uso de recursos
docker stats

# Ajustar memoria en docker-compose.yml si es necesario
# O cerrar otras aplicaciones para liberar RAM
```

**"DAG not appearing in Airflow"**
```bash
# Verificar que el archivo DAG esté correcto
docker-compose logs airflow-scheduler

# Refrescar la página de Airflow o esperar 30 segundos
# Los DAGs se escanean automáticamente cada 30 segundos
```

### Logs Útiles

```bash
# Ver todos los logs en tiempo real
./logs.sh

# Logs específicos de un servicio
docker-compose logs -f spark-master
docker-compose logs -f airflow-scheduler

# Logs del pipeline de validación
cat data/logs/pipeline_validation.log
```

## Configuración Avanzada

### Personalizar Recursos

Puedes ajustar la configuración de recursos editando `docker-compose.yml`:

```yaml
# Ejemplo: Aumentar memoria para Spark
spark-master:
  deploy:
    resources:
      limits:
        memory: 2G
      reservations:
        memory: 1G
```

### Agregar Nuevos Datasets

1. **Coloca tus archivos** en `/data/raw/`
2. **Actualiza el DAG** en `dags/data_pipeline_dag.py` si es necesario
3. **Modifica los scripts Spark** en `/spark/` para manejar tus formatos específicos

### Configurar Alertas

El sistema incluye un framework básico de alertas en `monitoring/alert_system.py`. Puedes:

- Configurar notificaciones por email
- Integrar con Slack/Teams
- Conectar con sistemas de monitoreo externos

## Características Técnicas Destacadas

### Escalabilidad
- **Spark Cluster Distribuido**: Fácil agregado de workers adicionales
- **Procesamiento Paralelo**: Datos particionados automáticamente
- **Resource Management**: Control granular de CPU y memoria

### Confiabilidad
- **Health Checks**: Monitoreo automático de todos los servicios
- **Retry Logic**: Reintentos automáticos en tareas que fallan
- **Data Validation**: Verificación de calidad en cada etapa
- **Atomic Operations**: Rollback automático si algo falla

### Observabilidad
- **Logging Estructurado**: Logs en formato JSON para análisis
- **Métricas de Performance**: Tiempo de ejecución y throughput
- **Monitoring Dashboard**: Interfaces web para monitoreo visual
- **Audit Trail**: Registro completo de todas las operaciones

### Mantenibilidad
- **Código Modular**: Cada componente tiene responsabilidades claras
- **Documentación Exhaustiva**: Comentarios y READMEs detallados
- **Testing Automatizado**: Scripts de validación incluidos
- **Configuration Management**: Separación clara entre config y código

## Próximas Mejoras

### Roadmap Técnico

**Fase 1: Cloud Native**
- [ ] Migración a Kubernetes con Helm charts
- [ ] Integración con proveedores cloud (AWS/GCP/Azure)
- [ ] Secretos management con Vault/K8s secrets
- [ ] Auto-scaling basado en métricas

**Fase 2: Advanced Analytics**
- [ ] Integración con Apache Superset para dashboards
- [ ] Machine Learning pipeline con MLflow
- [ ] Real-time streaming con Kafka/Kinesis
- [ ] Data lineage tracking automático

**Fase 3: Enterprise Features**
- [ ] RBAC granular y SSO integration
- [ ] Compliance logging (GDPR, SOX)
- [ ] Multi-tenant architecture
- [ ] Disaster recovery automático

### Optimizaciones Identificadas

**Performance**
- Implementar columnar storage (Parquet) para datasets grandes
- Cache inteligente para consultas repetitivas
- Compresión de datos históricos
- Query optimization automática

**Operational Excellence**
- CI/CD pipeline para deployment automático
- Infrastructure as Code con Terraform
- Automated testing en pipeline de desarrollo
- Blue/green deployments

## Reflexiones del Desarrollador

### Por Qué Esta Arquitectura

Cuando diseñé este sistema, pensé en los desafíos reales que he visto en empresas:

**Problema**: Los pipelines monolíticos se vuelven unmaintainable
**Solución**: Microservicios con responsabilidades claras

**Problema**: Scaling vertical tiene límites y es costoso
**Solución**: Arquitectura distribuida con scaling horizontal

**Problema**: Debugging en producción es complejo sin visibilidad
**Solución**: Logging exhaustivo y interfaces de monitoreo

**Problema**: Onboarding de nuevos developers es lento
**Solución**: Documentación comprehensive y setup automatizado

### Decisiones de Diseño

**Docker Compose vs Kubernetes**: Para desarrollo y demos, Docker Compose ofrece simplicidad sin sacrificar funcionalidad. Para producción, la migración a K8s sería straightforward.

**PostgreSQL vs Data Lakes**: Para datos estructurados con queries complejas, PostgreSQL ofrece ACID compliance y performance excelente. Para volúmenes masivos no estructurados, se integraría S3/HDFS.

**Airflow vs Alternativas**: Airflow tiene el ecosistema más maduro, community support incomparable, y es prácticamente el estándar de facto en la industria.

### Lecciones Aprendidas

**Service Orchestration**: La coordinación entre servicios distribuidos requiere healthchecks robustos y manejo cuidadoso de dependencies.

**Resource Balancing**: Ejecutar Spark y Airflow en el mismo host requiere tuning cuidadoso para evitar resource contention.

**Data Quality**: Validar datos en cada etapa del pipeline es crucial - garbage in, garbage out sigue siendo verdad.

**Documentation First**: Escribir documentación mientras develops, no después, resulta en mejor código y menos bugs.

## Contribuciones y Mejoras

Este proyecto refleja mi approach hacia el desarrollo de sistemas de datos enterprise-grade. Está diseñado siguiendo principios de:

- **Clean Code**: Naming consistente y funciones con responsabilidades únicas
- **Separation of Concerns**: Cada módulo hace una cosa y la hace bien
- **DRY Principle**: Reutilización de código donde tiene sentido
- **SOLID Principles**: Especialmente Single Responsibility y Dependency Inversion

Si encuentras bugs, tienes sugerencias de mejora, o quieres contribuir con nuevas features, toda contribución es bienvenida. 

### Cómo Contribuir

1. **Reporta Issues**: Usa los templates en GitHub Issues
2. **Sugiere Features**: Describe el use case y business value
3. **Contribuye Código**: Fork, develop, test, submit PR
4. **Mejora Documentación**: Siempre hay espacio para clarificaciones

## Contacto

Este proyecto fue desarrollado como parte de una evaluación técnica, pero refleja mi filosofía de desarrollo: build systems que sean production-ready desde el primer día, document everything, y siempre piensa en el siguiente developer que tendrá que mantener tu código.

Si tienes preguntas sobre implementación, decisiones de arquitectura, o quieres discutir mejores prácticas para pipelines de datos, estaré encantado de conversar.

---

*"La mejor arquitectura es la que resuelve problemas reales y puede evolucionar con el negocio."*

**Desarrollado con y muchas tazas de café**
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                │
                                ▼
                       ┌─────────────────┐
                       │  Spark Engine   │
                       │  Data Processor │
                       └─────────────────┘
```

## Estructura del Proyecto

```
data-pipeline/
├── 🐳 docker-compose.yml     # Orquestación de servicios
├── Dockerfile.airflow     # Container Airflow personalizado
├── requirements.txt       # Dependencias Python
├── start.sh              # Script de inicio
├── 🛑 stop.sh               # Script de parada
├── 🧹 clean.sh              # Script de limpieza
├── logs.sh               # Visor de logs
├── data/                    # 📂 Datos
│   ├── raw/                # Datos fuente
│   ├── processed/          # Datos procesados
│   └── logs/               # Logs del sistema
├── dags/                    # DAGs de Airflow
├── spark/                   # Jobs de Spark
├── sql/                     # 🗄Scripts SQL
├── config/                  # Configuraciones
└── monitoring/              # Monitoreo
```

## Comandos Útiles

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

## Próximos Pasos

1. **Personalizar datos**: Agregar tus CSV/JSON en `data/raw/`
2. **Crear DAGs**: Desarrollar pipelines en `dags/`
3. **Agregar transformaciones**: Usar Spark en `spark/`
4. **Configurar alertas**: Setup en `monitoring/`
5. **Escalar**: Agregar más workers y particionamiento

## Troubleshooting

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
*Hecho con ❤para macOS - Optimizado para Apple Silicon y Intel*
