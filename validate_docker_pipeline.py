#!/usr/bin/env python3
"""
Validador del Pipeline ETL en Docker
=====================================
Verifica que todos los componentes del pipeline estén funcionando correctamente.
"""

import subprocess
import json
import time
from datetime import datetime

def run_docker_command(cmd):
    """Ejecuta un comando Docker y retorna el resultado"""
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        return result.returncode == 0, result.stdout.strip(), result.stderr.strip()
    except Exception as e:
        return False, "", str(e)

def check_container_status():
    """Verifica el estado de todos los contenedores"""
    print("🐳 Verificando estado de contenedores...")
    
    success, output, error = run_docker_command("docker-compose ps --format json")
    if not success:
        print(f"   ❌ Error obteniendo estado: {error}")
        return False
    
    containers = []
    for line in output.split('\n'):
        if line.strip():
            try:
                containers.append(json.loads(line))
            except:
                pass
    
    all_running = True
    for container in containers:
        status = container.get('State', 'unknown')
        name = container.get('Service', 'unknown')
        if status == 'running':
            print(f"   ✅ {name}: {status}")
        else:
            print(f"   ❌ {name}: {status}")
            all_running = False
    
    return all_running

def check_airflow_services():
    """Verifica que Airflow esté funcionando"""
    print("\n⚡ Verificando servicios de Airflow...")
    
    # Verificar scheduler - método alternativo más confiable
    success, output, error = run_docker_command("docker exec test_gm-airflow-scheduler-1 pgrep -f 'airflow scheduler'")
    if success and output.strip():
        print("   ✅ Airflow Scheduler funcionando")
    else:
        # Método de respaldo usando logs
        success2, output2, error2 = run_docker_command("docker logs test_gm-airflow-scheduler-1 --tail 5 | grep -i 'scheduler'")
        if success2 and "scheduler" in output2.lower():
            print("   ✅ Airflow Scheduler funcionando (verificado por logs)")
        else:
            print("   ❌ Airflow Scheduler no encontrado")
            return False
    
    # Verificar webserver - método más confiable
    success, output, error = run_docker_command("docker exec test_gm-airflow-webserver-1 pgrep -f 'airflow webserver'")
    if success and output.strip():
        print("   ✅ Airflow Webserver funcionando")
    else:
        # Método de respaldo usando puerto
        success2, output2, error2 = run_docker_command("docker exec test_gm-airflow-webserver-1 netstat -tlnp | grep ':8080'")
        if success2 and ":8080" in output2:
            print("   ✅ Airflow Webserver funcionando (puerto 8080 activo)")
        else:
            print("   ❌ Airflow Webserver no encontrado")
            return False
    
    # Verificar DAGs
    success, output, error = run_docker_command("docker exec test_gm-airflow-scheduler-1 airflow dags list")
    if success and "data_pipeline_etl" in output:
        print("   ✅ DAG 'data_pipeline_etl' cargado correctamente")
        return True
    else:
        print("   ❌ DAG 'data_pipeline_etl' no encontrado")
        return False

def check_spark_cluster():
    """Verifica que Spark esté funcionando"""
    print("\n⚡ Verificando cluster de Spark...")
    
    # Verificar Spark Master
    success, output, error = run_docker_command("docker exec test_gm-spark-master-1 ps aux | grep spark")
    if success and "spark" in output.lower():
        print("   ✅ Spark Master funcionando")
    else:
        print("   ❌ Spark Master no encontrado")
        return False
    
    # Verificar Spark Worker
    success, output, error = run_docker_command("docker exec test_gm-spark-worker-1 ps aux | grep spark")
    if success and "spark" in output.lower():
        print("   ✅ Spark Worker funcionando")
        return True
    else:
        print("   ❌ Spark Worker no encontrado")
        return False

def check_databases():
    """Verifica las conexiones a bases de datos"""
    print("\n🗄️ Verificando bases de datos...")
    
    # PostgreSQL
    success, output, error = run_docker_command("docker exec test_gm-postgres-1 pg_isready -h localhost -p 5432")
    if success:
        print("   ✅ PostgreSQL respondiendo")
    else:
        print("   ❌ PostgreSQL no responde")
        return False
    
    # Redis
    success, output, error = run_docker_command("docker exec test_gm-redis-1 redis-cli ping")
    if success and "PONG" in output:
        print("   ✅ Redis respondiendo")
        return True
    else:
        print("   ❌ Redis no responde")
        return False

def check_data_files():
    """Verifica que los archivos de datos estén presentes"""
    print("\n📁 Verificando archivos de datos...")
    
    data_files = [
        "data/raw/sales_data.csv",
        "data/raw/customer_data.json", 
        "data/raw/products_data.csv",
        "data/raw/sales_data_large.csv",
        "data/raw/customer_data_large.json"
    ]
    
    all_present = True
    for file_path in data_files:
        success, output, error = run_docker_command(f"test -f {file_path} && echo 'exists' || echo 'missing'")
        if "exists" in output:
            print(f"   ✅ {file_path}")
        else:
            print(f"   ❌ {file_path} - Missing")
            all_present = False
    
    return all_present

def generate_validation_report():
    """Genera un reporte completo de validación"""
    print("🚀 VALIDACIÓN COMPLETA DEL PIPELINE ETL EN DOCKER")
    print("=" * 60)
    print(f"📅 Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    results = {}
    
    # Ejecutar todas las validaciones
    results['containers'] = check_container_status()
    results['airflow'] = check_airflow_services()
    results['spark'] = check_spark_cluster()
    results['databases'] = check_databases()
    results['data_files'] = check_data_files()
    
    # Resumen final
    print("\n📊 RESUMEN DE VALIDACIÓN")
    print("=" * 30)
    
    total_checks = len(results)
    passed_checks = sum(1 for result in results.values() if result)
    
    for component, status in results.items():
        status_icon = "✅" if status else "❌"
        print(f"{status_icon} {component.title()}: {'PASSED' if status else 'FAILED'}")
    
    print(f"\n📈 Resultado: {passed_checks}/{total_checks} componentes funcionando")
    
    if passed_checks == total_checks:
        print("🎉 ¡PIPELINE COMPLETAMENTE FUNCIONAL!")
        print("\n🌐 Acceso a interfaces:")
        print("   • Airflow UI: http://localhost:8080 (admin/admin123)")
        print("   • Spark UI:   http://localhost:8081")
        print("   • PostgreSQL: localhost:5432 (airflow/airflow123)")
        return True
    else:
        print("⚠️  Algunos componentes necesitan atención")
        return False

if __name__ == "__main__":
    success = generate_validation_report()
    exit(0 if success else 1)
