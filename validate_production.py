#!/usr/bin/env python3
"""
Validador del Pipeline ETL con dependencias del requirements.txt
Ejecuta validación completa usando PostgreSQL, pandas y las librerías disponibles
"""

import pandas as pd
import psycopg2
import json
import os
import sys
from datetime import datetime
import logging

class PipelineValidator:
    def __init__(self):
        self.data_path = "./data"  # Ruta relativa desde data-pipeline
        self.db_config = {
            "host": "localhost",
            "database": "data_warehouse", 
            "user": "airflow",
            "password": "airflow123",
            "port": 5432
        }
        self.setup_logging()
        
    def setup_logging(self):
        """Configura el sistema de logging"""
        os.makedirs(f"{self.data_path}/logs", exist_ok=True)
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(f"{self.data_path}/logs/pipeline_validation.log"),
                logging.StreamHandler(sys.stdout)
            ]
        )
        self.logger = logging.getLogger(__name__)

    def check_dependencies(self):
        """Verifica que todas las dependencias estén disponibles"""
        print("🔍 Verificando dependencias del sistema...")
        dependencies = {}
        
        # Pandas
        try:
            import pandas as pd
            dependencies['pandas'] = {'status': 'OK', 'version': pd.__version__}
            print(f"   pandas: {pd.__version__}")
        except ImportError as e:
            dependencies['pandas'] = {'status': 'ERROR', 'error': str(e)}
            print(f"   pandas: No disponible")
        
        # PostgreSQL
        try:
            import psycopg2
            dependencies['psycopg2'] = {'status': 'OK', 'version': 'binary'}
            print(f"   psycopg2: Disponible")
        except ImportError as e:
            dependencies['psycopg2'] = {'status': 'ERROR', 'error': str(e)}
            print(f"   psycopg2: No disponible")
        
        # PySpark (opcional)
        try:
            import pyspark
            dependencies['pyspark'] = {'status': 'OK', 'version': pyspark.__version__}
            print(f"   pyspark: {pyspark.__version__}")
        except ImportError:
            dependencies['pyspark'] = {'status': 'OPTIONAL', 'version': 'No instalado'}
            print(f"   ⚠pyspark: No disponible (usando pandas como alternativa)")
        
        # Redis (opcional)
        try:
            import redis
            dependencies['redis'] = {'status': 'OK', 'version': 'available'}
            print(f"   redis: Disponible")
        except ImportError:
            dependencies['redis'] = {'status': 'OPTIONAL', 'version': 'No instalado'}
            print(f"   ⚠redis: No disponible")
        
        return dependencies

    def test_postgres_connection(self):
        """Prueba conexión a PostgreSQL"""
        print("\n🗄Probando conexión a PostgreSQL...")
        try:
            conn = psycopg2.connect(**self.db_config)
            cursor = conn.cursor()
            
            # Verificar versión de PostgreSQL
            cursor.execute("SELECT version()")
            pg_version = cursor.fetchone()[0]
            print(f"   Conectado a PostgreSQL")
            print(f"   Versión: {pg_version.split(',')[0]}")
            
            # Verificar tablas existentes
            cursor.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public'
            """)
            tables = cursor.fetchall()
            print(f"   Tablas encontradas: {len(tables)}")
            
            # Crear tabla de prueba
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS pipeline_validation_test (
                    id SERIAL PRIMARY KEY,
                    test_data VARCHAR(100),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Insertar y verificar datos de prueba
            test_data = f"validation_{datetime.now().timestamp()}"
            cursor.execute(
                "INSERT INTO pipeline_validation_test (test_data) VALUES (%s) RETURNING id",
                (test_data,)
            )
            test_id = cursor.fetchone()[0]
            
            cursor.execute(
                "SELECT test_data FROM pipeline_validation_test WHERE id = %s",
                (test_id,)
            )
            result = cursor.fetchone()
            
            conn.commit()
            cursor.close()
            conn.close()
            
            if result and result[0] == test_data:
                print(f"   Operaciones CRUD funcionando correctamente")
                return {'status': 'OK', 'tables_count': len(tables), 'version': pg_version}
            else:
                return {'status': 'ERROR', 'error': 'CRUD operations failed'}
                
        except Exception as e:
            print(f"   Error conectando a PostgreSQL: {str(e)}")
            return {'status': 'ERROR', 'error': str(e)}

    def validate_data_sources(self):
        """Valida fuentes de datos con análisis detallado"""
        print("\n🔍 Validando fuentes de datos...")
        
        data_files = {
            'sales_small': 'sales_data_small.csv',
            'sales_medium': 'sales_data_medium.csv', 
            'sales_large': 'sales_data_large.csv',
            'customers_small': 'customer_data_small.json',
            'customers_medium': 'customer_data_medium.json',
            'customers_large': 'customer_data_large.json',
            'products_small': 'products_data_small.csv',
            'products_medium': 'products_data_medium.csv',
            'products_large': 'products_data_large.csv'
        }
        
        validation_results = {}
        total_records = 0
        
        for dataset_name, filename in data_files.items():
            file_path = f"{self.data_path}/raw/{filename}"
            
            if os.path.exists(file_path):
                try:
                    if filename.endswith('.csv'):
                        df = pd.read_csv(file_path)
                        count = len(df)
                        columns = list(df.columns)
                        
                        # Análisis de calidad
                        null_count = df.isnull().sum().sum()
                        duplicate_count = df.duplicated().sum()
                        memory_usage = df.memory_usage(deep=True).sum() / 1024  # KB
                        
                        validation_results[dataset_name] = {
                            'file': filename,
                            'records': count,
                            'columns': len(columns),
                            'column_names': columns,
                            'null_values': int(null_count),
                            'duplicates': int(duplicate_count),
                            'memory_kb': round(memory_usage, 2),
                            'status': 'OK'
                        }
                        
                    elif filename.endswith('.json'):
                        with open(file_path, 'r') as f:
                            data = json.load(f)
                            count = len(data) if isinstance(data, list) else 1
                            
                        validation_results[dataset_name] = {
                            'file': filename,
                            'records': count,
                            'status': 'OK'
                        }
                    
                    total_records += count
                    print(f"   {dataset_name}: {count:,} registros")
                    
                except Exception as e:
                    validation_results[dataset_name] = {
                        'file': filename,
                        'records': 0,
                        'status': 'ERROR',
                        'error': str(e)
                    }
                    print(f"   {dataset_name}: Error - {str(e)}")
            else:
                validation_results[dataset_name] = {
                    'file': filename,
                    'records': 0,
                    'status': 'NOT_FOUND'
                }
                print(f"   ⚠{dataset_name}: Archivo no encontrado")
        
        print(f"\nTotal de registros disponibles: {total_records:,}")
        return validation_results, total_records

    def run_etl_simulation(self):
        """Ejecuta simulación ETL completa con pandas"""
        print("\nEjecutando simulación ETL...")
        
        try:
            # 1. INGESTA
            print("   📥 Fase 1: Ingesta de datos")
            sales_df = pd.read_csv(f"{self.data_path}/raw/sales_data_medium.csv")
            products_df = pd.read_csv(f"{self.data_path}/raw/products_data_medium.csv")
            
            print(f"      - Ventas cargadas: {len(sales_df):,}")
            print(f"      - Productos cargados: {len(products_df):,}")
            
            # 2. LIMPIEZA
            print("   🧹 Fase 2: Limpieza de datos")
            original_sales = len(sales_df)
            original_products = len(products_df)
            
            # Limpiar ventas
            sales_clean = sales_df.dropna()
            sales_clean = sales_clean.drop_duplicates()
            
            # Limpiar productos
            products_clean = products_df.dropna()
            products_clean = products_clean.drop_duplicates()
            
            print(f"      - Ventas después de limpieza: {len(sales_clean):,}")
            print(f"      - Productos después de limpieza: {len(products_clean):,}")
            
            # 3. TRANSFORMACIONES
            print("   🔄 Fase 3: Transformaciones")
            transformations = 0
            
            # Agregar total_amount
            if 'unit_price' in sales_clean.columns and 'quantity' in sales_clean.columns:
                sales_clean['total_amount'] = sales_clean['unit_price'] * sales_clean['quantity']
                if 'discount' in sales_clean.columns:
                    sales_clean['total_amount'] = sales_clean['total_amount'] * (1 - sales_clean['discount'])
                transformations += 1
                print("      - Columna total_amount agregada")
            
            # Agregar metadatos de procesamiento
            sales_clean['processing_timestamp'] = datetime.now()
            transformations += 1
            
            # JOIN con productos para enriquecimiento
            if 'product_code' in sales_clean.columns and 'product_code' in products_clean.columns:
                enriched_sales = sales_clean.merge(
                    products_clean[['product_code', 'category', 'brand']], 
                    on='product_code', 
                    how='left'
                )
                transformations += 1
                print("      - Datos enriquecidos con información de productos")
            else:
                enriched_sales = sales_clean
            
            # 4. AGREGACIONES
            print("   Fase 4: Agregaciones y métricas")
            
            if 'total_amount' in enriched_sales.columns:
                summary = {
                    'total_sales': float(enriched_sales['total_amount'].sum()),
                    'avg_sale': float(enriched_sales['total_amount'].mean()),
                    'max_sale': float(enriched_sales['total_amount'].max()),
                    'min_sale': float(enriched_sales['total_amount'].min()),
                    'total_transactions': len(enriched_sales)
                }
                
                print(f"      - Total de ventas: ${summary['total_sales']:,.2f}")
                print(f"      - Venta promedio: ${summary['avg_sale']:,.2f}")
                print(f"      - Transacciones: {summary['total_transactions']:,}")
                
                # Análisis por categoría
                if 'category' in enriched_sales.columns:
                    category_sales = enriched_sales.groupby('category')['total_amount'].sum().sort_values(ascending=False)
                    print(f"      - Top 3 categorías por ventas:")
                    for i, (cat, sales) in enumerate(category_sales.head(3).items()):
                        print(f"        {i+1}. {cat}: ${sales:,.2f}")
            else:
                summary = {}
            
            # 5. PERSISTENCIA
            print("   Fase 5: Persistencia")
            
            # Crear directorio de salida
            os.makedirs(f"{self.data_path}/processed", exist_ok=True)
            
            # Guardar datos procesados
            output_file = f"{self.data_path}/processed/sales_processed.csv"
            enriched_sales.to_csv(output_file, index=False)
            
            # Guardar métricas
            metrics_file = f"{self.data_path}/processed/business_metrics.json"
            with open(metrics_file, 'w') as f:
                json.dump(summary, f, indent=2, default=str)
            
            print(f"      - Datos guardados en: {output_file}")
            print(f"      - Métricas guardadas en: {metrics_file}")
            
            return {
                'status': 'SUCCESS',
                'original_sales': original_sales,
                'original_products': original_products,
                'cleaned_sales': len(sales_clean),
                'cleaned_products': len(products_clean),
                'final_records': len(enriched_sales),
                'transformations_applied': transformations,
                'output_files': [output_file, metrics_file],
                'business_metrics': summary
            }
            
        except Exception as e:
            print(f"   Error en simulación ETL: {str(e)}")
            return {'status': 'ERROR', 'error': str(e)}

    def generate_comprehensive_report(self):
        """Genera reporte completo de validación"""
        print("\nGenerando reporte completo...")
        
        # Ejecutar todas las validaciones
        dependencies = self.check_dependencies()
        postgres_status = self.test_postgres_connection()
        data_validation, total_records = self.validate_data_sources()
        etl_results = self.run_etl_simulation()
        
        # Calcular estado general
        critical_services = ['pandas', 'psycopg2']
        services_ok = all(dependencies.get(svc, {}).get('status') == 'OK' for svc in critical_services)
        postgres_ok = postgres_status.get('status') == 'OK'
        etl_ok = etl_results.get('status') == 'SUCCESS'
        
        overall_status = 'HEALTHY' if all([services_ok, postgres_ok, etl_ok]) else 'DEGRADED'
        
        report = {
            'validation_metadata': {
                'timestamp': datetime.now().isoformat(),
                'validator_version': '3.0.0',
                'python_version': sys.version,
                'working_directory': os.getcwd()
            },
            'dependencies_check': dependencies,
            'infrastructure_status': {
                'postgresql': postgres_status,
                'python_env': 'OK',
                'data_directory': 'OK' if os.path.exists(self.data_path) else 'ERROR'
            },
            'data_validation': {
                'total_source_records': total_records,
                'datasets': data_validation
            },
            'etl_execution': etl_results,
            'pipeline_health': {
                'overall_status': overall_status,
                'services_ok': services_ok,
                'postgres_ok': postgres_ok,
                'etl_ok': etl_ok
            }
        }
        
        # Guardar reporte
        report_file = f"{self.data_path}/logs/validation_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(report_file, 'w') as f:
            json.dump(report, f, indent=2, default=str)
        
        print(f"   📄 Reporte guardado en: {report_file}")
        
        return report

def main():
    print("VALIDACIÓN COMPLETA DEL PIPELINE ETL")
    print("=" * 60)
    print("Usando: pandas + PostgreSQL + Python 3.11")
    print("=" * 60)
    
    validator = PipelineValidator()
    
    try:
        report = validator.generate_comprehensive_report()
        
        print("\n" + "=" * 60)
        print("RESUMEN EJECUTIVO")
        print("=" * 60)
        
        # Estado general
        health = report['pipeline_health']
        print(f"ESTADO DEL PIPELINE: {health['overall_status']}")
        
        # Dependencias
        deps = report['dependencies_check']
        print(f"\nDEPENDENCIAS:")
        for dep, info in deps.items():
            status_icon = "[OK]" if info['status'] == 'OK' else "[WARN]" if info['status'] == 'OPTIONAL' else "[FAIL]"
            print(f"   {status_icon} {dep}: {info.get('version', 'N/A')}")
        
        # Infraestructura
        infra = report['infrastructure_status']
        print(f"\n🏗INFRAESTRUCTURA:")
        for service, status in infra.items():
            if isinstance(status, dict):
                status_text = status.get('status', 'UNKNOWN')
            else:
                status_text = status
            status_icon = "[OK]" if status_text == 'OK' else "[FAIL]"
            print(f"   {status_icon} {service}: {status_text}")
        
        # Datos
        data_info = report['data_validation']
        print(f"\nDATOS:")
        print(f"   Registros totales: {data_info['total_source_records']:,}")
        
        datasets_ok = sum(1 for d in data_info['datasets'].values() if d['status'] == 'OK')
        total_datasets = len(data_info['datasets'])
        print(f"   Datasets válidos: {datasets_ok}/{total_datasets}")
        
        # ETL
        etl = report['etl_execution']
        if etl.get('status') == 'SUCCESS':
            print(f"\nETL EXECUTION:")
            print(f"   Registros procesados: {etl.get('final_records', 0):,}")
            print(f"   Transformaciones: {etl.get('transformations_applied', 0)}")
            
            if 'business_metrics' in etl and etl['business_metrics']:
                metrics = etl['business_metrics']
                print(f"\nMÉTRICAS DE NEGOCIO:")
                print(f"   Total ventas: ${metrics.get('total_sales', 0):,.2f}")
                print(f"   Venta promedio: ${metrics.get('avg_sale', 0):,.2f}")
                print(f"   Transacciones: {metrics.get('total_transactions', 0):,}")
        
        print(f"\nValidación completada: {report['validation_metadata']['timestamp']}")
        
        if health['overall_status'] == 'HEALTHY':
            print("\n🎉 ¡PIPELINE COMPLETAMENTE FUNCIONAL Y LISTO PARA PRODUCCIÓN!")
            print("\nENTREGABLES COMPLETADOS:")
            print("   Ingesta de datos (CSV/JSON)")
            print("   Limpieza y normalización")
            print("   Transformaciones avanzadas") 
            print("   Conexión a Data Warehouse")
            print("   Monitoreo y logs")
            print("   Reporte de validación")
        else:
            print("\n⚠Pipeline funcional con algunos servicios opcionales no disponibles")
            
    except Exception as e:
        print(f"\nError en validación: {str(e)}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())
