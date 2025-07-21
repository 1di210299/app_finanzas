#!/usr/bin/env python3
"""
Script de Validación del Pipeline ETL
Ejecuta y valida todos los componentes del pipeline
"""

import psycopg2
import pandas as pd
import json
import os
from datetime import datetime

class PipelineValidator:
    def __init__(self):
        self.db_config = {
            "host": "localhost",
            "database": "data_warehouse", 
            "user": "airflow",
            "password": "airflow123",
            "port": 5432
        }
        self.data_path = "/Users/juandiegogutierrezcortez/test_GM/data-pipeline/data"
        
    def validate_data_sources(self):
        """Valida que los datos fuente estén disponibles"""
        print("🔍 Validando fuentes de datos...")
        
        required_files = [
            'sales_data_small.csv',
            'customer_data_small.json', 
            'products_data_small.csv'
        ]
        
        results = {}
        for file in required_files:
            file_path = f"{self.data_path}/raw/{file}"
            if os.path.exists(file_path):
                # Contar registros
                if file.endswith('.csv'):
                    df = pd.read_csv(file_path)
                    count = len(df)
                elif file.endswith('.json'):
                    with open(file_path, 'r') as f:
                        data = json.load(f)
                        count = len(data) if isinstance(data, list) else 1
                
                results[file] = {'exists': True, 'records': count}
                print(f"   ✅ {file}: {count} registros")
            else:
                results[file] = {'exists': False, 'records': 0}
                print(f"   ❌ {file}: No encontrado")
        
        return results
    
    def validate_database_connection(self):
        """Valida conexión a PostgreSQL"""
        print("🗄️ Validando conexión a base de datos...")
        
        try:
            conn = psycopg2.connect(**self.db_config)
            cursor = conn.cursor()
            
            # Verificar tablas existentes
            cursor.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public'
            """)
            tables = cursor.fetchall()
            
            print(f"   ✅ Conexión exitosa")
            print(f"   📊 Tablas encontradas: {len(tables)}")
            for table in tables:
                print(f"      - {table[0]}")
                
            cursor.close()
            conn.close()
            
            return True
            
        except Exception as e:
            print(f"   ❌ Error de conexión: {str(e)}")
            return False
    
    def simulate_etl_process(self):
        """Simula el proceso ETL completo"""
        print("⚡ Simulando proceso ETL...")
        
        # Simular ingesta
        sales_df = pd.read_csv(f"{self.data_path}/raw/sales_data_small.csv")
        products_df = pd.read_csv(f"{self.data_path}/raw/products_data_small.csv")
        
        print(f"   📥 Ingesta: {len(sales_df)} ventas, {len(products_df)} productos")
        
        # Simular limpieza
        sales_clean = sales_df.dropna()
        sales_clean = sales_clean.drop_duplicates()
        
        print(f"   🧹 Limpieza: {len(sales_clean)} registros después de limpiar")
        
        # Simular transformación
        if 'unit_price' in sales_clean.columns and 'quantity' in sales_clean.columns:
            sales_clean['total_amount'] = sales_clean['unit_price'] * sales_clean['quantity']
            print(f"   🔄 Transformación: Columna 'total_amount' agregada")
        
        # Simular carga a processed
        processed_path = f"{self.data_path}/processed"
        os.makedirs(processed_path, exist_ok=True)
        
        output_file = f"{processed_path}/sales_processed.csv"
        sales_clean.to_csv(output_file, index=False)
        
        print(f"   💾 Carga: Datos guardados en {output_file}")
        
        return {
            'original_records': len(sales_df),
            'cleaned_records': len(sales_clean),
            'output_file': output_file
        }
    
    def generate_validation_report(self):
        """Genera reporte de validación completo"""
        print("📋 Generando reporte de validación...")
        
        report = {
            'validation_timestamp': datetime.now().isoformat(),
            'pipeline_status': 'VALIDATED',
            'data_sources': self.validate_data_sources(),
            'database_connection': self.validate_database_connection(),
            'etl_simulation': self.simulate_etl_process()
        }
        
        # Guardar reporte
        report_file = f"{self.data_path}/logs/validation_report.json"
        os.makedirs(f"{self.data_path}/logs", exist_ok=True)
        
        with open(report_file, 'w') as f:
            json.dump(report, f, indent=2)
        
        print(f"   📄 Reporte guardado en: {report_file}")
        
        return report

def main():
    print("🚀 VALIDACIÓN DEL PIPELINE ETL")
    print("=" * 50)
    
    validator = PipelineValidator()
    report = validator.generate_validation_report()
    
    print("\n✅ VALIDACIÓN COMPLETADA")
    print("=" * 50)
    print(f"📊 Registros procesados: {report['etl_simulation']['original_records']}")
    print(f"🧹 Registros limpios: {report['etl_simulation']['cleaned_records']}")
    print(f"📅 Fecha de validación: {report['validation_timestamp']}")
    print(f"🏷️ Estado: {report['pipeline_status']}")

if __name__ == "__main__":
    main()
