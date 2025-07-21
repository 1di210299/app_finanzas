import psycopg2
from datetime import datetime
import logging
import uuid

class DataQualityMonitor:
    def __init__(self):
        self.db_config = {
            "host": "postgres",
            "database": "data_warehouse", 
            "user": "airflow",
            "password": "airflow123",
            "port": 5432
        }
        self.run_id = str(uuid.uuid4())
        self.logger = self._setup_logger()

    def _setup_logger(self):
        logger = logging.getLogger(__name__)
        handler = logging.FileHandler("/opt/airflow/data/logs/data_quality.log")
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        return logger

    def _get_connection(self):
        return psycopg2.connect(**self.db_config)

    def _log_check_result(self, table_name, check_name, result, details, records_processed, records_failed):
        """Registra resultado de validación en la base de datos"""
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        INSERT INTO data_quality_logs 
                        (pipeline_run_id, table_name, check_name, check_result, check_details, 
                         records_processed, records_failed, execution_time)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """, (self.run_id, table_name, check_name, result, details, 
                          records_processed, records_failed, datetime.now()))
                    conn.commit()
        except Exception as e:
            self.logger.error(f"Error logging check result: {str(e)}")

    def check_data_completeness(self, table_name, expected_min_records=1):
        """Verifica que las tablas tengan datos suficientes"""
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                    record_count = cursor.fetchone()[0]
                    
                    if record_count >= expected_min_records:
                        result = "PASS"
                        details = f"Table has {record_count} records (>= {expected_min_records})"
                        self.logger.info(f"✓ Completeness check passed for {table_name}")
                    else:
                        result = "FAIL"
                        details = f"Table has only {record_count} records (< {expected_min_records})"
                        self.logger.error(f"✗ Completeness check failed for {table_name}")
                    
                    self._log_check_result(table_name, "completeness", result, details, record_count, 0)
                    return {"result": result, "count": record_count}
                    
        except Exception as e:
            self.logger.error(f"Error in completeness check for {table_name}: {str(e)}")
            self._log_check_result(table_name, "completeness", "ERROR", str(e), 0, 0)
            return {"result": "ERROR", "error": str(e)}

    def check_data_uniqueness(self, table_name, unique_columns):
        """Verifica que no haya duplicados en columnas clave"""
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    columns_str = ", ".join(unique_columns)
                    cursor.execute(f"""
                        SELECT COUNT(*) as total_count,
                               COUNT(DISTINCT {columns_str}) as unique_count
                        FROM {table_name}
                    """)
                    
                    total_count, unique_count = cursor.fetchone()
                    duplicates = total_count - unique_count
                    
                    if duplicates == 0:
                        result = "PASS"
                        details = f"No duplicates found in {columns_str}"
                        self.logger.info(f"✓ Uniqueness check passed for {table_name}")
                    else:
                        result = "FAIL"
                        details = f"Found {duplicates} duplicate records in {columns_str}"
                        self.logger.error(f"✗ Uniqueness check failed for {table_name}")
                    
                    self._log_check_result(table_name, "uniqueness", result, details, total_count, duplicates)
                    return {"result": result, "duplicates": duplicates}
                    
        except Exception as e:
            self.logger.error(f"Error in uniqueness check for {table_name}: {str(e)}")
            self._log_check_result(table_name, "uniqueness", "ERROR", str(e), 0, 0)
            return {"result": "ERROR", "error": str(e)}

    def check_referential_integrity(self):
        """Verifica integridad referencial entre tablas"""
        try:
            checks = []
            
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    # Verificar que todos los customer_id en fact_sales existen en dim_customers
                    cursor.execute("""
                        SELECT COUNT(*) 
                        FROM fact_sales fs 
                        LEFT JOIN dim_customers dc ON fs.customer_id = dc.customer_id 
                        WHERE dc.customer_id IS NULL
                    """)
                    orphan_customers = cursor.fetchone()[0]
                    
                    if orphan_customers == 0:
                        result = "PASS"
                        details = "All customer references are valid"
                    else:
                        result = "FAIL" 
                        details = f"Found {orphan_customers} orphan customer references"
                    
                    checks.append({"check": "customer_integrity", "result": result})
                    self._log_check_result("fact_sales", "customer_integrity", result, details, 0, orphan_customers)
                    
                    # Verificar que todos los product_id en fact_sales existen en dim_products
                    cursor.execute("""
                        SELECT COUNT(*) 
                        FROM fact_sales fs 
                        LEFT JOIN dim_products dp ON fs.product_id = dp.product_id 
                        WHERE dp.product_id IS NULL
                    """)
                    orphan_products = cursor.fetchone()[0]
                    
                    if orphan_products == 0:
                        result = "PASS"
                        details = "All product references are valid"
                    else:
                        result = "FAIL"
                        details = f"Found {orphan_products} orphan product references"
                    
                    checks.append({"check": "product_integrity", "result": result})
                    self._log_check_result("fact_sales", "product_integrity", result, details, 0, orphan_products)
            
            return checks
            
        except Exception as e:
            self.logger.error(f"Error in referential integrity check: {str(e)}")
            return [{"check": "referential_integrity", "result": "ERROR", "error": str(e)}]

    def check_business_rules(self):
        """Verifica reglas de negocio específicas"""
        try:
            checks = []
            
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    # Verificar que no hay ventas con cantidad <= 0
                    cursor.execute("SELECT COUNT(*) FROM fact_sales WHERE quantity <= 0")
                    invalid_quantities = cursor.fetchone()[0]
                    
                    if invalid_quantities == 0:
                        result = "PASS"
                        details = "All sales have valid quantities"
                    else:
                        result = "FAIL"
                        details = f"Found {invalid_quantities} sales with invalid quantities"
                    
                    checks.append({"check": "quantity_validation", "result": result})
                    self._log_check_result("fact_sales", "quantity_validation", result, details, 0, invalid_quantities)
                    
                    # Verificar que no hay precios negativos
                    cursor.execute("SELECT COUNT(*) FROM fact_sales WHERE unit_price < 0 OR total_amount < 0")
                    negative_prices = cursor.fetchone()[0]
                    
                    if negative_prices == 0:
                        result = "PASS"
                        details = "All prices are positive"
                    else:
                        result = "FAIL"
                        details = f"Found {negative_prices} records with negative prices"
                    
                    checks.append({"check": "price_validation", "result": result})
                    self._log_check_result("fact_sales", "price_validation", result, details, 0, negative_prices)
                    
                    # Verificar coherencia de fechas (no futuras)
                    cursor.execute("SELECT COUNT(*) FROM fact_sales WHERE sale_date > CURRENT_DATE")
                    future_dates = cursor.fetchone()[0]
                    
                    if future_dates == 0:
                        result = "PASS"
                        details = "No future sales dates found"
                    else:
                        result = "WARNING"
                        details = f"Found {future_dates} sales with future dates"
                    
                    checks.append({"check": "date_validation", "result": result})
                    self._log_check_result("fact_sales", "date_validation", result, details, 0, future_dates)
            
            return checks
            
        except Exception as e:
            self.logger.error(f"Error in business rules check: {str(e)}")
            return [{"check": "business_rules", "result": "ERROR", "error": str(e)}]

    def run_all_checks(self):
        """Ejecuta todas las validaciones de calidad"""
        try:
            self.logger.info(f"Starting data quality checks with run ID: {self.run_id}")
            
            results = {
                "run_id": self.run_id,
                "timestamp": datetime.now(),
                "checks": [],
                "total_checks": 0,
                "passed_checks": 0,
                "failed_checks": 0,
                "warning_checks": 0,
                "critical_failures": 0
            }
            
            # Checks de completitud
            tables_to_check = [
                ("dim_customers", 1),
                ("dim_products", 1), 
                ("fact_sales", 1)
            ]
            
            for table, min_records in tables_to_check:
                check_result = self.check_data_completeness(table, min_records)
                results["checks"].append({
                    "table": table,
                    "check": "completeness",
                    "result": check_result["result"]
                })
                results["total_checks"] += 1
                if check_result["result"] == "PASS":
                    results["passed_checks"] += 1
                elif check_result["result"] == "FAIL":
                    results["failed_checks"] += 1
                    results["critical_failures"] += 1
            
            # Checks de unicidad
            uniqueness_checks = [
                ("dim_customers", ["customer_code"]),
                ("dim_products", ["product_code"]),
                ("fact_sales", ["transaction_id"])
            ]
            
            for table, columns in uniqueness_checks:
                check_result = self.check_data_uniqueness(table, columns)
                results["checks"].append({
                    "table": table,
                    "check": "uniqueness", 
                    "result": check_result["result"]
                })
                results["total_checks"] += 1
                if check_result["result"] == "PASS":
                    results["passed_checks"] += 1
                elif check_result["result"] == "FAIL":
                    results["failed_checks"] += 1
                    results["critical_failures"] += 1
            
            # Checks de integridad referencial
            integrity_checks = self.check_referential_integrity()
            for check in integrity_checks:
                results["checks"].append(check)
                results["total_checks"] += 1
                if check["result"] == "PASS":
                    results["passed_checks"] += 1
                elif check["result"] == "FAIL":
                    results["failed_checks"] += 1
                    results["critical_failures"] += 1
            
            # Checks de reglas de negocio
            business_checks = self.check_business_rules()
            for check in business_checks:
                results["checks"].append(check)
                results["total_checks"] += 1
                if check["result"] == "PASS":
                    results["passed_checks"] += 1
                elif check["result"] == "FAIL":
                    results["failed_checks"] += 1
                elif check["result"] == "WARNING":
                    results["warning_checks"] += 1
            
            # Log resumen
            self.logger.info(f"""Data Quality Summary:
                Total Checks: {results['total_checks']}
                Passed: {results['passed_checks']}
                Failed: {results['failed_checks']}
                Warnings: {results['warning_checks']}
                Critical Failures: {results['critical_failures']}
            """)
            
            return results
            
        except Exception as e:
            self.logger.error(f"Error in data quality monitoring: {str(e)}")
            return {"critical_failures": 1, "error": str(e)}