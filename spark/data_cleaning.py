from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging
import re

class DataCleaner:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("DataCleaning") \
            .config("spark.sql.adaptive.enabled", "true") \
            .getOrCreate()
        
        self.data_path = "/opt/airflow/data"
        self.logger = self._setup_logger()

    def _setup_logger(self):
        logger = logging.getLogger(__name__)
        handler = logging.FileHandler(f"{self.data_path}/logs/cleaning.log")
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        return logger

    def clean_sales_data(self):
        """Limpia y valida datos de ventas"""
        try:
            df = self.spark.read.parquet(f"{self.data_path}/processed/raw_sales")
            
            initial_count = df.count()
            self.logger.info(f"Starting sales cleaning with {initial_count} records")
            
            # Eliminar duplicados basados en transaction_id
            df = df.dropDuplicates(["transaction_id"])
            
            # Limpiar y validar datos
            df = df.filter(
                (col("transaction_id").isNotNull()) &
                (col("customer_code").isNotNull()) &
                (col("product_code").isNotNull()) &
                (col("quantity") > 0) &
                (col("unit_price") > 0)
            )
            
            # Estandarizar formatos de fecha
            df = df.withColumn("sale_date", 
                             to_date(col("sale_date"), "yyyy-MM-dd"))
            
            # Crear timestamp completo
            df = df.withColumn("sale_timestamp", 
                             to_timestamp(col("sale_date")))
            
            # Calcular campos derivados
            df = df.withColumn("discount_amount", 
                             coalesce(col("discount"), lit(0.0))) \
                   .withColumn("tax_amount", 
                             col("unit_price") * col("quantity") * 0.1) \
                   .withColumn("total_amount", 
                             (col("unit_price") * col("quantity")) - 
                             coalesce(col("discount"), lit(0.0)) + 
                             (col("unit_price") * col("quantity") * 0.1))
            
            # Estandarizar métodos de pago
            df = df.withColumn("payment_method", 
                             upper(trim(col("payment_method"))))
            
            # Agregar código de moneda por defecto
            df = df.withColumn("currency_code", lit("USD"))
            
            # Filtrar registros con fechas válidas
            df = df.filter(col("sale_date").isNotNull())
            
            final_count = df.count()
            cleaned_records = initial_count - final_count
            
            # Guardar datos limpios
            df.write \
                .mode("overwrite") \
                .option("compression", "snappy") \
                .parquet(f"{self.data_path}/processed/clean_sales")
            
            self.logger.info(f"Sales cleaning completed. Removed {cleaned_records} invalid records")
            
            return {"success": True, "records_cleaned": final_count, "records_removed": cleaned_records}
            
        except Exception as e:
            self.logger.error(f"Error cleaning sales data: {str(e)}")
            return {"success": False, "error": str(e)}

    def clean_customer_data(self):
        """Limpia y valida datos de clientes"""
        try:
            df = self.spark.read.parquet(f"{self.data_path}/processed/raw_customers")
            
            initial_count = df.count()
            
            # Eliminar duplicados
            df = df.dropDuplicates(["customer_code"])
            
            # Validar campos obligatorios
            df = df.filter(
                (col("customer_code").isNotNull()) &
                (col("first_name").isNotNull()) &
                (col("last_name").isNotNull())
            )
            
            # Limpiar y estandarizar nombres
            df = df.withColumn("first_name", initcap(trim(col("first_name")))) \
                   .withColumn("last_name", initcap(trim(col("last_name"))))
            
            # Validar y limpiar emails
            email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
            df = df.withColumn("email", 
                             when(col("email").rlike(email_pattern), 
                                  lower(trim(col("email")))).otherwise(None))
            
            # Estandarizar números de teléfono (remover caracteres especiales)
            df = df.withColumn("phone", 
                             regexp_replace(col("phone"), "[^0-9+]", ""))
            
            # Estandarizar países y ciudades
            df = df.withColumn("country", initcap(trim(col("country")))) \
                   .withColumn("city", initcap(trim(col("city"))))
            
            # Validar fecha de registro
            df = df.withColumn("registration_date", 
                             to_date(col("registration_date"), "yyyy-MM-dd"))
            
            # Estandarizar segmentos de cliente
            df = df.withColumn("customer_segment", 
                             upper(trim(col("customer_segment"))))
            
            final_count = df.count()
            
            df.write \
                .mode("overwrite") \
                .option("compression", "snappy") \
                .parquet(f"{self.data_path}/processed/clean_customers")
            
            self.logger.info(f"Customer cleaning completed. Final count: {final_count}")
            
            return {"success": True, "records_cleaned": final_count}
            
        except Exception as e:
            self.logger.error(f"Error cleaning customer data: {str(e)}")
            return {"success": False, "error": str(e)}

    def clean_product_data(self):
        """Limpia y valida datos de productos"""
        try:
            df = self.spark.read.parquet(f"{self.data_path}/processed/raw_products")
            
            initial_count = df.count()
            
            # Eliminar duplicados
            df = df.dropDuplicates(["product_code"])
            
            # Validar campos obligatorios
            df = df.filter(
                (col("product_code").isNotNull()) &
                (col("product_name").isNotNull()) &
                (col("unit_price") > 0)
            )
            
            # Estandarizar texto
            df = df.withColumn("product_name", initcap(trim(col("product_name")))) \
                   .withColumn("category", initcap(trim(col("category")))) \
                   .withColumn("subcategory", initcap(trim(col("subcategory")))) \
                   .withColumn("brand", initcap(trim(col("brand"))))
            
            # Validar precios
            df = df.filter(
                (col("unit_price") > 0) &
                (col("cost_price") > 0) &
                (col("cost_price") <= col("unit_price"))
            )
            
            # Validar peso
            df = df.filter(col("weight_kg") > 0)
            
            final_count = df.count()
            
            df.write \
                .mode("overwrite") \
                .option("compression", "snappy") \
                .parquet(f"{self.data_path}/processed/clean_products")
            
            self.logger.info(f"Product cleaning completed. Final count: {final_count}")
            
            return {"success": True, "records_cleaned": final_count}
            
        except Exception as e:
            self.logger.error(f"Error cleaning product data: {str(e)}")
            return {"success": False, "error": str(e)}

    def clean_all_datasets(self):
        """Limpia todos los datasets"""
        try:
            total_cleaned = 0
            
            # Limpiar ventas
            sales_result = self.clean_sales_data()
            if not sales_result["success"]:
                return sales_result
            total_cleaned += sales_result["records_cleaned"]
            
            # Limpiar clientes
            customer_result = self.clean_customer_data()
            if not customer_result["success"]:
                return customer_result
            total_cleaned += customer_result["records_cleaned"]
            
            # Limpiar productos
            product_result = self.clean_product_data()
            if not product_result["success"]:
                return product_result
            total_cleaned += product_result["records_cleaned"]
            
            return {
                "success": True,
                "records_cleaned": total_cleaned,
                "message": "All datasets cleaned successfully"
            }
            
        except Exception as e:
            self.logger.error(f"Error in overall cleaning: {str(e)}")
            return {"success": False, "error": str(e)}
        
        finally:
            self.spark.stop()