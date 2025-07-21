from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import logging
import os
from datetime import datetime

class DataIngestion:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("DataIngestion") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .getOrCreate()
        
        self.data_path = "/opt/airflow/data"
        self.logger = self._setup_logger()

    def _setup_logger(self):
        logger = logging.getLogger(__name__)
        handler = logging.FileHandler(f"{self.data_path}/logs/ingestion.log")
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        return logger

    def ingest_sales_data(self):
        """Ingesta datos de ventas desde CSV"""
        try:
            schema = StructType([
                StructField("transaction_id", StringType(), True),
                StructField("customer_code", StringType(), True),
                StructField("product_code", StringType(), True),
                StructField("sale_date", StringType(), True),
                StructField("quantity", IntegerType(), True),
                StructField("unit_price", DoubleType(), True),
                StructField("discount", DoubleType(), True),
                StructField("payment_method", StringType(), True)
            ])
            
            df = self.spark.read \
                .option("header", "true") \
                .option("inferSchema", "false") \
                .schema(schema) \
                .csv(f"{self.data_path}/raw/sales_data.csv")
            
            # Agregar metadatos de ingesta
            df = df.withColumn("ingestion_timestamp", current_timestamp()) \
                   .withColumn("source_file", lit("sales_data.csv"))
            
            # Guardar datos raw ingestados
            df.write \
                .mode("overwrite") \
                .option("compression", "snappy") \
                .parquet(f"{self.data_path}/processed/raw_sales")
            
            record_count = df.count()
            self.logger.info(f"Sales data ingested: {record_count} records")
            
            return {"success": True, "records": record_count}
            
        except Exception as e:
            self.logger.error(f"Error ingesting sales data: {str(e)}")
            return {"success": False, "error": str(e)}

    def ingest_customer_data(self):
        """Ingesta datos de clientes desde JSON"""
        try:
            df = self.spark.read \
                .option("multiLine", "true") \
                .json(f"{self.data_path}/raw/customer_data.json")
            
            # Normalizar estructura JSON anidada
            df = df.select(
                col("customer_id").alias("customer_code"),
                col("personal_info.first_name").alias("first_name"),
                col("personal_info.last_name").alias("last_name"),
                col("contact.email").alias("email"),
                col("contact.phone").alias("phone"),
                col("address.country").alias("country"),
                col("address.city").alias("city"),
                col("registration_date"),
                col("segment").alias("customer_segment")
            ).withColumn("ingestion_timestamp", current_timestamp()) \
             .withColumn("source_file", lit("customer_data.json"))
            
            df.write \
                .mode("overwrite") \
                .option("compression", "snappy") \
                .parquet(f"{self.data_path}/processed/raw_customers")
            
            record_count = df.count()
            self.logger.info(f"Customer data ingested: {record_count} records")
            
            return {"success": True, "records": record_count}
            
        except Exception as e:
            self.logger.error(f"Error ingesting customer data: {str(e)}")
            return {"success": False, "error": str(e)}

    def ingest_product_data(self):
        """Ingesta datos de productos desde CSV"""
        try:
            schema = StructType([
                StructField("product_code", StringType(), True),
                StructField("product_name", StringType(), True),
                StructField("category", StringType(), True),
                StructField("subcategory", StringType(), True),
                StructField("brand", StringType(), True),
                StructField("unit_price", DoubleType(), True),
                StructField("cost_price", DoubleType(), True),
                StructField("weight_kg", DoubleType(), True)
            ])
            
            df = self.spark.read \
                .option("header", "true") \
                .schema(schema) \
                .csv(f"{self.data_path}/raw/products_data.csv")
            
            df = df.withColumn("ingestion_timestamp", current_timestamp()) \
                   .withColumn("source_file", lit("products_data.csv"))
            
            df.write \
                .mode("overwrite") \
                .option("compression", "snappy") \
                .parquet(f"{self.data_path}/processed/raw_products")
            
            record_count = df.count()
            self.logger.info(f"Product data ingested: {record_count} records")
            
            return {"success": True, "records": record_count}
            
        except Exception as e:
            self.logger.error(f"Error ingesting product data: {str(e)}")
            return {"success": False, "error": str(e)}

    def ingest_all_sources(self):
        """Ingesta todos los datasets"""
        try:
            total_records = 0
            
            # Ingestar ventas
            sales_result = self.ingest_sales_data()
            if not sales_result["success"]:
                return sales_result
            total_records += sales_result["records"]
            
            # Ingestar clientes
            customer_result = self.ingest_customer_data()
            if not customer_result["success"]:
                return customer_result
            total_records += customer_result["records"]
            
            # Ingestar productos
            product_result = self.ingest_product_data()
            if not product_result["success"]:
                return product_result
            total_records += product_result["records"]
            
            self.logger.info(f"Total records ingested: {total_records}")
            
            return {
                "success": True, 
                "records_ingested": total_records,
                "message": "All data sources ingested successfully"
            }
            
        except Exception as e:
            self.logger.error(f"Error in overall ingestion: {str(e)}")
            return {"success": False, "error": str(e)}
        
        finally:
            self.spark.stop()