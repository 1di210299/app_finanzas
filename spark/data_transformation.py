from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import logging

class DataTransformer:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("DataTransformation") \
            .config("spark.sql.adaptive.enabled", "true") \
            .getOrCreate()
        
        self.data_path = "/opt/airflow/data"
        self.logger = self._setup_logger()

    def _setup_logger(self):
        logger = logging.getLogger(__name__)
        handler = logging.FileHandler(f"{self.data_path}/logs/transformation.log")
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        return logger

    def create_customer_dimension(self):
        """Crea dimensión de clientes con SCD Type 1"""
        try:
            df = self.spark.read.parquet(f"{self.data_path}/processed/clean_customers")
            
            # Agregar campos de auditoría
            df = df.withColumn("created_at", current_timestamp()) \
                   .withColumn("updated_at", current_timestamp())
            
            # Seleccionar y renombrar columnas para la dimensión
            dim_customers = df.select(
                col("customer_code"),
                col("first_name"),
                col("last_name"),
                col("email"),
                col("phone"),
                col("country"),
                col("city"),
                col("registration_date"),
                col("customer_segment"),
                col("created_at"),
                col("updated_at")
            )
            
            dim_customers.write \
                .mode("overwrite") \
                .option("compression", "snappy") \
                .parquet(f"{self.data_path}/processed/dim_customers")
            
            count = dim_customers.count()
            self.logger.info(f"Customer dimension created with {count} records")
            
            return {"success": True, "records": count}
            
        except Exception as e:
            self.logger.error(f"Error creating customer dimension: {str(e)}")
            return {"success": False, "error": str(e)}

    def create_product_dimension(self):
        """Crea dimensión de productos"""
        try:
            df = self.spark.read.parquet(f"{self.data_path}/processed/clean_products")
            
            # Agregar campos de auditoría
            df = df.withColumn("created_at", current_timestamp()) \
                   .withColumn("updated_at", current_timestamp())
            
            dim_products = df.select(
                col("product_code"),
                col("product_name"),
                col("category"),
                col("subcategory"),
                col("brand"),
                col("unit_price"),
                col("cost_price"),
                col("weight_kg"),
                col("created_at"),
                col("updated_at")
            )
            
            dim_products.write \
                .mode("overwrite") \
                .option("compression", "snappy") \
                .parquet(f"{self.data_path}/processed/dim_products")
            
            count = dim_products.count()
            self.logger.info(f"Product dimension created with {count} records")
            
            return {"success": True, "records": count}
            
        except Exception as e:
            self.logger.error(f"Error creating product dimension: {str(e)}")
            return {"success": False, "error": str(e)}

    def create_sales_fact(self):
        """Crea tabla de hechos de ventas con enrichment"""
        try:
            # Cargar datos limpios
            sales = self.spark.read.parquet(f"{self.data_path}/processed/clean_sales")
            customers = self.spark.read.parquet(f"{self.data_path}/processed/dim_customers")
            products = self.spark.read.parquet(f"{self.data_path}/processed/dim_products")
            
            # Enriquecer ventas con información de dimensiones
            fact_sales = sales.alias("s") \
                .join(customers.alias("c"), 
                      col("s.customer_code") == col("c.customer_code"), "inner") \
                .join(products.alias("p"), 
                      col("s.product_code") == col("p.product_code"), "inner") \
                .select(
                    col("s.transaction_id"),
                    col("c.customer_code"),
                    col("p.product_code"),
                    col("s.sale_date"),
                    col("s.sale_timestamp"),
                    col("s.quantity"),
                    col("s.unit_price"),
                    col("s.discount_amount"),
                    col("s.tax_amount"),
                    col("s.total_amount"),
                    col("s.payment_method"),
                    col("s.currency_code"),
                    # Campos enriquecidos
                    col("c.customer_segment"),
                    col("p.category").alias("product_category"),
                    col("p.brand").alias("product_brand"),
                    current_timestamp().alias("created_at")
                )
            
            # Calcular métricas adicionales
            fact_sales = fact_sales.withColumn("profit_margin", 
                (col("unit_price") - col("p.cost_price")) / col("unit_price")) \
                .withColumn("order_size_category",
                    when(col("total_amount") < 100, "Small")
                    .when(col("total_amount") < 500, "Medium")
                    .otherwise("Large"))
            
            fact_sales.write \
                .mode("overwrite") \
                .partitionBy("sale_date") \
                .option("compression", "snappy") \
                .parquet(f"{self.data_path}/processed/fact_sales")
            
            count = fact_sales.count()
            self.logger.info(f"Sales fact table created with {count} records")
            
            return {"success": True, "records": count}
            
        except Exception as e:
            self.logger.error(f"Error creating sales fact: {str(e)}")
            return {"success": False, "error": str(e)}

    def create_aggregated_metrics(self):
        """Crea métricas agregadas para reportes"""
        try:
            fact_sales = self.spark.read.parquet(f"{self.data_path}/processed/fact_sales")
            
            # Métricas diarias por categoría
            daily_category_metrics = fact_sales.groupBy("sale_date", "product_category") \
                .agg(
                    sum("total_amount").alias("total_sales"),
                    sum("quantity").alias("total_quantity"),
                    count("transaction_id").alias("transaction_count"),
                    avg("total_amount").alias("avg_transaction_value"),
                    countDistinct("customer_code").alias("unique_customers")
                )
            
            daily_category_metrics.write \
                .mode("overwrite") \
                .partitionBy("sale_date") \
                .parquet(f"{self.data_path}/processed/daily_category_metrics")
            
            # Métricas de clientes (RFM Analysis preparación)
            window_spec = Window.partitionBy("customer_code")
            
            customer_metrics = fact_sales.withColumn("recency_days", 
                    datediff(current_date(), max("sale_date").over(window_spec))) \
                .groupBy("customer_code", "customer_segment") \
                .agg(
                    max("sale_date").alias("last_purchase_date"),
                    count("transaction_id").alias("frequency"),
                    sum("total_amount").alias("monetary_value"),
                    avg("total_amount").alias("avg_order_value"),
                    first("recency_days").alias("recency_days")
                )
            
            customer_metrics.write \
                .mode("overwrite") \
                .parquet(f"{self.data_path}/processed/customer_metrics")
            
            self.logger.info("Aggregated metrics created successfully")
            
            return {"success": True, "message": "Aggregated metrics created"}
            
        except Exception as e:
            self.logger.error(f"Error creating aggregated metrics: {str(e)}")
            return {"success": False, "error": str(e)}

    def transform_sales_data(self):
        """Ejecuta todas las transformaciones"""
        try:
            total_records = 0
            
            # Crear dimensiones
            customer_result = self.create_customer_dimension()
            if not customer_result["success"]:
                return customer_result
            total_records += customer_result["records"]
            
            product_result = self.create_product_dimension()
            if not product_result["success"]:
                return product_result
            total_records += product_result["records"]
            
            # Crear tabla de hechos
            fact_result = self.create_sales_fact()
            if not fact_result["success"]:
                return fact_result
            total_records += fact_result["records"]
            
            # Crear métricas agregadas
            metrics_result = self.create_aggregated_metrics()
            if not metrics_result["success"]:
                return metrics_result
            
            return {
                "success": True,
                "records_transformed": total_records,
                "message": "All transformations completed successfully"
            }
            
        except Exception as e:
            self.logger.error(f"Error in transformation pipeline: {str(e)}")
            return {"success": False, "error": str(e)}
        
        finally:
            self.spark.stop()