from pyspark.sql import SparkSession
import psycopg2
from psycopg2.extras import execute_values
import logging

class DataLoader:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("DataLoader") \
            .config("spark.jars", "/opt/spark/jars/postgresql-42.6.0.jar") \
            .getOrCreate()
        
        self.data_path = "/opt/airflow/data"
        self.db_config = {
            "host": "postgres",
            "database": "data_warehouse",
            "user": "airflow",
            "password": "airflow123",
            "port": 5432
        }
        self.logger = self._setup_logger()

    def _setup_logger(self):
        logger = logging.getLogger(__name__)
        handler = logging.FileHandler(f"{self.data_path}/logs/loading.log")
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        return logger

    def _get_db_connection(self):
        """Obtiene conexión a PostgreSQL"""
        return psycopg2.connect(**self.db_config)

    def load_customer_dimension(self):
        """Carga dimensión de clientes"""
        try:
            df = self.spark.read.parquet(f"{self.data_path}/processed/dim_customers")
            
            # Configurar conexión JDBC
            jdbc_url = f"jdbc:postgresql://{self.db_config['host']}:{self.db_config['port']}/{self.db_config['database']}"
            connection_properties = {
                "user": self.db_config["user"],
                "password": self.db_config["password"],
                "driver": "org.postgresql.Driver"
            }
            
            # Cargar datos con upsert usando JDBC
            df.write \
                .mode("overwrite") \
                .option("truncate", "true") \
                .jdbc(jdbc_url, "dim_customers", properties=connection_properties)
            
            count = df.count()
            self.logger.info(f"Loaded {count} customers to data warehouse")
            
            return {"success": True, "records": count}
            
        except Exception as e:
            self.logger.error(f"Error loading customer dimension: {str(e)}")
            return {"success": False, "error": str(e)}

    def load_product_dimension(self):
        """Carga dimensión de productos"""
        try:
            df = self.spark.read.parquet(f"{self.data_path}/processed/dim_products")
            
            jdbc_url = f"jdbc:postgresql://{self.db_config['host']}:{self.db_config['port']}/{self.db_config['database']}"
            connection_properties = {
                "user": self.db_config["user"],
                "password": self.db_config["password"],
                "driver": "org.postgresql.Driver"
            }
            
            df.write \
                .mode("overwrite") \
                .option("truncate", "true") \
                .jdbc(jdbc_url, "dim_products", properties=connection_properties)
            
            count = df.count()
            self.logger.info(f"Loaded {count} products to data warehouse")
            
            return {"success": True, "records": count}
            
        except Exception as e:
            self.logger.error(f"Error loading product dimension: {str(e)}")
            return {"success": False, "error": str(e)}

    def load_sales_fact(self):
        """Carga tabla de hechos de ventas con lookups de IDs"""
        try:
            # Leer datos transformados
            fact_df = self.spark.read.parquet(f"{self.data_path}/processed/fact_sales")
            
            # Leer dimensiones para obtener IDs
            customers_df = self.spark.read \
                .format("jdbc") \
                .option("url", f"jdbc:postgresql://{self.db_config['host']}:{self.db_config['port']}/{self.db_config['database']}") \
                .option("dbtable", "dim_customers") \
                .option("user", self.db_config["user"]) \
                .option("password", self.db_config["password"]) \
                .option("driver", "org.postgresql.Driver") \
                .load() \
                .select("customer_id", "customer_code")
            
            products_df = self.spark.read \
                .format("jdbc") \
                .option("url", f"jdbc:postgresql://{self.db_config['host']}:{self.db_config['port']}/{self.db_config['database']}") \
                .option("dbtable", "dim_products") \
                .option("user", self.db_config["user"]) \
                .option("password", self.db_config["password"]) \
                .option("driver", "org.postgresql.Driver") \
                .load() \
                .select("product_id", "product_code")
            
            # Hacer join para obtener IDs de dimensiones
            final_fact = fact_df.alias("f") \
                .join(customers_df.alias("c"), col("f.customer_code") == col("c.customer_code")) \
                .join(products_df.alias("p"), col("f.product_code") == col("p.product_code")) \
                .select(
                    col("f.transaction_id"),
                    col("c.customer_id"),
                    col("p.product_id"),
                    col("f.sale_date"),
                    col("f.sale_timestamp"),
                    col("f.quantity"),
                    col("f.unit_price"),
                    col("f.discount_amount"),
                    col("f.tax_amount"),
                    col("f.total_amount"),
                    col("f.payment_method"),
                    col("f.currency_code"),
                    col("f.created_at")
                )
            
            # Cargar a la tabla de hechos
            jdbc_url = f"jdbc:postgresql://{self.db_config['host']}:{self.db_config['port']}/{self.db_config['database']}"
            connection_properties = {
                "user": self.db_config["user"],
                "password": self.db_config["password"],
                "driver": "org.postgresql.Driver"
            }
            
            final_fact.write \
                .mode("append") \
                .jdbc(jdbc_url, "fact_sales", properties=connection_properties)
            
            count = final_fact.count()
            self.logger.info(f"Loaded {count} sales records to data warehouse")
            
            return {"success": True, "records": count}
            
        except Exception as e:
            self.logger.error(f"Error loading sales fact: {str(e)}")
            return {"success": False, "error": str(e)}

    def load_to_warehouse(self):
        """Carga todos los datos al Data Warehouse"""
        try:
            total_records = 0
            
            # Cargar dimensiones primero
            customer_result = self.load_customer_dimension()
            if not customer_result["success"]:
                return customer_result
            total_records += customer_result["records"]
            
            product_result = self.load_product_dimension()
            if not product_result["success"]:
                return product_result
            total_records += product_result["records"]
            
            # Cargar tabla de hechos
            sales_result = self.load_sales_fact()
            if not sales_result["success"]:
                return sales_result
            total_records += sales_result["records"]
            
            return {
                "success": True,
                "records_loaded": total_records,
                "message": "All data loaded to warehouse successfully"
            }
            
        except Exception as e:
            self.logger.error(f"Error in data loading pipeline: {str(e)}")
            return {"success": False, "error": str(e)}
        
        finally:
            self.spark.stop()