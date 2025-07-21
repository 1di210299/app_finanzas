-- Tabla de dimensión de clientes
CREATE TABLE IF NOT EXISTS dim_customers (
    customer_id SERIAL PRIMARY KEY,
    customer_code VARCHAR(50) UNIQUE NOT NULL,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(255),
    phone VARCHAR(20),
    country VARCHAR(100),
    city VARCHAR(100),
    registration_date DATE,
    customer_segment VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Tabla de dimensión de productos
CREATE TABLE IF NOT EXISTS dim_products (
    product_id SERIAL PRIMARY KEY,
    product_code VARCHAR(50) UNIQUE NOT NULL,
    product_name VARCHAR(255),
    category VARCHAR(100),
    subcategory VARCHAR(100),
    brand VARCHAR(100),
    unit_price DECIMAL(10,2),
    cost_price DECIMAL(10,2),
    weight_kg DECIMAL(8,3),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Tabla de hechos de ventas
CREATE TABLE IF NOT EXISTS fact_sales (
    sale_id SERIAL PRIMARY KEY,
    transaction_id VARCHAR(100) UNIQUE NOT NULL,
    customer_id INTEGER REFERENCES dim_customers(customer_id),
    product_id INTEGER REFERENCES dim_products(product_id),
    sale_date DATE NOT NULL,
    sale_timestamp TIMESTAMP NOT NULL,
    quantity INTEGER NOT NULL,
    unit_price DECIMAL(10,2) NOT NULL,
    discount_amount DECIMAL(10,2) DEFAULT 0,
    tax_amount DECIMAL(10,2) DEFAULT 0,
    total_amount DECIMAL(12,2) NOT NULL,
    payment_method VARCHAR(50),
    currency_code VARCHAR(3) DEFAULT 'USD',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Tabla de métricas agregadas diarias
CREATE TABLE IF NOT EXISTS daily_sales_summary (
    summary_date DATE PRIMARY KEY,
    total_sales_amount DECIMAL(15,2),
    total_transactions INTEGER,
    unique_customers INTEGER,
    avg_transaction_value DECIMAL(10,2),
    top_product_category VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Tabla de logs de calidad de datos
CREATE TABLE IF NOT EXISTS data_quality_logs (
    log_id SERIAL PRIMARY KEY,
    pipeline_run_id VARCHAR(100),
    table_name VARCHAR(100),
    check_name VARCHAR(100),
    check_result VARCHAR(20), -- PASS, FAIL, WARNING
    check_details TEXT,
    records_processed INTEGER,
    records_failed INTEGER,
    execution_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Índices para optimizar consultas
CREATE INDEX IF NOT EXISTS idx_fact_sales_date ON fact_sales(sale_date);
CREATE INDEX IF NOT EXISTS idx_fact_sales_customer ON fact_sales(customer_id);
CREATE INDEX IF NOT EXISTS idx_fact_sales_product ON fact_sales(product_id);
CREATE INDEX IF NOT EXISTS idx_customers_segment ON dim_customers(customer_segment);
CREATE INDEX IF NOT EXISTS idx_products_category ON dim_products(category);