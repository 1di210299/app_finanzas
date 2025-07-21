#!/usr/bin/env python3
"""
Generador Avanzado de Datos Simulados para Pipeline ETL
Diseñado específicamente para demostrar casos de calidad de datos
"""

import csv
import json
import random
import pandas as pd
from datetime import datetime, timedelta
import uuid
import string
import os

# Configuración de generación
SMALL_SIZE = 50      # Dataset pequeño para pruebas
MEDIUM_SIZE = 500    # Dataset mediano
LARGE_SIZE = 5000    # Dataset grande para rendimiento

# Listas para generar datos realistas
FIRST_NAMES = ['Juan', 'María', 'Carlos', 'Ana', 'Luis', 'Carmen', 'José', 'Patricia', 'Miguel', 'Laura',
               'Pedro', 'Isabel', 'Fernando', 'Rosa', 'Antonio', 'Elena', 'Manuel', 'Sofía', 'Ricardo', 'Victoria',
               'Diego', 'Gabriela', 'Alejandro', 'Beatriz', 'Roberto', 'Mónica', 'Daniel', 'Claudia', 'Andrés', 'Silvia']

LAST_NAMES = ['García', 'Rodriguez', 'López', 'Martínez', 'González', 'Pérez', 'Sánchez', 'Ramírez', 'Cruz', 'Flores',
              'Jiménez', 'Morales', 'Herrera', 'Medina', 'Castillo', 'Ortega', 'Silva', 'Vargas', 'Romero', 'Torres',
              'Ruiz', 'Mendoza', 'Guerrero', 'Vásquez', 'Castro', 'Ramos', 'Fernández', 'Gutiérrez', 'Delgado', 'Paredes']

CITIES = ['Lima', 'Arequipa', 'Cusco', 'Trujillo', 'Piura', 'Chiclayo', 'Huancayo', 'Ica', 'Tacna', 'Puno',
          'Chimbote', 'Ayacucho', 'Cajamarca', 'Pucallpa', 'Sullana', 'Chincha', 'Juliaca', 'Ilo', 'Talara', 'Barranca']

PRODUCT_CATEGORIES = {
    'Electronics': ['Smartphones', 'Laptops', 'Tablets', 'Accessories', 'Audio', 'Cameras'],
    'Clothing': ['Men', 'Women', 'Kids', 'Shoes', 'Accessories'],
    'Home': ['Furniture', 'Decor', 'Kitchen', 'Garden', 'Tools'],
    'Books': ['Fiction', 'Non-Fiction', 'Technical', 'Educational'],
    'Sports': ['Fitness', 'Outdoor', 'Team Sports', 'Water Sports']
}

PAYMENT_METHODS = ['CREDIT_CARD', 'DEBIT_CARD', 'CASH', 'PAYPAL', 'BANK_TRANSFER', 'CRYPTO']
CUSTOMER_SEGMENTS = ['PREMIUM', 'GOLD', 'SILVER', 'BRONZE', 'STANDARD']
CURRENCIES = ['USD', 'PEN', 'EUR']

def ensure_directory():
    """Asegura que el directorio data/raw existe"""
    os.makedirs('data/raw', exist_ok=True)

def generate_sales_data(size, filename):
    """Genera datos de ventas con casos específicos de calidad"""
    print(f"Generando {size} transacciones de ventas...")
    
    sales_data = []
    duplicate_count = 0
    null_count = 0
    
    # Generar transacciones base
    for i in range(1, size + 1):
        # Casos especiales de calidad (10% de los datos)
        if random.random() < 0.1:
            # Caso 1: Duplicados (5%)
            if random.random() < 0.5 and duplicate_count < size * 0.05:
                transaction_id = f"TXN{str(max(1, i-random.randint(1,10))).zfill(6)}"  # ID duplicado
                duplicate_count += 1
            else:
                transaction_id = f"TXN{str(i).zfill(6)}"
            
            # Caso 2: Valores nulos/vacíos (5%)
            if random.random() < 0.5 and null_count < size * 0.05:
                discount = None if random.random() < 0.5 else ""  # Null o vacío
                payment_method = None if random.random() < 0.3 else random.choice(PAYMENT_METHODS)
                null_count += 1
            else:
                discount = round(random.uniform(0, 100), 2)
                payment_method = random.choice(PAYMENT_METHODS)
        else:
            # Datos normales
            transaction_id = f"TXN{str(i).zfill(6)}"
            discount = round(random.uniform(0, 50), 2)
            payment_method = random.choice(PAYMENT_METHODS)
        
        # Generar fecha (últimos 365 días)
        base_date = datetime(2024, 1, 1)
        random_days = random.randint(0, 365)
        sale_date = base_date + timedelta(days=random_days)
        
        # Formatos de fecha inconsistentes (casos de calidad)
        if random.random() < 0.05:  # 5% con formato inconsistente
            if random.random() < 0.3:
                sale_date_str = sale_date.strftime('%d/%m/%Y')  # DD/MM/YYYY
            elif random.random() < 0.3:
                sale_date_str = sale_date.strftime('%m-%d-%Y')  # MM-DD-YYYY
            else:
                sale_date_str = sale_date.strftime('%Y%m%d')    # YYYYMMDD
        else:
            sale_date_str = sale_date.strftime('%Y-%m-%d')      # Formato estándar
        
        # Generar otros campos
        customer_code = f"CUST{str(random.randint(1, min(100, size//2))).zfill(4)}"
        product_code = f"PROD{str(random.randint(1, 50)).zfill(4)}"
        quantity = random.randint(1, 10)
        unit_price = round(random.uniform(10, 2000), 2)
        
        # Casos edge en precios
        if random.random() < 0.02:  # 2% con precios extremos
            unit_price = random.choice([0, -10, 999999.99])  # Precios inválidos
        
        sales_data.append({
            'transaction_id': transaction_id,
            'customer_code': customer_code,
            'product_code': product_code,
            'sale_date': sale_date_str,
            'quantity': quantity,
            'unit_price': unit_price,
            'discount': discount,
            'payment_method': payment_method,
            'currency': random.choice(CURRENCIES),
            'sales_channel': random.choice(['ONLINE', 'STORE', 'PHONE', 'MOBILE_APP']),
            'region': random.choice(['NORTE', 'SUR', 'CENTRO', 'ORIENTE'])
        })
    
    # Guardar CSV
    with open(f'data/raw/{filename}', 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['transaction_id', 'customer_code', 'product_code', 'sale_date', 
                     'quantity', 'unit_price', 'discount', 'payment_method', 
                     'currency', 'sales_channel', 'region']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(sales_data)
    
    print(f"   {len(sales_data)} transacciones generadas")
    print(f"   Duplicados introducidos: {duplicate_count}")
    print(f"   Valores nulos introducidos: {null_count}")
    
    return len(sales_data)

def generate_customer_data(size, filename):
    """Genera datos de clientes con casos de calidad"""
    print(f"Generando {size} clientes...")
    
    customers = []
    duplicate_emails = 0
    
    for i in range(1, size + 1):
        customer_id = f"CUST{str(i).zfill(4)}"
        
        # Nombres con casos especiales
        first_name = random.choice(FIRST_NAMES)
        last_name = random.choice(LAST_NAMES)
        
        # Casos de calidad en nombres
        if random.random() < 0.05:  # 5% con problemas
            if random.random() < 0.3:
                first_name = first_name.lower()  # Minúsculas
            elif random.random() < 0.3:
                first_name = f"  {first_name}  "  # Espacios extra
            else:
                first_name = first_name.upper()  # Mayúsculas
        
        # Email con duplicados ocasionales
        if random.random() < 0.03 and duplicate_emails < size * 0.03:
            email = f"duplicated.email@test.com"  # Email duplicado
            duplicate_emails += 1
        else:
            email_domain = random.choice(['gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com', 'empresa.com'])
            email = f"{first_name.lower().strip()}.{last_name.lower().strip()}{random.randint(1,999)}@{email_domain}"
        
        # Teléfonos con formatos inconsistentes
        if random.random() < 0.1:  # 10% con formato inconsistente
            phone_formats = [
                f"+51{random.randint(900000000, 999999999)}",
                f"051-{random.randint(900,999)}-{random.randint(100,999)}-{random.randint(100,999)}",
                f"({random.randint(900,999)}) {random.randint(100,999)}-{random.randint(1000,9999)}",
                f"{random.randint(900000000, 999999999)}"
            ]
            phone = random.choice(phone_formats)
        else:
            phone = f"+51-999-{random.randint(100000, 999999)}"
        
        # Fechas de registro
        reg_date = datetime(2020, 1, 1) + timedelta(days=random.randint(0, 1400))
        
        # Crear cliente
        customer = {
            "customer_id": customer_id,
            "personal_info": {
                "first_name": first_name,
                "last_name": last_name,
                "full_name": f"{first_name} {last_name}",
                "birth_date": (reg_date - timedelta(days=random.randint(6570, 25550))).strftime('%Y-%m-%d')  # 18-70 años
            },
            "contact": {
                "email": email,
                "phone": phone,
                "secondary_phone": f"+51-{random.randint(900,999)}-{random.randint(100000,999999)}" if random.random() < 0.3 else None
            },
            "address": {
                "country": "Peru",
                "city": random.choice(CITIES),
                "district": f"Distrito {random.randint(1,20)}",
                "postal_code": f"{random.randint(10000,99999)}"
            },
            "registration_date": reg_date.strftime('%Y-%m-%d'),
            "segment": random.choice(CUSTOMER_SEGMENTS),
            "status": random.choice(['ACTIVE', 'INACTIVE', 'SUSPENDED', 'PENDING']),
            "preferences": {
                "marketing_emails": random.choice([True, False]),
                "language": random.choice(['ES', 'EN']),
                "currency": random.choice(CURRENCIES)
            },
            "metadata": {
                "acquisition_channel": random.choice(['ONLINE', 'REFERRAL', 'ADVERTISING', 'ORGANIC']),
                "lifetime_value": round(random.uniform(100, 10000), 2),
                "last_activity": (datetime.now() - timedelta(days=random.randint(0, 90))).strftime('%Y-%m-%d')
            }
        }
        
        customers.append(customer)
    
    # Guardar JSON
    with open(f'data/raw/{filename}', 'w', encoding='utf-8') as jsonfile:
        json.dump(customers, jsonfile, indent=2, ensure_ascii=False)
    
    print(f"   {len(customers)} clientes generados")
    print(f"   Emails duplicados: {duplicate_emails}")
    
    return len(customers)

def generate_product_data(size, filename):
    """Genera datos de productos"""
    print(f"🛍Generando {size} productos...")
    
    products = []
    
    for i in range(1, size + 1):
        # Seleccionar categoría y subcategoría
        category = random.choice(list(PRODUCT_CATEGORIES.keys()))
        subcategory = random.choice(PRODUCT_CATEGORIES[category])
        
        # Generar nombre de producto
        product_adjectives = ['Pro', 'Max', 'Ultra', 'Premium', 'Standard', 'Basic', 'Advanced', 'Elite']
        product_name = f"{random.choice(product_adjectives)} {subcategory} {random.randint(100, 9999)}"
        
        # Precios con lógica de negocio
        cost_price = round(random.uniform(10, 1000), 2)
        markup = random.uniform(1.2, 3.0)  # Margen del 20% al 200%
        unit_price = round(cost_price * markup, 2)
        
        # Casos especiales de calidad
        if random.random() < 0.02:  # 2% con datos inconsistentes
            if random.random() < 0.5:
                cost_price = unit_price * random.uniform(1.1, 2.0)  # Costo mayor que precio (error)
            else:
                unit_price = 0  # Precio en 0 (error)
        
        product = {
            'product_code': f"PROD{str(i).zfill(4)}",
            'product_name': product_name,
            'category': category,
            'subcategory': subcategory,
            'brand': random.choice(['TechCorp', 'GlobalBrand', 'LocalMaker', 'PremiumCo', 'ValueBrand']),
            'unit_price': unit_price,
            'cost_price': cost_price,
            'weight_kg': round(random.uniform(0.1, 50), 3),
            'dimensions': f"{random.randint(5,100)}x{random.randint(5,100)}x{random.randint(5,100)}cm",
            'stock_quantity': random.randint(0, 1000),
            'supplier': f"Supplier_{random.randint(1,20)}",
            'created_date': (datetime.now() - timedelta(days=random.randint(30, 730))).strftime('%Y-%m-%d'),
            'status': random.choice(['ACTIVE', 'DISCONTINUED', 'OUT_OF_STOCK'])
        }
        
        products.append(product)
    
    # Guardar CSV
    with open(f'data/raw/{filename}', 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['product_code', 'product_name', 'category', 'subcategory', 'brand',
                     'unit_price', 'cost_price', 'weight_kg', 'dimensions', 'stock_quantity',
                     'supplier', 'created_date', 'status']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(products)
    
    print(f"   {len(products)} productos generados")
    
    return len(products)

def generate_suppliers_data(size, filename):
    """Genera datos de proveedores (tabla adicional para JOINs)"""
    print(f"Generando {size} proveedores...")
    
    suppliers = []
    
    for i in range(1, size + 1):
        supplier = {
            'supplier_id': f"SUP{str(i).zfill(3)}",
            'company_name': f"Supplier Company {i}",
            'contact_person': f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}",
            'email': f"contact{i}@supplier{i}.com",
            'phone': f"+51-{random.randint(900,999)}-{random.randint(100000,999999)}",
            'country': random.choice(['Peru', 'China', 'USA', 'Germany', 'Brazil']),
            'city': random.choice(CITIES),
            'rating': round(random.uniform(1, 5), 1),
            'contract_start': (datetime.now() - timedelta(days=random.randint(30, 1095))).strftime('%Y-%m-%d'),
            'payment_terms': random.choice(['NET_30', 'NET_60', 'PREPAID', 'COD'])
        }
        suppliers.append(supplier)
    
    # Guardar CSV
    with open(f'data/raw/{filename}', 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['supplier_id', 'company_name', 'contact_person', 'email', 'phone',
                     'country', 'city', 'rating', 'contract_start', 'payment_terms']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(suppliers)
    
    print(f"   {len(suppliers)} proveedores generados")
    return len(suppliers)

def generate_data_quality_report():
    """Genera reporte de casos de calidad insertados"""
    report = {
        "data_quality_cases": {
            "sales_data": {
                "duplicates": "5% de transacciones con IDs duplicados",
                "null_values": "5% de registros con campos nulos/vacíos",
                "date_formats": "5% de fechas en formatos inconsistentes (DD/MM/YYYY, MM-DD-YYYY, YYYYMMDD)",
                "invalid_prices": "2% de precios inválidos (negativos, cero, extremos)",
                "encoding_issues": "Acentos y caracteres especiales en texto"
            },
            "customer_data": {
                "name_formatting": "5% de nombres en formatos inconsistentes (mayúsculas, minúsculas, espacios)",
                "duplicate_emails": "3% de emails duplicados",
                "phone_formats": "10% de teléfonos en formatos inconsistentes",
                "missing_data": "Campos opcionales con valores nulos"
            },
            "product_data": {
                "pricing_logic": "2% de productos con costos mayores al precio de venta",
                "zero_prices": "Algunos productos con precio 0",
                "category_consistency": "Productos bien categorizados para testing"
            }
        },
        "data_volumes": {
            "small_dataset": "50-100 registros para pruebas rápidas",
            "medium_dataset": "500-1000 registros para testing normal",
            "large_dataset": "5000+ registros para pruebas de rendimiento"
        },
        "business_logic": {
            "relationships": "Claves foráneas consistentes entre tablas",
            "temporal_data": "Fechas en rangos lógicos (últimos 3 años)",
            "geographical": "Ciudades reales de Perú",
            "segments": "Segmentación de clientes realista"
        },
        "generation_timestamp": datetime.now().isoformat(),
        "purpose": "Datos generados para evaluación de pipeline ETL"
    }
    
    with open('data/raw/data_quality_report.json', 'w', encoding='utf-8') as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    
    print("Reporte de calidad de datos generado: data_quality_report.json")

def main():
    """Función principal"""
    print("GENERADOR DE DATOS SIMULADOS PARA PIPELINE ETL")
    print("=" * 60)
    print(f"Fecha de generación: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    # Asegurar que existe el directorio
    ensure_directory()
    
    # Datasets pequeños (para pruebas rápidas)
    print("GENERANDO DATASETS PEQUEÑOS (PRUEBAS RÁPIDAS)")
    sales_count = generate_sales_data(SMALL_SIZE, 'sales_data_small.csv')
    customer_count = generate_customer_data(min(20, SMALL_SIZE//2), 'customer_data_small.json')
    product_count = generate_product_data(min(15, SMALL_SIZE//3), 'products_data_small.csv')
    supplier_count = generate_suppliers_data(min(10, SMALL_SIZE//5), 'suppliers_data_small.csv')
    
    print()
    
    # Datasets medianos (para testing normal)
    print("GENERANDO DATASETS MEDIANOS (TESTING NORMAL)")
    sales_count += generate_sales_data(MEDIUM_SIZE, 'sales_data_medium.csv')
    customer_count += generate_customer_data(min(100, MEDIUM_SIZE//5), 'customer_data_medium.json')
    product_count += generate_product_data(min(50, MEDIUM_SIZE//10), 'products_data_medium.csv')
    
    print()
    
    # Datasets grandes (para rendimiento)
    print("GENERANDO DATASETS GRANDES (RENDIMIENTO)")
    sales_count += generate_sales_data(LARGE_SIZE, 'sales_data_large.csv')
    customer_count += generate_customer_data(min(500, LARGE_SIZE//10), 'customer_data_large.json')
    product_count += generate_product_data(min(200, LARGE_SIZE//25), 'products_data_large.csv')
    
    print()
    
    # Generar reporte de calidad
    generate_data_quality_report()
    
    print()
    print("=" * 60)
    print("GENERACIÓN COMPLETADA")
    print(f"Total registros generados:")
    print(f"   Ventas: {sales_count:,}")
    print(f"   Clientes: {customer_count:,}")
    print(f"   🛍Productos: {product_count:,}")
    print(f"   Proveedores: {supplier_count:,}")
    print(f"Archivos creados en: data/raw/")
    print("Reporte de calidad: data/raw/data_quality_report.json")
    print()
    print("Casos de calidad incluidos:")
    print("   Duplicados (5%)")
    print("   Valores nulos (5%)")
    print("   Formatos inconsistentes (5-10%)")
    print("   Lógica de negocio inválida (2%)")
    print("   Problemas de encoding")
    print()
    print("¡Datos listos para tu pipeline ETL!")

if __name__ == "__main__":
    main()