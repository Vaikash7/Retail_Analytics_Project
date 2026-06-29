# Databricks notebook source
# MAGIC %md
# MAGIC ####Products

# COMMAND ----------

import csv
import random
import os

# -------------------------------
# Configuration
# -------------------------------
BASE_DIR = "/Volumes/retail_catalog/default/retail_platform_volume/datasets/products"
OUTPUT_FILE = "products.csv"
NUM_RECORDS = 5000

os.makedirs(BASE_DIR, exist_ok=True)
file_path = os.path.join(BASE_DIR, OUTPUT_FILE)

# -------------------------------
# Reference Data
# -------------------------------
categories = {
    "Electronics": ["Smartphones", "Laptops", "Accessories"],
    "Fashion": ["Men Wear", "Women Wear", "Footwear"],
    "Home": ["Furniture", "Cookware"],
    "Sports": ["Cricket", "Football", "Fitness"],
    "Books": ["Fiction", "Education"],
    "Beauty": ["Skincare", "Cosmetics"]
}

brands = [
    "Apple","Samsung","Dell","HP",
    "Sony","Nike","Adidas","Puma",
    "LG","Philips"
]

# -------------------------------
# Data Generation
# -------------------------------
with open(file_path, mode="w", newline="", encoding="utf-8") as file:

    writer = csv.writer(file)

    writer.writerow([
        "ProductID",
        "ProductName",
        "Category",
        "SubCategory",
        "Brand",
        "CostPrice"
    ])

    for i in range(NUM_RECORDS):

        category = random.choice(list(categories.keys()))

        product_id = random.choice([
            str(i + 1),
            str(random.randint(1,1000))  # duplicates
        ])

        brand = random.choice([
            random.choice(brands),
            random.choice(brands),
            None
        ])

        cost_price = random.choice([
            str(round(random.uniform(100,50000),2)),
            str(round(random.uniform(100,50000),2)),
            "-100"
        ])

        writer.writerow([
            product_id,
            f"Product_{random.randint(1,100000)}",
            category,
            random.choice(categories[category]),
            brand,
            cost_price
        ])

print(f"Generated {NUM_RECORDS} records")
print(file_path)

df = spark.read \
    .option("header","true") \
    .option("inferSchema","false") \
    .csv("/Volumes/retail_catalog/default/retail_platform_volume/datasets/products")

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Orders

# COMMAND ----------

import csv
import random
from datetime import datetime, timedelta
import os

# -------------------------------
# Configuration
# -------------------------------
BASE_DIR = "/Volumes/retail_catalog/default/retail_platform_volume/datasets/orders"

NUM_BATCHES = 4
RECORDS_PER_BATCH = 20000

os.makedirs(BASE_DIR, exist_ok=True)

# -------------------------------
# Reference Data
# -------------------------------
stores = [
    "HYD01",
    "BLR01",
    "CHE01",
    "MUM01",
    "DEL01",
    "PUN01"
]

start_date = datetime(2024, 1, 1)
end_date = datetime(2026, 1, 1)

# -------------------------------
# Helper Function
# -------------------------------
def random_date(start, end):
    delta = end - start
    return start + timedelta(days=random.randint(0, delta.days))

# -------------------------------
# Batch Generation
# -------------------------------
for batch_num in range(1, NUM_BATCHES + 1):

    output_file = f"orders_batch_{batch_num}.csv"
    file_path = os.path.join(BASE_DIR, output_file)

    with open(file_path, mode="w", newline="", encoding="utf-8") as file:

        writer = csv.writer(file)

        writer.writerow([
            "OrderID",
            "CustomerID",
            "ProductID",
            "OrderDate",
            "Quantity",
            "UnitPrice",
            "StoreCode"
        ])

        for i in range(RECORDS_PER_BATCH):

            # Duplicate Order IDs
            order_id = random.choice([
                str(random.randint(100001, 180000)),
                str(random.randint(100001, 120000))
            ])

            # Null + Bad Quantity
            quantity = random.choice([
                str(random.randint(1, 10)),
                str(random.randint(1, 10)),
                str(random.randint(1, 10)),
                None,
                "-5"
            ])

            # Null Price
            unit_price = random.choice([
                str(round(random.uniform(100, 50000), 2)),
                str(round(random.uniform(100, 50000), 2)),
                None
            ])

            # Invalid Date
            order_date = random.choice([
                random_date(start_date, end_date).strftime("%Y-%m-%d"),
                random_date(start_date, end_date).strftime("%Y-%m-%d"),
                "2026-99-99"
            ])

            writer.writerow([
                order_id,
                str(random.randint(2001, 22000)),
                str(random.randint(1, 5000)),
                order_date,
                quantity,
                unit_price,
                random.choice(stores)
            ])

    print(f"Generated: {output_file}")

print("All Order Batches Generated Successfully")


df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "false") \
    .csv("/Volumes/retail_catalog/default/retail_platform_volume/datasets/orders")

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Customers

# COMMAND ----------

import csv
import random
from datetime import datetime, timedelta
import os

# -------------------------------
# Configuration
# -------------------------------
BASE_DIR = "/Volumes/retail_catalog/default/retail_platform_volume/datasets/customers"
OUTPUT_FILE = "customers.csv"
NUM_RECORDS = 20000

os.makedirs(BASE_DIR, exist_ok=True)
file_path = os.path.join(BASE_DIR, OUTPUT_FILE)

# -------------------------------
# Reference Data
# -------------------------------
cities = [
    ("Hyderabad","Telangana"),
    ("Bangalore","Karnataka"),
    ("Chennai","Tamil Nadu"),
    ("Mumbai","Maharashtra"),
    ("Delhi","Delhi"),
    ("Pune","Maharashtra")
]

# -------------------------------
# Data Generation
# -------------------------------
with open(file_path, mode="w", newline="", encoding="utf-8") as file:

    writer = csv.writer(file)

    writer.writerow([
        "CustomerID",
        "FirstName",
        "LastName",
        "Email",
        "Phone",
        "City",
        "State",
        "LastUpdated"
    ])

    for i in range(NUM_RECORDS):

        city,state = random.choice(cities)

        customer_id = random.choice([
            str(2000 + i),
            str(random.randint(2001,5000))  # duplicates
        ])

        email = random.choice([
            f"user{i}@gmail.com",
            f"user{i}@yahoo.com",
            None
        ])

        phone = random.choice([
            f"9{random.randint(100000000,999999999)}",
            f"9{random.randint(100000000,999999999)}",
            "INVALID_PHONE"
        ])

        writer.writerow([
            customer_id,
            f"FirstName_{random.randint(1,10000)}",
            f"LastName_{random.randint(1,10000)}",
            email,
            phone,
            city,
            state,
            str(datetime.now())
        ])

print(f"Generated {NUM_RECORDS} records")
print(file_path)

df = spark.read \
    .option("header","true") \
    .option("inferSchema","false") \
    .csv("/Volumes/retail_catalog/default/retail_platform_volume/datasets/customers")

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ####exchange rates

# COMMAND ----------

import json
import random
from datetime import datetime, timedelta

path = "/Volumes/retail_catalog/default/retail_platform_volume/datasets/exchange_rates/exchange_rates.json"

currencies = ["INR","EUR","GBP","AED","JPY"]

records = []

for i in range(100):

    record = {
        "base": "USD",
        "date": (
            datetime(2025,1,1)
            + timedelta(days=i)
        ).strftime("%Y-%m-%d"),
        "rates": {
            c: str(
                round(
                    random.uniform(0.5,100),
                    2
                )
            )
            for c in currencies
        }
    }

    records.append(record)

with open(path,"w") as f:

    for row in records:
        f.write(
            json.dumps(row)
            + "\n"
        )

print("JSON Generated")