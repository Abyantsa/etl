from sqlalchemy import create_engine, text
import pandas as pd

# PostgreSQL connection config
pg_user = "postgres"
pg_pass = "Aby090305"
pg_host = "localhost"
pg_port = "5432"
pg_adventureworks = "adventureworks1"
pg_stagging = "stagging"
pg_dw_final = "dw_final"


# SQLAlchemy engines
engine_adventure = create_engine(f"postgresql+psycopg2://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_adventureworks}")
engine_stagging = create_engine(f"postgresql+psycopg2://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_stagging}")
engine_dw_final = create_engine(f"postgresql+psycopg2://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_dw_final}")


# Drop all raw & star schema tables
def drop_all_tables():
    tables = [
        "raw_salesorderdetail", "raw_salesorderheader", "raw_product", "raw_customer",
        "raw_person", "raw_productcategory", "raw_productsubcategory",
        "dim_product", "dim_customer", "dim_category", "dim_date", "fact_penjualan"
    ]
    with engine_stagging.connect() as conn:
        for table in tables:
            conn.execute(text(f"DROP TABLE IF EXISTS {table} CASCADE;"))
    print("🧹 All raw and star schema tables dropped.")

# Step A: Copy AdventureWorks to Stagging
def copy_raw_tables():
    tables_to_copy = {
        "Sales.SalesOrderDetail": "raw_salesorderdetail",
        "Sales.SalesOrderHeader": "raw_salesorderheader",
        "Production.Product": "raw_product",
        "Sales.Customer": "raw_customer",
        "Person.Person": "raw_person",
        "Production.ProductCategory": "raw_productcategory",
        "Production.ProductSubcategory": "raw_productsubcategory"
    }
    for src_table, dest_table in tables_to_copy.items():
        print(f"📥 Copying {src_table} to {dest_table}...")
        df = pd.read_sql(f"SELECT * FROM {src_table}", engine_adventure)
        df.to_sql(dest_table, engine_stagging, if_exists='replace', index=False)
        print(f"✅ {dest_table} copied.")

# Step B: Create Star Schema Tables
def create_dim_tables():
    with engine_stagging.connect() as conn:
        conn.execute(text("""
        DROP TABLE IF EXISTS dim_product CASCADE;
        CREATE TABLE dim_product (
            productid INT PRIMARY KEY,
            name TEXT,
            color TEXT,
            size TEXT,
            weight NUMERIC
        );
        """))
        
        conn.execute(text("""
        DROP TABLE IF EXISTS dim_customer CASCADE;
        CREATE TABLE dim_customer (
            customerid INT PRIMARY KEY,
            name TEXT,
            title TEXT,
            demographic TEXT
        );
        """))
        
        conn.execute(text("""
        DROP TABLE IF EXISTS dim_category CASCADE;
        CREATE TABLE dim_category (
            categoryid INT PRIMARY KEY,
            productcategoryid INT,
            categoryname TEXT,
            productsubcategoryid INT,
            subcategoryname TEXT
        );
        """))
        
        conn.execute(text("""
        DROP TABLE IF EXISTS dim_date CASCADE;
        CREATE TABLE dim_date (
            datekey INT PRIMARY KEY,
            fulldate DATE,
            day INT,
            month INT,
            year INT
        );
        """))
        
        conn.execute(text("""
        DROP TABLE IF EXISTS fact_penjualan CASCADE;
        CREATE TABLE fact_penjualan (
            factid SERIAL PRIMARY KEY,
            productid INT REFERENCES dim_product(productid),
            customerid INT REFERENCES dim_customer(customerid),
            categoryid INT,
            qtyproduct INT,
            unitprice NUMERIC,
            unitpricedisc NUMERIC,
            totalpenjualan NUMERIC,
            datekey INT REFERENCES dim_date(datekey)
        );
        """))
    print("✅ Star schema tables created.")


# Transform from raw_* (Stagging)
def extract_fact_sales_order_detail():
    df = pd.read_sql("""
        SELECT SalesOrderDetailID, ProductID, OrderQty, UnitPrice, UnitPriceDiscount, SalesOrderID 
        FROM raw_salesorderdetail
    """, engine_stagging)
    
    # Pastikan kolomnya diubah dengan benar
    df = df.rename(columns={
        'SalesOrderDetailID': 'salesorderdetailid',
        'ProductID': 'productid',
        'OrderQty': 'qtyproduct',
        'UnitPrice': 'unitprice',
        'UnitPriceDiscount': 'unitpricedisc',  # Pastikan kolom ini ada dan sesuai
        'SalesOrderID': 'salesorderid'
    })
    return df
    


def extract_fact_sales_order_header():
    return pd.read_sql("""
        SELECT SalesOrderID, OrderDate, CustomerID 
        FROM raw_salesorderheader
    """, engine_stagging).rename(columns={
        'SalesOrderID': 'salesorderid',
        'OrderDate': 'orderdate',
        'CustomerID': 'customerid'
    })

def extract_dim_product():
    return pd.read_sql("""
        SELECT ProductID, Name, Color, Size, Weight 
        FROM raw_product
    """, engine_stagging).rename(columns={
        'ProductID': 'productid',
        'Name': 'name',
        'Color': 'color',
        'Size': 'size',
        'Weight': 'weight'
    })

def extract_dim_customer():
    return pd.read_sql("""
        SELECT c.CustomerID, 
               p.FirstName || ' ' || p.LastName AS Name,
               p.Title, 
               p.AdditionalContactInfo AS Demographic
        FROM raw_customer c
        JOIN raw_person p ON c.PersonID = p.BusinessEntityID
    """, engine_stagging).rename(columns={
        'CustomerID': 'customerid',
        'Name': 'name',
        'Title': 'title',
        'Demographic': 'demographic'
    })

def extract_dim_category():
    return pd.read_sql("""
        SELECT 
            ROW_NUMBER() OVER() AS CategoryID,
            pc.ProductCategoryID,
            pc.Name AS CategoryName,
            psc.ProductSubcategoryID,
            psc.Name AS SubcategoryName
        FROM raw_productcategory pc
        JOIN raw_productsubcategory psc 
            ON pc.ProductCategoryID = psc.ProductCategoryID
    """, engine_stagging).rename(columns={
        'CategoryID': 'categoryid',
        'ProductCategoryID': 'productcategoryid',
        'CategoryName': 'categoryname',
        'ProductSubcategoryID': 'productsubcategoryid',
        'SubcategoryName': 'subcategoryname'
    })

def generate_dim_date(start='2010-01-01', end='2014-12-31'):
    date_range = pd.date_range(start=start, end=end)
    df = pd.DataFrame()
    df['fulldate'] = date_range
    df['datekey'] = df['fulldate'].dt.strftime('%Y%m%d').astype(int)
    df['day'] = df['fulldate'].dt.day
    df['month'] = df['fulldate'].dt.month
    df['year'] = df['fulldate'].dt.year
    return df[['datekey', 'fulldate', 'day', 'month', 'year']]

def load_to_stagging(df, table_name):
    df.to_sql(table_name, engine_stagging, if_exists='replace', index=False)
    print(f"✅ Loaded {len(df)} rows into {table_name}")

def load_to_dw_final():
    tables = ["dim_product", "dim_customer", "dim_category", "dim_date", "fact_penjualan"]
    for table in tables:
        print(f"⬆️  Loading {table} to dw_final...")
        df = pd.read_sql(f"SELECT * FROM {table}", engine_stagging)
        df.to_sql(table, engine_dw_final, if_exists='replace', index=False)
        print(f"✅ {table} loaded to dw_final.")


# MAIN
if __name__ == "__main__":
    print("🧹 Dropping existing tables...")
    drop_all_tables()

    print("🚀 Copying data from AdventureWorks to Stagging...")
    copy_raw_tables()

    print("📐 Creating star schema...")
    create_dim_tables()

    # Transform and Load dim_product
    print("🔄 Transforming and Loading dim_product...")
    load_to_stagging(extract_dim_product(), "dim_product")

    # Transform and Load dim_customer
    print("🔄 Transforming and Loading dim_customer...")
    load_to_stagging(extract_dim_customer(), "dim_customer")

    # Transform and Load dim_category
    print("🔄 Transforming and Loading dim_category...")
    load_to_stagging(extract_dim_category(), "dim_category")

    # Generate and Load dim_date
    print("📅 Generating and Loading dim_date...")
    load_to_stagging(generate_dim_date(), "dim_date")

    # Transforming and Loading fact_penjualan
    print("📦 Transforming and Loading fact_penjualan...")
    
    # Mengambil data detail dan header untuk faktur penjualan
    df_detail = extract_fact_sales_order_detail()
    df_header = extract_fact_sales_order_header()
    df_fact = pd.merge(df_detail, df_header, on='salesorderid')

    # Memeriksa kolom yang ada di df_detail dan df_header
    print("Kolom di df_detail:", df_detail.columns)
    print("Kolom di df_header:", df_header.columns)


    # Menghitung totalpenjualan
    df_fact['totalpenjualan'] = df_fact['orderqty'] * (df_fact['unitprice'] - df_fact['unitpricediscount'])

    # Menambahkan datekey dari orderdate
    df_fact['datekey'] = pd.to_datetime(df_fact['orderdate']).dt.strftime('%Y%m%d').astype(int)

    # Menambahkan kategori (placeholder, bisa update dengan mapping)
    df_fact['categoryid'] = 1  # Placeholder: bisa diupdate pakai mapping product ke subcategory

    # Pilih kolom yang diperlukan untuk fact_penjualan
    df_fact = df_fact[['productid', 'customerid', 'categoryid', 'orderqty', 'unitprice', 'unitpricediscount', 'totalpenjualan', 'datekey']]

    # Load to stagging
    load_to_stagging(df_fact, "fact_penjualan")

    print("🎉 ETL process completed successfully!")

    print("🏁 Loading data to Data Warehouse (dw_final)...")
    load_to_dw_final()

    print("✅ All data successfully loaded to dw_final!")

