**Python Data Engineering:** Building data pipelines, ETL processes, and automation.
Building data pipelines and automating ETL processes using **Snowpark for Python** leverages Snowflake's scalable cloud data platform. Below is a structured guide with examples, best practices, and automation strategies.

---

### **1. Setup & Configuration**
#### Install Snowpark Python Package:
```bash
pip install snowflake-snowpark-python
```

#### Configure Connection to Snowflake:
Use a `connection.json` file or environment variables:
```python
from snowflake.snowpark import Session

connection_params = {
    "account": "<your_account>",
    "user": "<your_user>",
    "password": "<your_password>",
    "role": "<your_role>",
    "warehouse": "<your_warehouse>",
    "database": "<your_database>",
    "schema": "<your_schema>"
}

session = Session.builder.configs(connection_params).create()
print(session.sql("select current_warehouse(), current_database(), current_schema()").collect())
```

---

### **2. Building a Simple ETL Pipeline**
#### **Example Use Case**:  
Extract sales data, calculate total revenue per region, and load into a target table.

#### **Step 1: Extract**
Read data from Snowflake tables or external stages:
```python
# Read from a Snowflake table
sales_df = session.table("RAW_SALES_DATA")

# Read from an external stage (e.g., CSV file in S3)
stage_df = session.read.option("pattern", ".*sales_data.*.csv").csv("@MY_STAGE/sales/")

from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
from snowflake.snowpark.types import IntegerType, StringType, StructField, StructType, DateType,TimestampType,DoubleType

connection_parameters = {"account":"KQHYZCT-KZ61200",
"user":"SYEDSYD26",
"password": "st0drawih8C=9-fEPLju",
"role":"ACCOUNTADMIN",
"warehouse":"COMPUTE_WH",
"database":"DEMO_DB",
"schema":"PUBLIC"
}

session = Session.builder.configs(connection_parameters).create()

employee_s3 = session.read.csv('@my_s3_stage/employee/')

schema = StructType([StructField("FIRST_NAME", StringType()),
StructField("LAST_NAME", StringType()),
StructField("EMAIL", StringType()),
StructField("ADDRESS", StringType()),
StructField("CITY", StringType()),
 StructField("DOJ",DateType())])

# Use session.read.schema and session.read.csv and mention the command to read data from s3
employee_s3 = session.read.schema(schema).csv('@my_s3_stage/employee/') # IN CASE OF ERRORS, IT CREATE THE TABLE AND EXECUTE IT; IN ERROR CASE, IT DELETE THE TEMP TABLE
employee_s3.show()
employee_s3 = session.read.options({"ON_ERROR":"CONTINUE"}).schema(schema).csv('@my_s3_stage/employee/') # TO HANDLE THE ERROR
employee_s3.show() # EVERY TIME .SHOW() is executed, it CREATE TABLE => COPY THE DATA INTO THE TABLE => SELECT THE TABLE => DROP THE TABLE
type(employee_s3)

employee_s3 = employee_s3.cache_result() # HERE IT IS GOOD TO CACHE THE RESULT
employee_s3.is_cached

employee_s4=employee_s3.cache_result()
```

#### **Step 2: Transform**
Use Snowpark DataFrame APIs for transformations:
```python
from snowflake.snowpark.functions import col, sum as _sum

# Filter and aggregate data
transformed_df = (
    stage_df
    .filter(col("AMOUNT") > 0)
    .groupBy("REGION")
    .agg(_sum(col("AMOUNT")).as_("TOTAL_REVENUE"))
)

type(employee_s5)

employee_s3.columns

employee_s5=employee_s3.select("FIRST_NAME","LAST_NAME").filter(col("FIRST_NAME")=='Nyssa')
employee_s5.show()

employee_s3.show()

employee_s3.queries # TO SEE THE QUERIES
```

#### **Step 3: Load**
Write results back to Snowflake:
```python
transformed_df.write.mode("overwrite").save_as_table("AGGREGATED_SALES")
```

#### **Step 4: Automate with Stored Procedures**
Encapsulate logic into a stored procedure:
```python
def run_etl(session: Session):
    try:
        # Your ETL logic here
        transformed_df = session.table("RAW_SALES_DATA").groupBy(...)
        transformed_df.write.mode("overwrite").save_as_table("AGGREGATED_SALES")
        return "Success"
    except Exception as e:
        return f"Error: {str(e)}"

# Register the stored procedure
session.sproc.register(run_etl, name="RUN_SALES_ETL", is_permanent=True, stage_location="@MY_STAGE/sprocs")

session = Session.builder.configs(connection_parameters).create()

customer = session.table("SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.CUSTOMER")

customer = customer.filter(col("C_NATIONKEY")=='23').select("C_NAME")

customer.write.mode() #not to be executed

customerwrt = customer.write.mode("append").save_as_table("DEMO_DB.PUBLIC.SNOW_CUSTOMER") # when it is executed, it will create a table and insert the data of the previous query => .mode("overwrite")
# when the above command re-executed, then it will recreate the table and re-insert the data in it. => .mode("overwrite")
# when the above command have .mode("append") => then it will check for the table and if it exist then it will append the data to it.
# NOTE: in write method, we do not get any information on how many records inserted, therefore we need to query SQL in SNOWFLAKE
employee_s3_json.write.mode("overwrite").save_as_table("DEMO_DB.PUBLIC.JSON_BOOK_PARSED")

df.write.mode("overwrite").parquet("@my_stage/output_data/") # Overwrite a file in a Stage
customer.write.mode("append").save_as_table("DEMO_DB.PUBLIC.SNOW_CUSTOMER") # Append to an existing table
df.write.mode("error").csv("@my_stage/data.csv")  # Throws error if data.csv exists

```

---

### **3. Automation Strategies**
#### **Option 1: Snowflake Tasks**
Schedule pipelines using Snowflake’s native task scheduler:
```sql
CREATE TASK AGGREGATE_SALES_TASK
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = 'USING CRON 0 8 * * * UTC'
AS
  CALL RUN_SALES_ETL();
```

#### **Option 2: External Schedulers (e.g., Apache Airflow)**
Use Snowflake hooks in Airflow DAGs:
```python
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

def run_snowpark_etl():
    hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
    hook.run("CALL RUN_SALES_ETL()")

# In your DAG
PythonOperator(task_id="run_etl", python_callable=run_snowpark_etl)
```

---

### **4. Advanced Features**
#### **User-Defined Functions (UDFs)**
Custom transformations in Python:
```python
from snowflake.snowpark.types import IntegerType
from snowflake.snowpark.functions import udf

@udf(name="ADD_TAX", return_type=IntegerType(), input_types=[IntegerType()])
def add_tax(amount: int) -> int:
    return int(amount * 1.1)

# Use in a DataFrame
df.withColumn("AMOUNT_WITH_TAX", add_tax(col("AMOUNT")))
```

```python
from snowflake.snowpark.types import IntegerType, StringType
from snowflake.snowpark.functions import udf, col
import pandas as pd

# create or replace stage demo_db.public.udf_stage -- SQL 
@udf(session = session_new,name='a_plus_b', input_types=[IntegerType(), IntegerType()], return_type=IntegerType(), stage_location='@udf_stage',is_permanent=True, replace=True)
def a_plus_b(a: int, b: int) -> int:
    return a+b
# required stage location for this.
```

```sql
SELECT C_CURRENT_HDEMO_SK, A_PLUS_B(C_CURRENT_HDEMO_SK,3) FROM DEMO_DB.PUBLIC.CUSTOMER_TEST LIMIT 10; --if limit is 10, then the python code will be executed 10 times.

SELECT C_CURRENT_HDEMO_SK, A_PLUS_B(C_CURRENT_HDEMO_SK,3) FROM DEMO_DB.PUBLIC.CUSTOMER_TEST 
WHERE C_CURRENT_HDEMO_SK IS NOT NULL LIMIT 1000;
-- NOTE: here the function would have executed 962 times, because rest of the value of C_CURRENT_HDEMO_SK is NULL. 

CREATE FUNCTION A_PLUS_B_SQL(a INTEGER, b INTEGER)
    RETURNS INTEGER
    AS
    $$
        a + b
    $$
;

SELECT C_CURRENT_HDEMO_SK, A_PLUS_B_SQL(C_CURRENT_HDEMO_SK,3) FROM DEMO_DB.PUBLIC.CUSTOMER_TEST 
WHERE C_CURRENT_HDEMO_SK IS NOT NULL LIMIT 1000;

```
#### A_PLUS_B_SQL vs A_PLUS_B
- A_PLUS_B_SQL is a SQL function and A_PLUS_B is a python UDF.
- A_PLUS_B_SQL is executed in the snowflake engine and A_PLUS_B is executed in the python engine.
- A_PLUS_B_SQL is faster than A_PLUS_B because it is executed in the snowflake engine and A_PLUS_B is executed in the python engine.
- A_PLUS_B_SQL took 86ms and A_PLUS_B took 962ms for 1000 records.

#### **Caching & Optimization**
- Cache intermediate results:
  ```python
  transformed_df.cache_result()
  ```
- Push down filters/aggregations to Snowflake for performance.

---

### **5. Best Practices**
1. **Security**:
   - Use **roles with least privileges**.
   - Store credentials in **secret managers** (e.g., AWS Secrets Manager).
2. **Error Handling**:
   ```python
   try:
       # ETL logic
   except Exception as e:
       session.rollback()
       raise
   finally:
       session.close()
   ```
3. **Monitoring**:
   - Track task history in Snowflake:
     ```sql
     SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY());
     ```
4. **Version Control**:
   - Store code in Git and use CI/CD pipelines for deployments.

---

### **6. Example Pipeline Architecture**
```
[Raw Data Sources] → [Snowflake Stage] → [Snowpark ETL] → [Aggregated Tables] → [Dashboards]
```
- **Sources**: CSVs, JSON, streaming data (via Snowflake Streams).
- **Automation**: Triggered via Tasks or Airflow.
- **Output**: Power BI/Tableau dashboards or ML models.

---

### **7. Troubleshooting Tips**
- Use `df.show()` and `df.explain()` to debug DataFrames.
- Check Snowflake query history for performance bottlenecks.
- Enable logging:
  ```python
  import logging
  logging.basicConfig(level=logging.INFO)
  ```

---

By combining Snowpark’s in-database processing with automation tools, you can build scalable, maintainable pipelines. For complex workflows, pair Snowflake Tasks with Airflow for orchestration.


**Snowflake:** Cloud data warehousing, querying, and optimization.
Here’s a comprehensive guide to **Snowflake** for cloud data warehousing, querying, and optimization:

---

### **1. Snowflake Architecture Overview**
Snowflake is a **fully managed, cloud-native data warehouse** built for scalability and performance. Key features include:
- **Separation of Compute & Storage**:  
  - Storage scales automatically (uses cloud object storage like AWS S3, Azure Blob, or GCP Cloud Storage).  
  - Compute scales via **virtual warehouses** (clusters of resources for query execution).  
- **Elasticity**: Automatically scales up/down based on workload.  
- **Multi-Cluster Warehouses**: Supports concurrent workloads (e.g., separate warehouses for reporting and ETL).  
- **Zero Copy Cloning**: Create instant, cost-free copies of databases/tables for testing/development.  
- **Data Sharing**: Share live data across Snowflake accounts securely (no data movement).  

---

### **2. Core Querying Concepts**
Snowflake uses **SQL** as its primary interface. Key capabilities:
#### **Structured Data**:
```sql
SELECT customer_id, SUM(order_amount) AS total_spent
FROM orders
WHERE order_date > '2023-01-01'
GROUP BY customer_id
ORDER BY total_spent DESC;
```

#### **Semi-Structured Data (JSON/XML/Parquet)**:
- Use `VARIANT` type for JSON:
  ```sql
  SELECT 
    value:customer_id::INT AS customer_id,
    value:order_details[0].product_id::STRING AS first_product
  FROM parsed_orders;
  ```

#### **Window Functions**:
```sql
SELECT 
  product_id,
  sales_date,
  SUM(units_sold) OVER (PARTITION BY product_id ORDER BY sales_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total
FROM sales;
```
```python
from snowflake.snowpark.functions import col, sum as snow_sum
from snowflake.snowpark.window import Window, WindowFrame

# Load the sales table
sales_df = session.table("sales")

# Define the window specification
window_spec = (
    Window.partition_by("product_id")  # Equivalent to PARTITION BY product_id
    .order_by("sales_date")            # Equivalent to ORDER BY sales_date
    .rows_between(                     # Equivalent to ROWS BETWEEN ... 
        WindowFrame.unboundedPreceding, 
        WindowFrame.currentRow         # UNBOUNDED PRECEDING AND CURRENT ROW
    )
)

# Calculate running total
result_df = sales_df.select(
    col("product_id"),
    col("sales_date"),
    snow_sum("units_sold").over(window_spec).alias("running_total")  # SUM() OVER (...)
)

# Show the result
result_df.show()
```

```sql
-- Query to find the top-ranked supplier(s) by account balance
WITH RankedSuppliers AS (
    SELECT
        s_name,
        s_acctbal,
        RANK() OVER (ORDER BY s_acctbal DESC) AS RANK
    FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.SUPPLIER
)
SELECT s_name, s_acctbal
FROM RankedSuppliers
WHERE RANK = 1;
```

```python
from snowflake.snowpark.window import Window
from snowflake.snowpark.functions import col, rank

# Load the supplier table
supplier = session.table("SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.SUPPLIER")

# Define the window specification to rank suppliers by account balance
window_spec = Window.order_by(col("s_acctbal").desc())

# Apply rank() and filter for the top-ranked supplier(s)
supplier_rank = supplier.select(
    col("s_name"),
    col("s_acctbal"),
    rank().over(window_spec).as_("RANK")
).filter(col("RANK") == 1)

# Show result
supplier_rank.show()
```

#### **Time Travel & Fail-safe**:
- Query historical data (up to 90 days):
  ```sql
  SELECT * FROM orders 
  AT (TIMESTAMP => '2023-12-31 23:59:59');

  SELECT *
  FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY(DATEADD('HOURS',-1,CURRENT_TIMESTAMP()),CURRENT_TIMESTAMP()))
  ORDER BY START_TIME;


  ```

---

### **3. Optimization Strategies**
#### **A. Table Design**
- **Clustering Keys**:  
  Improve query performance on large tables by defining clustering keys (e.g., `DATE`, `REGION`):
  ```sql
  CREATE TABLE sales (
    order_id INT,
    order_date DATE,
    region STRING
  ) CLUSTER BY (region, order_date);
  ```

```python
from snowflake.snowpark.types import StructType, StructField, IntegerType, DateType, StringType

# Define the table schema (columns and their data types)
schema = StructType([
    StructField("order_id", IntegerType()),
    StructField("order_date", DateType()),
    StructField("region", StringType())
])

# Create the table with clustering on "region" and "order_date"
session.create_table(
    "sales", 
    schema=schema, 
    cluster_by=["region", "order_date"]
)
```

- **Materialized Views**:  
  Precompute expensive joins/aggregations:
  ```sql
  CREATE MATERIALIZED VIEW sales_summary AS
  SELECT region, SUM(order_amount) AS total_sales
  FROM sales
  GROUP BY region;
  ```
  ```python
  # Create a materialized view using SQL in Snowpark
  session.sql("""
      CREATE MATERIALIZED VIEW sales_summary AS
      SELECT region, SUM(order_amount) AS total_sales
      FROM sales
      GROUP BY region
  """).collect()
  ```
## Materialized Views vs. Traditional Views

| Feature                 | Traditional View (Regular View)          | Materialized View                      |
|-------------------------|------------------------------------------|---------------------------------------|
| **What It Is** | A saved SQL query (like a recipe).       | A saved SQL query and its result (like a pre-baked cake). |
| **Speed** | Slow if the query is complex (re-runs every time). | Super fast (uses precomputed results). |
| **Storage** | No extra storage needed.                 | Uses storage (saves the result).        |
| **Updates** | Always shows latest data.                | Might be **outdated** until refreshed.     |

- **Micro-partitions**: Snowflake automatically partitions data for optimal pruning. Monitor with `SYSTEM$CLUSTERING_INFORMATION`.

#### **B. Query Optimization**
- **Use Result Caching**:  
  Snowflake caches query results for 24 hours. Force reuse with `RESULT_SCAN`:
  ```sql
  SET query_tag = 'cached_query';
  SELECT * FROM large_table WHERE date = '2023-01-01';
  -- Later:
  SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
  ```

- **Avoid SELECT**: Query only required columns to reduce I/O.  
- **Filter Early**: Push filters (`WHERE` clauses) upstream to reduce data processed.  
- **Join Optimization**:  
  - Join small tables first.  
  - Use `BROADCAST` hints for small dimension tables:
    ```sql
    SELECT /*+ BROADCAST(dimension_table) */ ...
    ```
    ## BROADCAST: Real-Time Example

    **Scenario:**
    
    You're running a lemonade stand chain.
    
    * **Big Table**: `Sales` (millions of rows, like daily sales per store).
    * **Small Table**: `Stores` (hundreds of rows, like store addresses and managers).
    
    **Problem:**
    
    Joining `Sales` and `Stores` is slow because Snowflake has to shuffle the huge `Sales` table across nodes.
    
    **Solution:**
    
    Use `BROADCAST` to copy the small `Stores` table to every node. Now, each node can join its local `Sales` data with the copied `Stores` data instantly!

    ```python
    -- Tell Snowflake to BROADCAST the small "Stores" table
    SELECT 
      /*+ BROADCAST(Stores) */ 
      Sales.store_id, 
      Stores.manager_name, 
      SUM(Sales.amount) AS total_sales
    FROM 
      Sales
    JOIN 
      Stores ON Sales.store_id = Stores.store_id
    GROUP BY 
      Sales.store_id, Stores.manager_name;
    ```
    ## Key Notes

    **1. Use BROADCAST When:**
    
    * One table is **small** (e.g., less than 100MB).
    * The other table is **huge** (e.g., millions of rows).
    
    **2. Don't Use BROADCAST When:**
    
    * Both tables are big (copying both wastes memory).
    * The small table is **not really small** (it'll crash your system!).
    
    **3. Snowflake Magic:**
    
    Snowflake usually auto-optimizes joins, but `/*+ BROADCAST(table_alias) */` gives it a hint to speed things up even more.
#### **C. Warehouse Management**
- **Auto-Suspend/Resume**: Save costs by auto-suspending idle warehouses:
  ```sql
  CREATE WAREHOUSE reporting_wh 
  WITH WAREHOUSE_SIZE = 'MEDIUM'
  AUTO_SUSPEND = 60; -- Suspend after 60 seconds of inactivity
  ```

- **Warehouse Scaling**: Use **multi-cluster warehouses** for unpredictable concurrency:
  ```sql
  CREATE WAREHOUSE elastic_wh
  WAREHOUSE_SIZE = 'XSMALL'
  MAX_CLUSTER_COUNT = 5;
  ```

#### **D. Monitoring & Profiling**
- **QUERY_HISTORY**: Analyze query performance:
  ```sql
  SELECT query_text, execution_time, bytes_scanned
  FROM TABLE(information_schema.query_history())
  WHERE execution_time > 1000; -- Filter slow queries
  ```

- **EXPLAIN Command**: Understand query execution plans:
  ```sql
  EXPLAIN USING TABULAR
  SELECT * FROM large_table WHERE date = '2023-01-01';
  ```

---

### **4. Security & Governance**
- **Role-Based Access Control (RBAC)**:
  ```sql
  CREATE ROLE analyst;
  GRANT SELECT ON TABLE sales_data TO ROLE analyst;
  GRANT ROLE analyst TO USER john_doe;
  ```

- **Data Encryption**:  
  All data is encrypted at rest (AES-256) and in transit (TLS).  
- **Masking Policies**: Dynamically mask sensitive data:
  ```sql
  CREATE OR REPLACE MASKING POLICY email_mask AS (val STRING) 
  RETURNS STRING ->
  CASE WHEN CURRENT_ROLE() IN ('ADMIN') THEN val
       ELSE '***'
  END;
  ```

- **Tagging & Classification**:  
  Tag columns for governance (e.g., PII, PHI):
  ```sql
  ALTER TABLE customers ADD TAG pii_tag;
  ```

---

### **5. Integration with Ecosystem**
- **BI Tools**: Connect Tableau, Power BI, or Looker via Snowflake ODBC/JDBC drivers.  
- **Data Lake Integration**: Query external data (Parquet/CSV/JSON) in cloud storage:
  ```sql
  CREATE EXTERNAL TABLE logs_ext (
    log_data VARIANT
  ) LOCATION = @logs_stage/logs/
  FILE_FORMAT = parquet_format;
  ```
- **Snowflake + Snowpark**: Build pipelines in Python/Java/Scala directly in Snowflake (as discussed earlier).  
## Snowpark Sample Scripts with Schema Handling by Data Category

| Data Category   | File Type | Sub-types (Common Variations)         | Schema Usage                                       | Snowpark Sample Script with Schema Handling                                                                                                                                                                                                                            |
|-----------------|-----------|---------------------------------------|----------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Structured      | CSV       | Delimiter, Header, Data types         | Use Explicit Schema (Recommended)                   | ```python\n from snowflake.snowpark.types import StructType, StructField, StringType, IntegerType, FloatType\n\nschema_csv = StructType([StructField("id", IntegerType()), StructField("name", StringType()), StructField("price", FloatType())])\ndf_csv_explicit_schema = session.read.options({"header": True, "delimiter": ","}).schema(schema_csv).csv("@my_stage/data.csv")\n``` |
|                 |           |                                       | Consider Inference for Quick Exploration           | ```python\n df_csv_infer_schema = session.read.options({"header": True, "delimiter": ",", "inferSchema": True}).csv("@my_stage/data.csv")\n```                                                                                                                    |
|                 | TSV       | Header, Data types                    | Use Explicit Schema (Recommended)                   | ```python\n from snowflake.snowpark.types import StructType, StructField, StringType, DateType\n\nschema_tsv = StructType([StructField("product", StringType()), StructField("sale_date", DateType())])\ndf_tsv_explicit_schema = session.read.options({"header": True, "delimiter": "\\t"}).schema(schema_tsv).csv("@my_stage/data.tsv")\n``` |
|                 |           |                                       | Consider Inference for Quick Exploration           | ```python\n df_tsv_infer_schema = session.read.options({"header": True, "delimiter": "\\t", "inferSchema": True}).csv("@my_stage/data.tsv")\n```                                                                                                                    |
|                 | Parquet   |                                       | Schema is Usually Embedded (No need to explicitly define) | ```python\n df_parquet = session.read.parquet("@my_stage/data.parquet")\n# Parquet files typically contain schema information within the file itself.\n```                                                                                                        |
|                 | Avro      |                                       | Schema is Usually Embedded (No need to explicitly define) | ```python\n df_avro = session.read.avro("@my_stage/data.avro")\n# Avro files also contain schema information.\n```                                                                                                                                           |
|                 | ORC       |                                       | Schema is Usually Embedded (No need to explicitly define) | ```python\n df_orc = session.read.orc("@my_stage/data.orc")\n# ORC files also contain schema information.\n```                                                                                                                                             |
| Semi-structured | JSON      | Simple structures                     | Often Rely on Schema Inference Initially           | ```python\n df_json_infer = session.read.json("@my_stage/simple_data.json")\n```                                                                                                                                                                                   |
|                 |           | Complex or Nested structures          | Consider Explicit Schema for Specific Use Cases or Transformations | ```python\n from snowflake.snowpark.types import StructType, StructField, StringType, IntegerType, ArrayType, ObjectType\n\nschema_json_complex = StructType([StructField("name", StringType()), StructField("details", ObjectType({"age": IntegerType(), "hobbies": ArrayType(StringType())}))])\ndf_json_explicit_schema = session.read.schema(schema_json_complex).json("@my_stage/complex_data.json")\n``` |
|                 | XML       | (Read as text, schema not directly applicable at read time) | Schema is Defined During Parsing                     | ```python\n df_xml_raw = session.read.text("@my_stage/data.xml")\ndf_xml_parsed = df_xml_raw.select(sf.parse_xml(sf.col("value")).alias("xml_data"))\n# The 'schema' here is defined by how you extract data using XML functions.\ndf_xml_extracted = df_xml_parsed.select(sf.xml_extract_path_text(sf.col("xml_data"), 'path/to/element').alias('element_value'))\n``` |
---
```python
from snowflake.snowpark.types import StructType, StructField, StringType, IntegerType, FloatType
schema_csv = StructType([StructField("id", IntegerType()), StructField("name", StringType()), StructField("price", FloatType())])
df_csv_explicit_schema = session.read.options({"header": True, "delimiter": ","}).schema(schema_csv).csv("@my_stage/data.csv")
df_csv_infer_schema = session.read.options({"header": True, "delimiter": ",", "inferSchema": True}).csv("@my_stage/data.csv")

from snowflake.snowpark.types import StructType, StructField, StringType, DateType
schema_tsv = StructType([StructField("product", StringType()), StructField("sale_date", DateType())])
df_tsv_explicit_schema = session.read.options({"header": True, "delimiter": "\\t"}).schema(schema_tsv).csv("@my_stage/data.tsv")

df_tsv_infer_schema = session.read.options({"header": True, "delimiter": "\\t", "inferSchema": True}).csv("@my_stage/data.tsv")
df_parquet = session.read.parquet("@my_stage/data.parquet")
# Parquet files typically contain schema information within the file itself.
df_avro = session.read.avro("@my_stage/data.avro")
# Avro files also contain schema information.
df_orc = session.read.orc("@my_stage/data.orc")
# ORC files also contain schema information.
df_json_infer = session.read.json("@my_stage/simple_data.json")

from snowflake.snowpark.types import StructType, StructField, StringType, IntegerType, ArrayType, ObjectType

schema_json_complex = StructType([StructField("name", StringType()), StructField("details", ObjectType({"age": IntegerType(), "hobbies": ArrayType(StringType())}))])
df_json_explicit_schema = session.read.schema(schema_json_complex).json("@my_stage/complex_data.json")
df_xml_raw = session.read.text("@my_stage/data.xml")
df_xml_parsed = df_xml_raw.select(sf.parse_xml(sf.col("value")).alias("xml_data"))
# The 'schema' here is defined by how you extract data using XML functions.
df_xml_extracted = df_xml_parsed.select(sf.xml_extract_path_text(sf.col("xml_data"), 'path/to/element').alias('element_value'))
```
---

### **6. Example: Optimized ELT Pipeline**
```sql
-- Stage raw data from S3
CREATE STAGE raw_data_stage URL='s3://my-bucket/data/'
CREDENTIALS=(AWS_KEY_ID='...' AWS_SECRET_KEY='...');

-- Load into a Snowflake table
COPY INTO raw_sales FROM @raw_data_stage/sales/
FILE_FORMAT = (TYPE = CSV);

-- Aggregate with clustering
CREATE OR REPLACE TABLE sales_summary CLUSTER BY (region, sale_date) AS
SELECT region, sale_date, SUM(amount) AS total_sales
FROM raw_sales
GROUP BY region, sale_date;

-- Schedule refresh with a task
CREATE TASK refresh_summary
  WAREHOUSE = reporting_wh
  SCHEDULE = 'USING CRON 0 8 * * * UTC'
AS
  INSERT INTO sales_summary SELECT ...;
```

---

### **7. Best Practices**
1. **Right-Size Warehouses**: Choose warehouse sizes based on workload (e.g., `XSMALL` for dashboards, `XLARGE` for ETL).  
2. **Use Time Travel**: Recover from accidental deletes/updates without backups.  
3. **Leverage Caching**: Reuse query results where possible.  
4. **Monitor Costs**: Track credit usage via `WAREHOUSE_METERING_HISTORY`.  
5. **Partition Large Datasets**: Use clustering or partition by date for time-based queries.  

---

### **8. Troubleshooting Tips**
- **Slow Queries?**  
  - Check `bytes_scanned` in `QUERY_HISTORY` (optimize filters).  
  - Add clustering keys or use materialized views.  
- **High Credit Usage?**  
  - Downscale warehouses or limit `MAX_CLUSTER_COUNT`.  
  - Use `AUTO_SUSPEND` for development warehouses.  
- **Data Skew?**  
  - Redistribute tables with `CLUSTER BY` or `REPARTITION`.  

---

By leveraging Snowflake’s architecture, optimized querying, and governance features, you can build scalable, high-performance data solutions. Pair it with Snowpark for advanced analytics and automation workflows.
