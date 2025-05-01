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

from snowflake.snowpark.functions import udf
from snowflake.snowpark.types import IntegerType
import pandas as pd

@udf(return_type=IntegerType(), input_types=[IntegerType(), IntegerType()], vectorized=True)
def vector_add(series1: pd.Series, series2: pd.Series) -> pd.Series:
    """Adds two Pandas Series element-wise."""
    return series1 + series2

# Assuming you have a DataFrame 'df' with columns 'col1' and 'col2'
df = session.create_dataframe([[1, 2], [3, 4], [5, 6]], columns=["col1", "col2"])

df = df.with_column("sum_vectorized", vector_add(df["col1"], df["col2"]))
df.show()
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

```python
from snowflake.snowpark.types import IntegerType, StringType
from snowflake.snowpark.functions import udf, col
import pandas as pd

session_new = session # Assuming you have an active Snowpark session

@udf(session=session_new, name='a_plus_b_scalar', input_types=[IntegerType(), IntegerType()], return_type=IntegerType(), stage_location='@udf_stage', is_permanent=True, replace=True)
def a_plus_b_scalar(a: int, b: int) -> int:
    return a + b

# Now in SQL, you can call the scalar UDF
query = """
SELECT C_CURRENT_HDEMO_SK, A_PLUS_B_SCALAR(C_CURRENT_HDEMO_SK, 3)
FROM DEMO_DB.PUBLIC.CUSTOMER_TEST
LIMIT 10;
"""
session.sql(query).show()

from snowflake.snowpark.types import IntegerType, StringType
from snowflake.snowpark.functions import udf, col
import pandas as pd

session_new = session # Assuming you have an active Snowpark session

@udf(session=session_new, name='a_plus_b_vectorized', input_types=[IntegerType(), IntegerType()], return_type=IntegerType(), stage_location='@udf_stage', is_permanent=True, replace=True, vectorized=True)
def a_plus_b_vectorized(series_a: pd.Series, series_b: pd.Series) -> pd.Series:
    return series_a + series_b

customer_test_df = session.table("DEMO_DB.PUBLIC.CUSTOMER_TEST")
result_df = customer_test_df.select(
    col("C_CURRENT_HDEMO_SK"),
    a_plus_b_vectorized(col("C_CURRENT_HDEMO_SK"), col(lit(3)))
)
result_df.limit(10).show()

from snowflake.snowpark.functions import udf, col, lit
from snowflake.snowpark.types import IntegerType
import pandas as pd

@udf(name='vector_add_func', input_types=[IntegerType(), IntegerType()], return_type=IntegerType(), vectorized=True)
def vector_add(series1: pd.Series, series2: pd.Series) -> pd.Series:
    return series1 + series2

# Assuming you have a Snowpark session 'session' and a table 'my_table'
df = session.table("my_table")

# "Querying" and applying the vectorized UDF using the DataFrame API
result_df = df.select(
    col("column_a"),
    col("column_b"),
    vector_add("column_a", "column_b").alias("sum_vectorized")
)

result_df.show()

# You can further filter, order, etc., using DataFrame operations
filtered_df = result_df.filter(col("sum_vectorized") > 10)
filtered_df.show()

# If you need to execute a SQL query that *uses* the result of the vectorized UDF,
# you might create a temporary view or table from the DataFrame:
result_df.create_temp_view("vectorized_results")
sql_query = "SELECT * FROM vectorized_results WHERE sum_vectorized < 20"
session.sql(sql_query).show()
```
 - Regular scalar UDFs can be called directly in standard SQL queries.
 - Vectorized UDFs (defined with vectorized=True) are designed to be used within the Snowpark DataFrame API, where the batching and execution are managed by the Snowpark runtime for optimized performance. You cannot directly call them in the same way in a pure SQL query.

#### **User-Defined Table Functions - UDTF**
```python
schema = StructType([
     StructField("symbol", StringType()),
     StructField("cost", StringType())
 ])

@udtf(name="process_stock_price", is_permanent=True, stage_location="@demo_stage", replace=True, packages=["snowflake-snowpark-python","pandas"],session=session_new,input_types=[StringType(),IntegerType(),IntegerType()],output_schema=schema)
class StockSale:
    def process(self, symbol, quantity, price):
         cost = quantity * price
         yield (symbol, cost)

```
- yield is a way for a function to be a "generator" – to produce a series of results in a more controlled and memory-friendly way, especially when you might have many results to produce.

```python
import json
from snowflake.snowpark.files import SnowflakeFile
from snowflake.snowpark.functions import sproc,udtf,col
import pandas as pd
import snowflake.snowpark as snowpark
from snowflake.snowpark.types  import PandasDataFrameType,MapType,PandasSeriesType,StringType,StructType,StructField,IntegerType,BooleanType

schema = StructType([
     StructField("soloistRoles", StringType()),
     StructField("soloistInstrument", StringType()),
     StructField("id", StringType()),
     StructField("soloist_FirstName", StringType()),
     StructField("soloist_LastName", StringType()),
 ])

@udtf(name="parse_json_sp_local_udtf", is_permanent=True, stage_location="@demo_stage", replace=True, packages=["snowflake-snowpark-python","pandas"],session=session_new,input_types=[StringType()],output_schema=schema)
class StockSale:
    def process(self,file_path):
        with SnowflakeFile.open(file_path) as f:
            # Read json file and normalize data.
            nycphil = json.load(f)
            works_data = pd.json_normalize(data=nycphil['programs'], record_path='works', meta=['id', 'orchestra','programID','season'])
            # Drop columns which is not required.
            works_data = works_data.drop(['soloists','movement.em','movement._','workTitle._','workTitle.em'], axis=1)

            
            # Split conductor name as first-name and last-name
            works_data[['composer_FirstName', 'composer_LastName']] = works_data['composerName'].loc[works_data['composerName'].str.split().str.len() == 2].str.split(expand=True)
            works_data = works_data.drop(['composerName'], axis=1)
            
            # Create another data frame with name  soloist_df               
            soloist_df = pd.json_normalize(data=nycphil['programs'], record_path=['works', 'soloists'], 
                                    meta=['id'])
                                    
            soloist_df[['soloist_FirstName', 'soloist_LastName']] = soloist_df['soloistName'].loc[soloist_df['soloistName'].str.split().str.len() == 2].str.split(expand=True)
            soloist_df = soloist_df.drop(['soloistName'], axis=1)
        
            #session_new.write_pandas(soloist_df, "soloist_data_udf", auto_create_table=True,overwrite=True)
            #session_new.write_pandas(works_data, "programs_data_udf", auto_create_table=True,overwrite=True)

            for _, row in soloist_df.iterrows():
                yield  (row['soloistRoles'],row['soloistInstrument'],row['id'],row['soloist_FirstName'],
                row['soloist_LastName'])

```

```sql
SELECT * FROM TABLE(DEMO_DB.PUBLIC.PARSE_JSON_SP_LOCAL_UDTF(build_scoped_file_url('@demo_stage','raw_nyc_phil.json'))) 
```

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

## **SQL:** Writing efficient queries for data manipulation and analysis.
Here’s a comprehensive guide to writing **efficient SQL queries** for data manipulation and analysis, with best practices, optimization tips, and examples.

---

### **1. Query Structure Basics**
#### **SELECT Statements**
- Use explicit column names instead of `SELECT *`:
  ```sql
  -- Avoid
  SELECT * FROM employees;

  -- Prefer
  SELECT id, name, department FROM employees;
  ```
  ```python
  # ❌ Avoid: Fetches ALL columns (not efficient or explicit)
  df = session.table("employees")
  df.show()


  from snowflake.snowpark.functions import col

  # ✅ Explicitly select only required columns
  df = session.table("employees").select(
    col("id"),
    col("name"),
    col("department")
  )
  df.show()

  # ✅ Also valid for simple column names
  df = session.table("employees").select("id", "name", "department")
  df.show()
  ```

- Filter early with `WHERE` to reduce data processed:
  ```sql
  SELECT * FROM sales 
  WHERE sale_date >= '2023-01-01';
  ```
  ```python
  # Filter early to reduce data
  df = session.table("sales").filter(col("sale_date") >= "2023-01-01")
  df.show()
  ```

- Use `LIMIT` to test queries on small datasets:
  ```sql
  SELECT * FROM large_table LIMIT 100;
  ```
  ```python
  # Test on small data
  df = session.table("large_table").limit(100)
  df.show()
  ```

---

### **2. Efficient JOINs**
- **Prefer INNER JOIN** unless you need unmatched rows:
  ```sql
  -- Get orders with customer info
  SELECT o.order_id, c.name
  FROM orders o
  INNER JOIN customers c ON o.customer_id = c.id;
  ```
  ```python
  # Join orders with customers
  orders = session.table("orders")
  customers = session.table("customers")

  joined_df = orders.join(
    customers,
    orders["customer_id"] == customers["id"],
    how="inner"
  ).select(orders["order_id"], customers["name"])
  joined_df.show()
  ```

- **Avoid Cartesian Products** (unintended cross joins):
  ```sql
  -- Bad: No JOIN condition!
  SELECT * FROM orders, customers;

  -- Good: Always specify ON or USING
  SELECT * FROM orders o JOIN customers c ON o.customer_id = c.id;
  ```
  ```python
  # Always specify join condition
  joined_df = orders.join(
    customers,
    orders["customer_id"] == customers["id"],
    how="inner"
  )
  ```

- **Use LEFT JOIN for Optional Relationships**:
  ```sql
  -- Get all customers, even those without orders
  SELECT c.name, o.order_id
  FROM customers c
  LEFT JOIN orders o ON c.id = o.customer_id;
  ```
  ```python
  # Get all customers, even without orders
  joined_df = customers.join(
    orders,
    customers["id"] == orders["customer_id"],
    how="left"
  ).select(customers["name"], orders["order_id"])
  joined_df.show()
  ```

---

### **3. Aggregation & Filtering**
- Use `GROUP BY` for summaries and `HAVING` for post-aggregation filtering:
  ```sql
  -- Total sales per region, filtering regions with > $1M
  SELECT region, SUM(amount) AS total_sales
  FROM sales
  GROUP BY region
  HAVING SUM(amount) > 1000000;
  ```
  ```python
  from snowflake.snowpark.functions import sum as snow_sum

  # Total sales per region with HAVING
  sales_df = session.table("sales")

  agg_df = sales_df.group_by("region").agg(
    snow_sum("amount").alias("total_sales")
  ).filter(snow_sum("amount") > 1_000_000)

  agg_df.show()
  ```

- Avoid unnecessary columns in `GROUP BY`:
  ```sql
  -- Bad: Extra column forces grouping
  SELECT region, product_id, SUM(amount)
  FROM sales
  GROUP BY region, product_id;

  -- Better: Only group by necessary columns
  SELECT region, SUM(amount)
  FROM sales
  GROUP BY region;
  ```
  ```python
  # Only group by necessary columns
  agg_df = sales_df.group_by("region").agg(
    snow_sum("amount").alias("total_sales")
  )
  ```

---

### **4. Subqueries vs. Common Table Expressions (CTEs)**
- **Subqueries** (use for simple nested logic):
  ```sql
  -- Find employees earning more than average
  SELECT name, salary
  FROM employees
  WHERE salary > (SELECT AVG(salary) FROM employees);
  ```
  ```python
  # Find employees earning more than average
  avg_salary = (session.table("employees")
              .select(snow_avg("salary")).first()[0])

  high_earners = session.table("employees").filter(
    col("salary") > avg_salary
  ).select("name", "salary")
  ```

- **CTEs** (use for readability and reuse):
  ```sql
  WITH AvgSalary AS (
    SELECT AVG(salary) AS avg_salary FROM employees
  )
  SELECT e.name, e.salary
  FROM employees e, AvgSalary a
  WHERE e.salary > a.avg_salary;
  ```
  ```python
  from snowflake.snowpark.functions import avg as snow_avg

  # With CTE
  avg_salary_cte = (session.table("employees")
                  .select(snow_avg("salary").alias("avg_salary")))

  high_earners = (session.table("employees")
                .join(avg_salary_cte, how="cross")
                .filter(col("salary") > col("avg_salary"))
                .select(col("name"), col("salary")))
  ```

---

### **5. Indexing for Speed**
- **Create indexes on**:
  - Primary/foreign keys.
  - Columns used in `WHERE`, `JOIN`, or `ORDER BY`.
  ```sql
  CREATE INDEX idx_customer_id ON orders(customer_id);
  ```

- **Avoid over-indexing**: Too many indexes slow down `INSERT`/`UPDATE`.
- **Use composite indexes** for multi-column filters:
  ```sql
  CREATE INDEX idx_region_date ON sales(region, sale_date);
  ```

---

### **6. Optimization Techniques**
- **Use EXISTS Instead of IN** for better performance:
  ```sql
  -- Prefer EXISTS
  SELECT name FROM customers c
  WHERE EXISTS (
    SELECT 1 FROM orders o WHERE o.customer_id = c.id
  );

  -- Avoid IN with subqueries
  SELECT name FROM customers
  WHERE id IN (SELECT customer_id FROM orders);
  ```
  ```python
  # Use exists() for better performance
  orders_subquery = (session.table("orders")
                   .select("customer_id").distinct())

  customers_with_orders = (session.table("customers")
                         .join(orders_subquery, 
                               on=col("id") == col("customer_id"),
                               how="inner")
                         .select("name"))
  ```

- **Avoid Functions on Indexed Columns**:
  ```sql
  -- Bad: Prevents index use
  SELECT * FROM sales WHERE DATE(sale_date) = '2023-01-01';

  -- Good: Sargable condition
  SELECT * FROM sales 
  WHERE sale_date >= '2023-01-01' AND sale_date < '2023-01-02';
  ```
  ```python
  # Sargable condition
  filtered_df = session.table("sales").filter(
    (col("sale_date") >= "2023-01-01") &
    (col("sale_date") < "2023-01-02")
  )
  ```

- **Use UNION ALL Instead of UNION** when duplicates aren’t an issue:
  ```sql
  -- UNION ALL skips duplicate checks
  SELECT name FROM employees
  UNION ALL
  SELECT name FROM contractors;
  ```
  ```python
  # Union without deduplication
  combined_df = (session.table("employees")
               .select("name")
               .union_all(session.table("contractors").select("name")))
  ```

---

### **7. Execution Plan Analysis**
- Use `EXPLAIN` or `EXPLAIN ANALYZE` to debug performance:
  ```sql
  EXPLAIN ANALYZE
  SELECT * FROM sales WHERE region = 'APAC';
  ```

- Look for:
  - Full table scans (bad).
  - Index usage (good).
  - High-cost operations like sorts or hashes.

---

### **8. Data Manipulation (DML)**
- **INSERT Efficiently**:
  ```sql
  -- Bulk insert
  INSERT INTO logs (user_id, action)
  SELECT user_id, 'login' FROM active_users;
  ```
  ```python
  # Use SQL for DML
  session.sql("""
    INSERT INTO logs (user_id, action)
    SELECT user_id, 'login' FROM active_users
  """).collect()
  ```

- **UPDATE with Care**:
  ```sql
  -- Always use WHERE to avoid full-table updates
  UPDATE employees
  SET salary = salary * 1.1
  WHERE department = 'Engineering';
  ```
  ```python
  session.sql("""
    UPDATE employees
    SET salary = salary * 1.1
    WHERE department = 'Engineering'
  """).collect()
  ```

- **DELETE vs. TRUNCATE**:
  ```sql
  TRUNCATE TABLE temp_data; -- Faster than DELETE for full tables
  ```
  ```python
  session.sql("TRUNCATE TABLE temp_data").collect()
  ```

---

### **9. Advanced Analysis with Window Functions**
- **Ranking**:
  ```sql
  -- Top 3 earners per department
  SELECT name, department, salary
  FROM (
    SELECT *, 
      RANK() OVER (PARTITION BY department ORDER BY salary DESC) as rk
    FROM employees
  ) ranked
  WHERE rk <= 3;
  ```
  ```python
  from snowflake.snowpark.window import Window
  from snowflake.snowpark.functions import rank

  # Rank employees by salary
  window_spec = Window.partition_by("department").order_by(col("salary").desc())

  ranked_df = (session.table("employees")
             .select(
                 col("name"),
                 col("department"),
                 col("salary"),
                 rank().over(window_spec).alias("rk")
             ).filter(col("rk") <= 3))
  ```

- **Running Totals**:
  ```sql
  -- Cumulative sales by region
  SELECT sale_date, region, amount,
    SUM(amount) OVER (PARTITION BY region ORDER BY sale_date) AS running_total
  FROM sales;
  ```
  ```python
  # Cumulative sales by region
  window_spec = (Window.partition_by("region")
               .order_by("sale_date")
               .rows_between(Window.unboundedPreceding, Window.currentRow))

  running_total_df = (session.table("sales")
                    .select(
                        col("sale_date"),
                        col("region"),
                        col("amount"),
                        snow_sum("amount").over(window_spec).alias("running_total")
                    ))
  ```

- **Time Series Gaps & Islands**:
  ```sql
  -- Find consecutive login dates
  WITH Logins AS (
    SELECT user_id, login_date,
      DATE_SUB(login_date, ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY login_date)) AS grp
    FROM user_logins
  )
  SELECT user_id, MIN(login_date), MAX(login_date)
  FROM Logins
  GROUP BY user_id, grp;
  ```
  

---

### **10. Best Practices**
1. **Format for Readability**:
   ```sql
   SELECT 
     o.order_id,
     c.name AS customer_name,
     SUM(oi.quantity * oi.price) AS total_amount
   FROM orders o
   JOIN customers c ON o.customer_id = c.id
   JOIN order_items oi ON o.order_id = oi.order_id
   GROUP BY o.order_id, c.name;
   ```
   ```python
   # Chain operations for clarity
   result_df = (session.table("orders")
             .join(session.table("customers"), 
                   on=col("customer_id") == col("id"), 
                   how="inner")
             .join(session.table("order_items"), 
                   on=col("order_id") == col("order_id"), 
                   how="inner")
             .group_by(col("order_id"), col("name"))
             .agg(snow_sum(col("quantity") * col("price")).alias("total_amount"))))
   ```

2. **Parameterize Queries** to prevent SQL injection:
   ```sql
   -- Use placeholders (language-specific)
   SELECT * FROM users WHERE id = %s; -- Python example
   ```
   
   ```python
   # Use Python f-strings or bind variables
   user_id = 123
   session.sql(f"SELECT * FROM users WHERE id = {user_id}").collect()
   ```

3. **Version Control** your SQL scripts (e.g., Git).

4. **Test on Small Data** before scaling.

5. **Leverage Database-Specific Features**:
   - **Snowflake**: Use `RESULT_SCAN`, clustering keys, and `STAGE` for external data.
   - **PostgreSQL**: Use `EXPLAIN ANALYZE`, `JSONB`, and `MATERIALIZED VIEWS`.
   - **BigQuery**: Use partitioned tables and `ARRAY_AGG`.

---

### **11. Common Pitfalls to Avoid**
- **N+1 Queries**: Fetching data row-by-row instead of joining.
- **Overusing DISTINCT**: Often a sign of incorrect JOINs.
- **Ignoring NULLs**: Use `COALESCE` or `IFNULL` where needed.
- **Unbounded Window Frames**: Specify `ROWS BETWEEN` for performance.

---

### **12. Example: Optimized Analysis Workflow**
```sql
-- Step 1: Identify high-value customers
WITH HighValueCustomers AS (
  SELECT customer_id
  FROM orders
  GROUP BY customer_id
  HAVING SUM(amount) > 10000
)

-- Step 2: Get their recent purchases
SELECT c.name, o.order_id, o.amount
FROM customers c
JOIN orders o ON c.id = o.customer_id
WHERE c.id IN (SELECT customer_id FROM HighValueCustomers)
  AND o.order_date >= '2023-01-01'
ORDER BY o.amount DESC
LIMIT 100;
```
```python
from snowflake.snowpark.functions import col, sum as snow_sum

# Load orders table
orders_df = session.table("orders")

# Step 1: Find customers with total orders > $10,000
high_value_customers = (
    orders_df.group_by("customer_id")
    .agg(snow_sum("amount").alias("total_amount"))
    .filter(col("total_amount") > 10000)
    .select("customer_id")
)

# Load customers table
customers_df = session.table("customers")

# Join customers with orders and filter for high-value customers
recent_purchases = (
    customers_df.alias("c")
    .join(
        orders_df.alias("o"),
        col("c.id") == col("o.customer_id"),
        how="inner"
    )
    .filter(
        col("o.order_date") >= "2023-01-01",
        col("c.id").isin(high_value_customers.select("customer_id"))
    )
    .select(
        col("c.name").alias("customer_name"),
        col("o.order_id"),
        col("o.amount")
    )
    .order_by(col("o.amount").desc())
    .limit(100)
)

# Show the result
recent_purchases.show()
```
---

By applying these techniques, you’ll write **faster, cleaner, and more maintainable SQL** for both transactional and analytical workloads. Always combine query optimization with proper indexing and schema design for maximum efficiency.

Here’s a structured guide to **Python for Data Engineering**, covering key libraries, ETL pipelines, automation, error handling, and performance optimization with real-world examples and best practices.

---

### **1. Key Libraries for Data Engineering**
#### **Pandas**
For tabular data manipulation:
```python
import pandas as pd

# Load and clean data
df = pd.read_csv("data.csv")
df.dropna(inplace=True)
df["sales"] = df["quantity"] * df["price"]
```

#### **NumPy**
For numerical operations and vectorization:
```python
import numpy as np

# Efficient array operations
arr = np.array([1, 2, 3])
squared = arr ** 2  # Vectorized computation
```

#### **PySpark**
For distributed processing of large datasets:
```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ETL").getOrCreate()
df = spark.read.parquet("s3://data/transactions/")
df.filter(df.amount > 100).write.parquet("cleaned_data/")
```

#### **SQLAlchemy**
For database abstraction:
```python
from sqlalchemy import create_engine

engine = create_engine("postgresql://user:password@localhost/db")
df.to_sql("table_name", engine, if_exists="replace", index=False)
```

#### **Snowflake Connector**
For Snowflake integration:
```python
import snowflake.connector

conn = snowflake.connector.connect(
    user='user',
    password='password',
    account='account',
    warehouse='compute_wh'
)
cur = conn.cursor()
cur.execute("SELECT * FROM sales LIMIT 100")
results = cur.fetchall()
```

---

### **2. Building ETL Pipelines**
#### **Step 1: Extract**
- **From Files**:
  ```python
  df = pd.read_json("data.json")  # JSON/CSV/Parquet
  ```
- **From Databases**:
  ```python
  engine = create_engine("mysql://user:pass@host/db")
  df = pd.read_sql("SELECT * FROM orders", engine)
  ```

#### **Step 2: Transform**
- **Data Cleaning**:
  ```python
  df["date"] = pd.to_datetime(df["date"])
  df.drop_duplicates(subset=["order_id"], inplace=True)
  ```
- **Aggregation**:
  ```python
  summary = df.groupby("region").agg({"sales": "sum"}).reset_index()
  ```

#### **Step 3: Load**
- **To Snowflake**:
  ```python
  from snowflake.connector.pandas_tools import write_pandas

  write_pandas(conn, df, "TARGET_TABLE", auto_create_table=True)
  ```
- **To Data Lakes**:
  ```python
  df.to_parquet("s3://bucket/output/data.parquet")
  ```

#### **Example Pipeline**:
```python
def etl_pipeline():
    # Extract
    raw = pd.read_csv("raw_data.csv")
    # Transform
    cleaned = raw.dropna().assign(total=lambda x: x.qty * x.price)
    # Load
    cleaned.to_sql("processed_sales", engine, if_exists="append")

etl_pipeline()
```

---

### **3. Automation with Python Scripts**
#### **Data Cleaning Script**:
```python
import os
import pandas as pd

def clean_data(file_path):
    try:
        df = pd.read_csv(file_path)
        df.dropna(inplace=True)
        df.to_csv(f"cleaned_{os.path.basename(file_path)}", index=False)
    except Exception as e:
        print(f"Error: {e}")

clean_data("data.csv")
```

#### **Scheduling with Cron**:
```bash
# Run daily at 2 AM
0 2 * * * /usr/bin/python3 /path/to/etl_script.py
```

#### **Airflow DAG**:
```python
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

def run_etl():
    from etl_script import etl_pipeline
    etl_pipeline()

dag = DAG("daily_etl", schedule_interval="@daily", start_date=datetime(2023, 1, 1))

task = PythonOperator(task_id="run_etl", python_callable=run_etl, dag=dag)
```

---

### **4. Error Handling and Logging**
#### **Try-Except Blocks**:
```python
try:
    df = pd.read_csv("missing_file.csv")
except FileNotFoundError:
    print("Input file not found. Check path.")
```

#### **Logging**:
```python
import logging

logging.basicConfig(filename="etl.log", level=logging.INFO)
try:
    logging.info("Starting ETL...")
    # Your code
except Exception as e:
    logging.error(f"Error: {str(e)}")
```

#### **Retry Logic**:
```python
from tenacity import retry, stop_after_attempt

@retry(stop=stop_after_attempt(3))
def fetch_data():
    # Retry up to 3 times if fails
    response = requests.get("https://api.example.com/data")
    response.raise_for_status()
    return response.json()
```

---

### **5. Performance Optimization**
#### **Memory Management**
- Use appropriate data types:
  ```python
  df["category"] = df["category"].astype("category")  # Reduces memory usage
  ```
 -  `df["category"].astype("category")` is a way to make your computer use less memory when you have columns with lots of repeated text (or other similar values) by turning those texts into secret codes! It's like being a super organized toy sorter who uses clever shortcuts to save space and time.


#### **Vectorization**
- Replace loops with NumPy/Pandas vectorized ops:
  ```python
  # Inefficient
  df["discounted_price"] = [price * 0.9 for price in df["price"]]
  
  # Efficient
  df["discounted_price"] = df["price"] * 0.9
  ```

#### **Distributed Processing with PySpark**
- Process terabytes of data:
  ```python
  spark = SparkSession.builder \
      .appName("LargeData") \
      .config("spark.sql.shuffle.partitions", "8") \
      .getOrCreate()
  ```

#### **Batch Processing**
- Use chunking for large files:
  ```python
  for chunk in pd.read_csv("big_data.csv", chunksize=10000):
      process(chunk)  # Process in batches
  ```

#### **Best Practices**
- Use **Dask** for out-of-core computation:
  ```python
  import dask.dataframe as dd
  df = dd.read_csv("huge_data.csv")
  df.groupby("id").mean().compute()
  ```
- Cache intermediate results in Spark:
  ```python
  df.cache()
  ```

---

### **6. Real-World Integration: Snowflake & Python**
#### **ETL with Snowflake Connector**:
```python
def load_to_snowflake():
    conn = snowflake.connector.connect(
        user="user", password="password", account="account"
    )
    cur = conn.cursor()
    try:
        # Extract
        cur.execute("SELECT * FROM raw_data")
        data = cur.fetchall()
        df = pd.DataFrame(data, columns=[desc[0] for desc in cur.description])
        # Transform
        df["total"] = df["qty"] * df["price"]
        # Load
        write_pandas(conn, df, "PROCESSED_DATA")
    except Exception as e:
        logging.error(f"Snowflake Error: {e}")
    finally:
        conn.close()

load_to_snowflake()
```

---

### **7. Best Practices Summary**
| Area               | Practice                                                                 |
|--------------------|--------------------------------------------------------------------------|
| **Code Structure** | Modularize functions, use config files, version control (Git).          |
| **Security**       | Use environment variables for secrets (e.g., `os.getenv("API_KEY")`).   |
| **Testing**        | Validate data quality (e.g., `assert df.isnull().sum().sum() == 0`).     |
| **Monitoring**     | Log metrics (e.g., row counts, runtime) for debugging pipelines.         |

---

By combining these tools and techniques, you can build **robust, scalable data pipelines** tailored to your infrastructure (e.g., Snowflake, AWS, or local clusters). Always profile performance-critical sections using tools like `cProfile` or Spark’s UI for optimization.

---
Here’s a **complete Python script** using **Snowpark** to build an ETL pipeline that reads a CSV, cleans nulls, applies transformations (group-by, joins), and loads to a Snowflake table. This approach leverages **Snowpark’s DataFrame API** for scalability and performance.

---

### **1. Prerequisites**
#### Install Snowpark:
```bash
pip install snowflake-snowpark-python
```

#### Configure `connection.json`:
```json
{
  "account": "<your_account>",
  "user": "<your_user>",
  "password": "<your_password>",
  "role": "<your_role>",
  "warehouse": "<your_warehouse>",
  "database": "<your_database>",
  "schema": "<your_schema>"
}
```

---

### **2. Full Pipeline Script**
```python
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, when
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_session():
    """Create a Snowflake session."""
    try:
        session = Session.builder.configs({
            "connection_name": "my_connection",
            "account": "<your_account>",
            "user": "<your_user>",
            "password": "<your_password>",
            "role": "<your_role>",
            "warehouse": "<your_warehouse>",
            "database": "<your_database>",
            "schema": "<your_schema>"
        }).create()
        logger.info("Connected to Snowflake")
        return session
    except Exception as e:
        logger.error(f"Connection failed: {e}")
        raise

def load_csv_to_snowflake(session: Session):
    """Read CSV from Snowflake stage, clean, transform, and load."""
    try:
        # Step 1: Read CSV from Snowflake stage
        df_raw = (
            session.read
            .option("FIELD_DELIMITER", ",")
            .option("SKIP_HEADER", 1)
            .csv("@MY_STAGE/data/sales.csv")
        )
        df_raw = df_raw.to_df(["DATE", "REGION_ID", "SALES", "UNITS"])

        # df_raw.to_df(["DATE", ...]) is a method of the Snowpark DataFrame that renames its columns. The result of this operation is still a Snowpark DataFrame with         the updated column names.

        df_pandas = df_raw.to_pandas()
        print(type(df_pandas))  # Output: <class 'pandas.core.frame.DataFrame'>

        # Step 2: Clean null values
        df_cleaned = (
            df_raw
            .filter(col("SALES").is_not_null() & col("UNITS").is_not_null())
            .with_column("SALES", col("SALES").cast("float"))
            .with_column("UNITS", col("UNITS").cast("int"))
        )

        # Step 3: Join with region dimension table
        df_regions = session.table("REGIONS")
        df_joined = df_cleaned.join(df_regions, df_cleaned["REGION_ID"] == df_regions["ID"], join_type="inner")

        # Step 4: Group-by transformation
        df_summary = (
            df_joined
            .group_by("REGION_NAME")
            .agg(
                {"SALES": "sum", "UNITS": "sum"}
            )
            .with_column_renamed('"SUM(SALES)"', "TOTAL_SALES")
            .with_column_renamed('"SUM(UNITS)"', "TOTAL_UNITS")
        )

        # Step 5: Load to target table
        df_summary.write.save_as_table("SALES_SUMMARY", mode="overwrite")
        logger.info("Pipeline completed successfully")

    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        session.rollback()
        raise
    finally:
        session.close()

if __name__ == "__main__":
    session = create_session()
    load_csv_to_snowflake(session)
```

---

### **3. Key Components Explained**
#### **A. Reading CSV from Snowflake Stage**
- Assumes `sales.csv` is uploaded to `@MY_STAGE/data/`.
- Uses Snowpark’s `read.csv()` with options for delimiters and headers.

#### **B. Data Cleaning**
- Filters null values using `filter()` and `is_not_null()`.
- Casts columns to appropriate types (`float` for sales, `int` for units).

#### **C. Joining with Dimension Table**
- Joins with a `REGIONS` table to map `REGION_ID` to human-readable `REGION_NAME`.

#### **D. Aggregation**
- Groups by `REGION_NAME` and aggregates sales/units totals.

#### **E. Loading to Snowflake**
- Writes output to `SALES_SUMMARY` table using `save_as_table()`.

---

### **4. Required Snowflake Objects**
#### Create Regions Table:
```sql
CREATE TABLE REGIONS (
    ID INT PRIMARY KEY,
    REGION_NAME STRING
);
INSERT INTO REGIONS VALUES 
(1, 'North America'),
(2, 'Europe'),
(3, 'Asia-Pacific');
```

#### Upload CSV to Stage:
```sql
PUT file:///path/to/sales.csv @MY_STAGE/data/;
```

---

### **5. Automation Options**
#### **Option 1: Schedule with Snowflake Task**
```sql
CREATE TASK run_sales_pipeline
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = 'USING CRON 0 8 * * * UTC'
AS
  CALL SYSTEM$SEND_QUERY('EXECUTE SCRIPT ETL_SCRIPT;');
```

#### **Option 2: Run via Python Scheduler**
Use `cron` or Airflow to trigger the script:
```bash
# Example: Run daily at 7 AM
0 7 * * * /usr/bin/python3 /path/to/pipeline_script.py
```

---

### **6. Best Practices**
- **Error Handling**: Use `try-except` blocks and rollback on failure.
- **Logging**: Log success/failure events for debugging.
- **Performance**: Push transformations to Snowflake (avoid Pandas for large data).
- **Security**: Store credentials in environment variables or secret managers.

---

This script demonstrates a **fully integrated Snowpark pipeline** that scales for large datasets while adhering to best practices. Replace placeholder values (e.g., `ACCOUNT`, `USER`) with your Snowflake credentials.

---
Here are detailed answers to the interview questions, structured to demonstrate technical knowledge and practical application:

---

### **1. Handling Missing Data in Pandas**
**Key Techniques**:
- **Identify Missing Values**:  
  Use `isnull()` or `isna()` to detect missing values.  
  ```python
  df.isnull().sum()  # Count missing values per column
  ```

- **Drop Missing Values**:  
  Use `dropna()` to remove rows/columns with missing values.  
  ```python
  df.dropna(inplace=True)  # Remove rows with any missing values
  df.dropna(axis=1, thresh=100)  # Drop columns with <100 non-null values
  ```

- **Fill Missing Values**:  
  Use `fillna()` with scalars, forward/backward fill, or statistical values.  
  ```python
  df.fillna(0, inplace=True)  # Fill with 0
  df.fillna(df.mean(), inplace=True)  # Fill numeric NaNs with column means
  df.fillna(method='ffill', inplace=True)  # Forward-fill missing values
  ```

- **Interpolate Missing Values**:  
  Use `interpolate()` for time-series or linear interpolation.  
  ```python
  df.interpolate(method='linear', inplace=True)
  ```

**When to Use Each**:
- Drop if missing data is <5% and random.
- Fill with mean/median/mode for numerical data.
- Use interpolation for time-series data.
- Replace with domain-specific values (e.g., `"Unknown"` for categorical features).

---

### **2. Merge Two Datasets and Remove Duplicates**
**Function to Merge and Deduplicate**:
```python
import pandas as pd

def merge_and_deduplicate(df1: pd.DataFrame, df2: pd.DataFrame, key: str) -> pd.DataFrame:
    """
    Merges two datasets on a common key and removes duplicates.
    
    Args:
        df1: First DataFrame
        df2: Second DataFrame
        key: Column name to merge on
        
    Returns:
        Merged and deduplicated DataFrame
    """
    try:
        # Merge datasets (outer join to retain all rows)
        merged_df = pd.merge(df1, df2, on=key, how='outer')
        
        # Remove duplicates based on all columns
        deduped_df = merged_df.drop_duplicates()
        
        return deduped_df
    except Exception as e:
        print(f"Error: {e}")
        return pd.DataFrame()
```

**Example Usage**:
```python
df_a = pd.DataFrame({'id': [1, 2, 3], 'name': ['Alice', 'Bob', 'Charlie']})
df_b = pd.DataFrame({'id': [2, 3, 4], 'age': [25, 30, 35]})

result = merge_and_deduplicate(df_a, df_b, 'id')
print(result)
```

**Output**:
```
   id     name   age
0   1    Alice   NaN
1   2      Bob  25.0
2   3  Charlie  30.0
3   4      NaN  35.0
```

**Notes**:
- `drop_duplicates()` removes identical rows (based on all columns).
- Adjust `subset=[cols]` to deduplicate on specific columns.
- Handle missing values post-merge if needed (e.g., `fillna()`).

---

### **3. Optimize a Slow ETL Script for Millions of Rows**
**Strategies for Optimization**:
#### **A. Data Processing Optimization**
1. **Use Efficient Data Types**:  
   Downcast numeric types and use `category` for strings.  
   ```python
   df['column'] = df['column'].astype('category')  # Reduce memory usage
   df['int_col'] = pd.to_numeric(df['int_col'], downcast='integer')
   ```

2. **Vectorized Operations**:  
   Avoid loops; use Pandas/Numpy vectorization.  
   ```python
   # Slow
   df['new_col'] = [x * 2 for x in df['old_col']]
   
   # Fast
   df['new_col'] = df['old_col'] * 2
   ```

3. **Chunking for Large Files**:  
   Process data in batches with `chunksize`.  
   ```python
   for chunk in pd.read_csv("large_file.csv", chunksize=100000):
       process(chunk)  # Process 100k rows at a time
   ```

4. **Parallel Processing**:  
   Use **Dask** or **PySpark** for distributed computing.  
   ```python
   import dask.dataframe as dd
   df = dd.read_csv("huge_data.csv")
   result = df.groupby("category").value.mean().compute()
   ```

#### **B. Database Optimization**
1. **Bulk Loading**:  
   Replace row-wise inserts with bulk operations.  
   ```python
   from snowflake.connector.pandas_tools import write_pandas
   write_pandas(conn, df, "TARGET_TABLE")  # Bulk load to Snowflake
   ```

2. **Push Down Transformations**:  
   Offload filtering/aggregation to SQL.  
   ```sql
   -- Instead of filtering in Python
   SELECT * FROM source_table WHERE date > '2023-01-01'
   ```

#### **C. Code Profiling & Monitoring**
1. **Profile Bottlenecks**:  
   Use `cProfile` to identify slow functions.  
   ```bash
   python -m cProfile -s time etl_script.py
   ```

2. **Logging & Metrics**:  
   Track execution time and memory usage.  
   ```python
   import logging
   import time

   start_time = time.time()
   logging.info("Processing data...")
   # Your code
   logging.info(f"Completed in {time.time() - start_time:.2f} seconds")
   ```

#### **D. Infrastructure Scalability**
1. **Snowpark for Large-Scale ETL**:  
   Leverage Snowflake’s compute resources.  
   ```python
   session = Session.builder.configs(connection_params).create()
   snow_df = session.read.parquet("@stage/data/")
   snow_df.write.save_as_table("target_table")
   ```

2. **Cloud-Based Solutions**:  
   Use AWS Glue, Azure Data Factory, or GCP Dataflow for serverless ETL.

---

### **Summary of Best Practices**
| Area               | Optimization Strategy                          |
|--------------------|-----------------------------------------------|
| **Data Types**     | Use `category`, downcast integers/floats      |
| **Vectorization**  | Avoid loops; use Pandas/Numpy ops             |
| **Batch Processing**| Chunking for large datasets                   |
| **Distributed Computing**| Dask, PySpark, or Snowpark               |
| **Database**       | Bulk loads, push-down transformations         |
| **Monitoring**     | Profile code, log metrics                     |

By applying these strategies, you can reduce runtime from hours to minutes and scale ETL pipelines for big data.
---
Here’s a structured guide to **SQL** for data engineering, focusing on **general concepts** and **Snowflake-specific features**, with examples, optimization tips, and best practices.

---

### **1. Core SQL Queries**
#### **SELECT Statements**
- **Basic Query**:
  ```sql
  SELECT name, department, salary 
  FROM employees 
  WHERE salary > 50000 
  ORDER BY salary DESC;
  ```

- **DISTINCT Values**:
  ```sql
  SELECT DISTINCT region 
  FROM sales_data;
  ```

#### **JOINs**
- **INNER JOIN**: Get matching rows from both tables.
  ```sql
  SELECT o.order_id, c.name
  FROM orders o
  INNER JOIN customers c ON o.customer_id = c.id;
  ```

- **LEFT JOIN**: Include unmatched rows from the left table.
  ```sql
  SELECT c.name, o.order_id
  FROM customers c
  LEFT JOIN orders o ON c.id = o.customer_id;
  ```

- **SELF JOIN**: Compare rows within the same table.
  ```sql
  SELECT a.name AS employee, b.name AS manager
  FROM employees a
  INNER JOIN employees b ON a.manager_id = b.id;
  ```

#### **GROUP BY & Aggregation**
- **Basic Aggregation**:
  ```sql
  SELECT department, 
         COUNT(*) AS num_employees, 
         AVG(salary) AS avg_salary
  FROM employees
  GROUP BY department;
  ```

- **HAVING Clause** (filter after aggregation):
  ```sql
  SELECT department, AVG(salary) AS avg_salary
  FROM employees
  GROUP BY department
  HAVING AVG(salary) > 60000;
  ```

#### **Subqueries & CTEs**
- **Subquery**:
  ```sql
  SELECT name, salary
  FROM employees
  WHERE salary > (SELECT AVG(salary) FROM employees);
  ```

- **Common Table Expression (CTE)**:
  ```sql
  WITH AvgSalary AS (
    SELECT AVG(salary) AS avg_salary FROM employees
  )
  SELECT name, salary
  FROM employees, AvgSalary
  WHERE salary > AvgSalary.avg_salary;
  ```

#### **Window Functions**
- **Ranking**:
  ```sql
  SELECT name, department, salary,
         RANK() OVER (PARTITION BY department ORDER BY salary DESC) AS rank
  FROM employees;
  ```

- **Running Totals**:
  ```sql
  SELECT sale_date, amount,
         SUM(amount) OVER (ORDER BY sale_date) AS running_total
  FROM sales;
  ```

---

### **2. Snowflake-Specific Features**
#### **Time Travel (Data Recovery)**
- Query historical data (within retention period, up to 90 days):
  ```sql
  -- Restore a dropped table
  UNDROP TABLE sales_data;

  -- Query data as of 1 hour ago
  SELECT * FROM sales_data 
  AT (TIMESTAMP => CURRENT_TIMESTAMP() - INTERVAL '1 HOUR');
  ```

#### **Semi-Structured Data (JSON/Parquet)**
- **VARIANT Type**: Store JSON directly.
  ```sql
  CREATE TABLE logs (
    id INT,
    data VARIANT
  );

  -- Query JSON fields
  SELECT data:name::STRING AS user_name,
         data:address.city::STRING AS city
  FROM logs;
  ```

- **FLATTEN Function**: Expand arrays in JSON.
  ```sql
  SELECT f.value::STRING AS item
  FROM orders, LATERAL FLATTEN(input => data:items) f;
  ```

#### **Query Optimization in Snowflake**
- **Clustering Keys** (for large tables):
  ```sql
  ALTER TABLE sales 
  CLUSTER BY (region, sale_date);
  ```

- **Materialized Views**:
  ```sql
  CREATE MATERIALIZED VIEW sales_summary AS
  SELECT region, SUM(amount) AS total_sales
  FROM sales
  GROUP BY region;
  ```

- **RESULT_SCAN**: Reuse cached query results.
  ```sql
  SET query_tag = 'cached_query';
  SELECT * FROM large_table WHERE date = '2023-01-01';

  -- Reuse cached result later
  SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
  ```

---

### **3. Performance Optimization**
#### **Indexing (Snowflake Clustering vs. Traditional Indexes)**
- **Traditional Databases** (e.g., PostgreSQL/MySQL):
  ```sql
  CREATE INDEX idx_customer_id ON orders(customer_id);
  ```

- **Snowflake Clustering Keys**:
  ```sql
  CREATE TABLE sales (
    sale_id INT,
    region STRING,
    amount NUMBER
  ) CLUSTER BY (region, sale_date);
  ```

#### **Partitioning**
- **Range Partitioning** (e.g., by date):
  ```sql
  CREATE TABLE sales (
    sale_date DATE,
    amount NUMBER
  ) PARTITION BY RANGE (sale_date) (
    PARTITION p_2023_01 VALUES LESS THAN ('2023-02-01'),
    PARTITION p_2023_02 VALUES LESS THAN ('2023-03-01')
  );
  ```

- **List Partitioning** (e.g., by region):
  ```sql
  CREATE TABLE sales (
    region STRING,
    amount NUMBER
  ) PARTITION BY LIST (region) (
    PARTITION p_na VALUES IN ('US', 'CA'),
    PARTITION p_eu VALUES IN ('UK', 'DE')
  );
  ```

---

### **4. Data Modeling**
#### **Star Schema**
- **Fact Table**: Core metrics (e.g., sales).
- **Dimension Tables**: Descriptive data (e.g., customers, products).

**Example**:
```sql
-- Fact Table
CREATE TABLE fact_sales (
  sale_id INT,
  customer_id INT,
  product_id INT,
  amount DECIMAL(10,2)
);

-- Dimension Table
CREATE TABLE dim_customers (
  customer_id INT PRIMARY KEY,
  name STRING,
  region STRING
);
```

#### **Normalization vs. Denormalization**
- **Normalization** (reduce redundancy):
  ```sql
  -- Customers in one table, orders in another
  SELECT * FROM customers c
  JOIN orders o ON c.id = o.customer_id;
  ```

- **Denormalization** (optimize for reads):
  ```sql
  -- Single table with redundant customer info
  CREATE TABLE denormalized_sales (
    sale_id INT,
    customer_name STRING,
    customer_region STRING,
    amount DECIMAL(10,2)
  );
  ```

#### **When to Use Which?**
- **Normalized**: OLTP - Online Transaction Processing systems (frequent writes, data integrity).
- **Denormalized**: OLAP - Online Analytical Processing systems (complex queries, read-heavy workloads).

- **Normalization** is about organizing your information neatly in separate places to avoid repetition and ensure accuracy, especially when you have lots of changes happening.
- **Denormalization** is about putting information together in one place to make it faster to look up and analyze, even if it means repeating some information. You choose based on whether you do more writing/changing of data or more reading/analyzing of data.

---

### **5. Best Practices**
#### **General SQL**
- Use `EXPLAIN ANALYZE` to debug query plans.
- Avoid `SELECT *`; select only required columns.
- Filter early with `WHERE` to reduce data processed.

#### **Snowflake**
- Use **clustering keys** for large tables.
- Leverage **materialized views** for pre-aggregated data.
- Use **stages** (`@stage`) for bulk data loading.

#### **Performance**
- Partition tables by date or region.
- Use appropriate **data types** (e.g., `DATE` instead of `STRING`).
- Use **bulk operations** instead of row-wise inserts.

#### **Common Pitfalls**
- **Unbounded Window Frames**: Always specify `ROWS BETWEEN`.
- **Overusing DISTINCT**: Often a sign of incorrect JOINs.
- **Ignoring NULLs**: Use `COALESCE` or `IFNULL` for safer logic.

---

### **6. Example: Optimized Snowflake ELT**
```sql
-- Stage raw CSV data
CREATE STAGE sales_stage 
  URL = 's3://my-bucket/sales/'
  CREDENTIALS = (AWS_KEY_ID='...' AWS_SECRET_KEY='...');

-- Load into Snowflake table
COPY INTO raw_sales 
FROM @sales_stage 
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1);

-- Transform with clustering
CREATE OR REPLACE TABLE cleaned_sales CLUSTER BY (sale_date) AS
SELECT 
  sale_id,
  sale_date,
  region,
  amount * 1.1 AS adjusted_amount  -- Add tax
FROM raw_sales
WHERE amount > 0;

-- Schedule refresh with a task
CREATE TASK refresh_cleaned_sales
  WAREHOUSE = compute_wh
  SCHEDULE = 'USING CRON 0 8 * * * UTC'
AS
  INSERT INTO cleaned_sales SELECT ...;
```

---

By combining **SQL fundamentals** with **Snowflake’s cloud-native features**, you can build scalable, high-performance data pipelines. Always pair query optimization with proper schema design and Snowflake-specific tools like clustering and time travel.

---
Here are detailed answers to the interview questions, tailored to **Snowflake** and general SQL principles:

---

### **1. How do you optimize a query joining large tables in Snowflake?**

#### **Key Optimization Strategies**:
1. **Clustering Keys**:
   - Define clustering keys on columns used in `JOIN`, `WHERE`, or `GROUP BY` clauses to reduce data scanned.
   - Example:
     ```sql
     ALTER TABLE large_table CLUSTER BY (region, sale_date);
     ```

2. **Join Order & Type**:
   - Join small tables first to reduce intermediate result sizes.
   - Use `BROADCAST` hints for small dimension tables:
     ```sql
     SELECT /*+ BROADCAST(dim_table) */ ...
     ```

3. **Materialized Views**:
   - Precompute expensive joins/aggregations:
     ```sql
     CREATE MATERIALIZED VIEW sales_summary AS
     SELECT region, SUM(amount) AS total_sales
     FROM sales
     GROUP BY region;
     ```

4. **Prune Unnecessary Data**:
   - Filter early with `WHERE` clauses to reduce rows processed.
   - Select only required columns (avoid `SELECT *`).

5. **Query Profiling**:
   - Use `EXPLAIN` to analyze execution plans:
     ```sql
     EXPLAIN USING TABULAR SELECT * FROM large_table WHERE date = '2023-01-01';
     ```

6. **Result Caching**:
   - Reuse cached results for repetitive queries:
     ```sql
     SET query_tag = 'cached_query';
     SELECT * FROM large_table WHERE date = '2023-01-01';
     -- Later:
     SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
     ```

7. **Warehouse Scaling**:
   - Use multi-cluster warehouses for high concurrency:
     ```sql
     CREATE WAREHOUSE elastic_wh 
     MAX_CLUSTER_COUNT = 5;
     ```

---

### **2. Explain the difference between a view and a materialized view.**

| Feature               | **View**                                  | **Materialized View**                          |
|-----------------------|-------------------------------------------|------------------------------------------------|
| **Storage**           | Virtual (no physical storage)             | Stores precomputed results on disk             |
| **Performance**       | Slow for complex queries (executed at runtime) | Fast (results cached)                          |
| **Data Freshness**    | Real-time (reflects current table data)   | Stale until refreshed (auto-refreshed in Snowflake) |
| **Use Case**          | Simplify complex queries or security views| Speed up frequently run queries on large datasets |
| **Refresh**           | No refresh needed                         | Automatically refreshed when base data changes |

#### **Example**:
- **View**:
  ```sql
  CREATE VIEW active_users AS
  SELECT * FROM users WHERE status = 'active';
  ```
- **Materialized View**:
  ```sql
  CREATE MATERIALIZED VIEW sales_summary AS
  SELECT region, SUM(amount) AS total_sales
  FROM sales
  GROUP BY region;
  ```

---

### **3. Write a query to find duplicate records in a table.**

#### **Method 1: Using GROUP BY and HAVING**
Finds duplicates based on specific columns (e.g., `name`, `email`):
```sql
SELECT name, email, COUNT(*) AS duplicate_count
FROM users
GROUP BY name, email
HAVING COUNT(*) > 1;
```

#### **Method 2: Using ROW_NUMBER() Window Function**
Identifies full duplicate rows (all columns identical):
```sql
WITH RankedRows AS (
  SELECT *, 
         ROW_NUMBER() OVER (PARTITION BY name, email ORDER BY id) AS rn
  FROM users
)
SELECT *
FROM RankedRows
WHERE rn > 1;
```

#### **Method 3: Using QUALIFY (Snowflake-Specific)**
Simplifies filtering duplicates directly:
```sql
SELECT *
FROM users
QUALIFY ROW_NUMBER() OVER (PARTITION BY name, email ORDER BY id) > 1;
```

---
Here’s a structured overview of **Snowflake basics** covering **warehouses**, **roles**, **stages**, and **file formats**, with examples and best practices:

---

### **1. Warehouses (Compute Clusters)**
**Purpose**:  
Snowflake's compute layer responsible for executing queries (e.g., SELECT, INSERT, JOIN).  
- **Separation of Compute & Storage**: Warehouses scale independently from data storage.  
- **Multi-Cluster Support**: Handle concurrent workloads (e.g., separate warehouses for ETL and reporting).  

#### **Key Concepts**:
- **Warehouse Types**:
  - **Standard**: General-purpose compute (e.g., `XSMALL` to `X LARGE`).  
  - **Snowpark**: Optimized for Python/Java/Scala code execution.  
  - **Data Exchange**: For data sharing with external accounts.  

- **Auto-Suspend/Resume**:  
  Save costs by auto-suspending idle warehouses.  
  ```sql
  CREATE WAREHOUSE reporting_wh
  WAREHOUSE_SIZE = 'MEDIUM'
  AUTO_SUSPEND = 60; -- Suspend after 60 seconds of inactivity
  ```

- **Scaling**:
  - **Single-Cluster**: Fixed size (e.g., `MEDIUM`).  
  - **Multi-Cluster**: Auto-scale based on workload.  
    ```sql
    CREATE WAREHOUSE elastic_wh
    WAREHOUSE_SIZE = 'XSMALL'
    MAX_CLUSTER_COUNT = 5;
    ```

**Best Practice**:  
Use smaller warehouses for lightweight queries and larger ones for bulk data processing.

---

### **2. Roles (Access Control)**
**Purpose**:  
Role-Based Access Control (RBAC) manages permissions for users and objects.  

#### **Default Roles**:
| Role           | Permissions                          |
|----------------|--------------------------------------|
| **ACCOUNTADMIN** | Full administrative control         |
| **SYSADMIN**     | Manages warehouses, databases, roles |
| **USER**         | Default role for new users          |

#### **Custom Roles**:
```sql
-- Create a role
CREATE ROLE analyst;

-- Grant privileges
GRANT SELECT ON TABLE sales_data TO ROLE analyst;

-- Assign role to a user
GRANT ROLE analyst TO USER john_doe;
```

#### **Role Hierarchy**:
```sql
-- Grant a role to another role
GRANT ROLE analyst TO ROLE data_scientist;
```

**Best Practice**:  
Follow the **principle of least privilege**—grant only necessary permissions.

---

### **3. Stages (Data Storage for Loading)**
**Purpose**:  
Stages store data files (e.g., CSV, JSON, Parquet) before loading into Snowflake tables.  

#### **Types of Stages**:
1. **Internal Stages**:
   - Managed by Snowflake.  
   - Types:  
     - **User Stage**: Private to a user (`@~`).  
     - **Table Stage**: Private to a table (`@%table_name`).  
     - **Named Internal Stage**: Custom stage for shared access.

   ```sql
   -- Create a named internal stage
   CREATE STAGE my_stage;

   -- Upload files to the stage (CLI)
   PUT file:///path/to/data.csv @my_stage/data/;
   ```

2. **External Stages**:
   - Point to external cloud storage (e.g., AWS S3, Azure Blob).  
   - Requires credentials and storage integration.  

   ```sql
   -- Create external stage pointing to S3
   CREATE STAGE s3_stage
   URL = 's3://my-bucket/data/'
   CREDENTIALS = (AWS_KEY_ID='...' AWS_SECRET_KEY='...');
   ```

#### **Loading Data**:
```sql
-- Load data from a stage into a table
COPY INTO sales_table
FROM @my_stage/data/
FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1);
```

**Best Practice**:  
Use **external stages** for large datasets and **internal stages** for small, transient files.

---

### **4. File Formats**
**Purpose**:  
Define how Snowflake parses data files (e.g., CSV, JSON, Parquet).  

#### **Supported Formats**:
| Format    | Use Case                        |
|-----------|---------------------------------|
| CSV       | Simple tabular data             |
| JSON      | Semi-structured data            |
| Parquet   | Columnar format for large data  |
| ORC       | Hadoop ecosystem compatibility  |
| Avro      | Schema evolution support        |

#### **Create File Format**:
```sql
-- CSV format
CREATE FILE FORMAT csv_format
TYPE = CSV
FIELD_DELIMITER = ','
SKIP_HEADER = 1;

-- JSON format
CREATE FILE FORMAT json_format
TYPE = JSON;

-- Parquet format
CREATE FILE FORMAT parquet_format
TYPE = PARQUET;
```

#### **Reference in COPY INTO**:
```sql
COPY INTO customers
FROM @my_stage/customers/
FILE_FORMAT = csv_format;
```

**Best Practice**:  
Use **Parquet** or **ORC** for large-scale data due to compression and columnar efficiency.

---

### **Summary Table**
| Component       | Key Features                                      | Best Use Case                          |
|-----------------|---------------------------------------------------|----------------------------------------|
| **Warehouses**  | Scalable compute clusters, auto-suspend, multi-cluster | Query execution, ETL, reporting        |
| **Roles**       | RBAC, hierarchy, granular permissions             | Security, access control               |
| **Stages**      | Internal/external storage for data files          | Data ingestion (bulk loading)          |
| **File Formats**| CSV, JSON, Parquet, ORC, Avro                     | Parsing structured/semi-structured data|

---

### **Snowflake-Specific Tips**
- **Time Travel**: Recover dropped tables or historical data (up to 90 days).  
  ```sql
  UNDROP TABLE sales_data;
  ```
- **Zero-Copy Cloning**: Create instant copies for testing.  
  ```sql
  CREATE TABLE test_sales CLONE production_sales;
  ```
- **Result Caching**: Reuse query results for 24 hours.  
  ```sql
  SET query_tag = 'cached_query';
  SELECT * FROM large_table WHERE date = '2023-01-01';
  ```

By mastering these core components, you can efficiently manage **data pipelines**, **security**, and **query performance** in Snowflake.
---

### **Summary of Best Practices**
- **Optimize Joins**: Use clustering keys, filter early, and leverage materialized views.
- **Views vs. Materialized Views**: Use views for real-time logic and materialized views for performance-critical queries.
- **Detect Duplicates**: Use `GROUP BY HAVING`, `ROW_NUMBER()`, or `QUALIFY` based on your needs.

By applying these strategies, you can ensure **efficient Snowflake queries**, **scalable data models**, and **clean data pipelines**.
---
Here’s a structured guide to **using Snowpark in Python** to interact with **Snowflake data** using **DataFrames**, including setup, transformations, and best practices.

---

### **1. Setup & Configuration**
#### **Install Snowpark Python Package**:
```bash
pip install snowflake-snowpark-python
```

#### **Configure Connection**:
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
```

---

### **2. Key Concepts in Snowpark**
#### **Snowpark DataFrame vs. Pandas DataFrame**:
- **Snowpark DataFrames**: Lazily evaluated, operations are pushed to Snowflake (not loaded into memory).  
- **Pandas DataFrames**: In-memory, single-node processing (suitable for small datasets).  

#### **Core Operations**:
- **Create DataFrame**: From Snowflake tables, stages, or queries.  
- **Transform**: Use DataFrame APIs (e.g., `filter`, `select`, `agg`).  
- **Action**: Trigger execution (e.g., `show()`, `collect()`).  

---

### **3. Working with DataFrames**
#### **Step 1: Read Data**
- **From Snowflake Table**:
  ```python
  df = session.table("SALES_DATA")
  df.show()
  ```

- **From External Stage (CSV/JSON/Parquet)**:
  ```python
  df = session.read.option("pattern", ".*sales.*.csv").csv("@MY_STAGE/sales/")
  df = df.to_df(["DATE", "REGION", "AMOUNT"])  # Rename columns
  ```

#### **Step 2: Transform Data**
- **Select Columns**:
  ```python
  df_selected = df.select("DATE", "AMOUNT")
  ```

- **Filter Rows**:
  ```python
  df_filtered = df.filter((df.AMOUNT > 1000) & (df.REGION == "APAC"))
  ```

- **Aggregate Data**:
  ```python
  from snowflake.snowpark.functions import col, sum as _sum

  df_agg = df.group_by("REGION").agg(_sum(col("AMOUNT")).alias("TOTAL_SALES"))
  ```

- **Join DataFrames**:
  ```python
  df_regions = session.table("REGIONS")
  df_joined = df.join(df_regions, df["REGION"] == df_regions["NAME"], join_type="inner")
  ```

#### **Step 3: Save Results**
- **Write to Table**:
  ```python
  df_agg.write.mode("overwrite").save_as_table("REGIONAL_SALES")
  ```

- **Create Temporary View**:
  ```python
  df.create_or_replace_temp_view("TEMP_SALES_VIEW")
  ```

---

### **4. Advanced Features**
#### **User-Defined Functions (UDFs)**
Define custom logic in Python:
```python
from snowflake.snowpark.functions import udf
from snowflake.snowpark.types import IntegerType, StringType

@udf(name="ADD_TAX", return_type=IntegerType(), input_types=[IntegerType()])
def add_tax(amount: int) -> int:
    return int(amount * 1.1)

# Use in DataFrame
df.withColumn("AMOUNT_WITH_TAX", add_tax(col("AMOUNT"))).show()
```

#### **Stored Procedures**
Encapsulate logic for reuse:
```python
def run_etl(session: Session):
    try:
        df = session.table("RAW_SALES")
        df_cleaned = df.filter(col("AMOUNT") > 0)
        df_cleaned.write.mode("overwrite").save_as_table("CLEANED_SALES")
        return "Success"
    except Exception as e:
        return f"Error: {str(e)}"

session.sproc.register(run_etl, name="RUN_ETL_PIPELINE", is_permanent=True, stage_location="@MY_STAGE/sp")
```

#### **Call Stored Procedure**:
```sql
CALL RUN_ETL_PIPELINE();
```

---

### **5. Query Optimization**
- **Clustering Keys**:
  ```python
  session.sql("""
  CREATE TABLE CLEANED_SALES (
    DATE DATE,
    AMOUNT INT
  ) CLUSTER BY (DATE)
  """).collect()
  ```

- **Caching**:
  ```python
  df_cached = df.cache_result()
  ```

- **Push Down Filters**:
  Avoid converting to Pandas until final step to leverage Snowflake’s compute.

---

### **6. Complete ETL Pipeline Example**
```python
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, sum as _sum

def etl_pipeline(session: Session):
    try:
        # Extract
        df_sales = session.read.option("pattern", ".*sales.*.csv").csv("@MY_STAGE/sales/")
        df_regions = session.table("REGIONS")

        # Transform
        df_cleaned = df_sales.filter(col("_c2").cast("int") > 0)  # Filter invalid amounts
        df_joined = df_cleaned.join(df_regions, df_cleaned["_c1"] == df_regions["NAME"])
        df_agg = df_joined.group_by("CONTINENT").agg(_sum(col("_c2")).alias("TOTAL_SALES"))

        # Load
        df_agg.write.mode("overwrite").save_as_table("GLOBAL_SALES_SUMMARY")
        return "Pipeline Success"
    except Exception as e:
        return f"Pipeline Failed: {str(e)}"

# Register and call as stored procedure
session.sproc.register(etl_pipeline, name="RUN_ETL", is_permanent=True, stage_location="@MY_STAGE/sp")
session.call("RUN_ETL")
```

---

### **7. Best Practices**
1. **Use Lazy Evaluation**: Chain transformations before calling actions (e.g., `collect()`).  
2. **Minimize Data Transfer**: Avoid `.to_pandas()` unless necessary.  
3. **Session Management**: Close sessions properly:
   ```python
   session.close()
   ```
4. **Logging**: Use Python’s `logging` module for debugging.  
5. **Security**: Store credentials in environment variables or secret managers (e.g., AWS Secrets Manager).

---

### **8. Automation**
- **Snowflake Tasks**:
  ```sql
  CREATE TASK RUN_ETL_DAILY
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = 'USING CRON 0 8 * * * UTC'
  AS
    CALL RUN_ETL();
  ```

- **Apache Airflow**:
  Use Snowflake hook to trigger stored procedures:
  ```python
  from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

  def run_snowpark_etl():
      hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
      hook.run("CALL RUN_ETL()")
  ```

---

By leveraging **Snowpark’s DataFrame API**, you can build scalable, efficient data pipelines directly in Python while harnessing Snowflake’s cloud-native compute and storage capabilities. Always combine this with Snowflake’s features like clustering and materialized views for optimal performance.

---
Here’s a structured guide to **integrating Snowpark** with **Pandas** or **Spark-like** operations for hybrid data engineering workflows, including best practices, code examples, and performance considerations.

---

### **1. Snowpark vs. Pandas vs. PySpark**
| Feature               | **Snowpark**                          | **Pandas**                        | **PySpark**                         |
|-----------------------|----------------------------------------|-----------------------------------|-------------------------------------|
| **Execution Engine**  | Snowflake (cloud-native)               | Local (in-memory)                 | Distributed (Spark clusters)        |
| **Scalability**       | Scales with Snowflake compute          | Limited to single machine         | Scales horizontally                 |
| **Use Case**          | Cloud ETL, large-scale SQL/DataFrames  | Small datasets, quick analysis    | Big data processing (batch/stream)  |
| **Data Transfer**     | Minimal (data stays in Snowflake)      | Full data loaded into memory      | Data stored in Spark cluster memory |

---

### **2. Integrating Snowpark with Pandas**
#### **Scenario**:  
You want to leverage Snowflake for initial data filtering/aggregation and then use Pandas for detailed analysis.

#### **Steps**:
1. **Extract Data in Snowpark**:
   ```python
   from snowflake.snowpark import Session

   session = Session.builder.configs(connection_params).create()
   snow_df = session.table("LARGE_SALES_DATA").filter(col("AMOUNT") > 1000)
   ```

2. **Convert to Pandas**:
   ```python
   pandas_df = snow_df.to_pandas()  # Triggers execution and pulls data
   ```

3. **Perform Pandas Operations**:
   ```python
   pandas_df["tax"] = pandas_df["AMOUNT"] * 0.1
   pandas_df.groupby("REGION")["tax"].sum().plot(kind="bar")
   ```

#### **Best Practices**:
- **Filter Early**: Use Snowpark to reduce data size before converting to Pandas.
- **Memory Limits**: Avoid converting massive datasets to Pandas (risk of OOM errors).
- **Caching**: Cache intermediate Snowpark results with `.cache_result()`.

---

### **3. Snowpark with PySpark (Spark-Like Operations)**
#### **Scenario**:  
You want to use Spark-like distributed processing but store data in Snowflake.

#### **Integration Options**:
1. **Read from Snowflake into Spark**:
   ```python
   spark = SparkSession.builder \
       .appName("SnowflakeIntegration") \
       .getOrCreate()

   df = spark.read \
       .format("snowflake") \
       .options(**{
           "sfURL": "https://<account>.snowflakecomputing.com",
           "sfUser": "user",
           "sfPassword": "password",
           "sfDatabase": "DB",
           "sfSchema": "SCHEMA",
           "sfWarehouse": "WAREHOUSE"
       }) \
       .option("query", "SELECT * FROM LARGE_TABLE WHERE AMOUNT > 1000") \
       .load()
   ```

2. **Write Back to Snowflake**:
   ```python
   df.write \
       .format("snowflake") \
       .options(**{
           "sfURL": "https://<account>.snowflakecomputing.com",
           "sfUser": "user",
           "sfPassword": "password",
           "sfDatabase": "DB",
           "sfSchema": "SCHEMA",
           "sfWarehouse": "WAREHOUSE"
       }) \
       .option("dbtable", "PROCESSED_DATA") \
       .mode("overwrite") \
       .save()
   ```

#### **Best Practices**:
- **Push Down Predicates**: Filter data in Snowflake before loading into Spark.
- **Use Parquet**: Leverage Snowflake’s `COPY INTO` to unload data in Parquet format for Spark.
- **Warehouse Size**: Use large Snowflake warehouses for high-volume data transfers.

---

### **4. Hybrid Pipeline Example**
#### **Use Case**:  
Ingest raw data in Snowflake, clean with Snowpark, enrich with Pandas, and automate via Snowflake Tasks or Airflow.

#### **Code Snippet**:
```python
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col

def hybrid_pipeline():
    # Step 1: Snowpark for filtering/aggregation
    session = Session.builder.configs(connection_params).create()
    snow_df = session.table("RAW_SALES").filter(col("STATUS") == "COMPLETED")

    # Step 2: Convert to Pandas for complex analysis
    pandas_df = snow_df.to_pandas()
    pandas_df["adjusted_amount"] = pandas_df["AMOUNT"] * 0.95  # Apply discount

    # Step 3: Save back to Snowflake (via Snowpark or Pandas)
    session.write_pandas(pandas_df, "ADJUSTED_SALES", auto_create_table=True)

hybrid_pipeline()
```

#### **Automation**:
- **Snowflake Task**:
  ```sql
  CREATE TASK run_hybrid_pipeline
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = 'USING CRON 0 8 * * * UTC'
  AS
    CALL SYSTEM$EXECUTE_PYTHON_SCRIPT('hybrid_pipeline.py');
  ```

- **Airflow DAG**:
  ```python
  from airflow.operators.python_operator import PythonOperator

  dag = DAG("daily_pipeline", schedule_interval="@daily", start_date=datetime(2023, 1, 1))
  task = PythonOperator(task_id="run_pipeline", python_callable=hybrid_pipeline, dag=dag)
  ```

---

### **5. Performance Optimization**
| Area               | Strategy                                                                 |
|--------------------|--------------------------------------------------------------------------|
| **Data Transfer**   | Use `.filter()` and `.select()` in Snowpark before converting to Pandas. |
| **Caching**         | Cache intermediate Snowpark results with `.cache_result()`.             |
| **Batch Processing**| Use chunked writes for large Pandas DataFrames.                         |
| **Warehouse Size**  | Use larger Snowflake warehouses for heavy transformations.              |
| **Vectorization**   | Use NumPy/Pandas vectorized ops instead of loops.                       |

---

### **6. Error Handling & Logging**
```python
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def safe_pipeline():
    session = Session.builder.configs(connection_params).create()
    try:
        # Your code
        logger.info("Pipeline started")
        snow_df = session.table("RAW_DATA").filter(col("DATE") > "2023-01-01")
        pandas_df = snow_df.to_pandas()
        # Process...
        logger.info("Pipeline completed")
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        session.rollback()
    finally:
        session.close()

safe_pipeline()
```

---

### **7. When to Use Each Tool**
| Scenario                             | Recommended Tool        |
|--------------------------------------|--------------------------|
| Small datasets, local analysis       | Pandas                   |
| Large-scale cloud ETL                | Snowpark                 |
| Distributed batch/streaming pipelines| PySpark + Snowflake      |
| Interactive data exploration         | Snowpark + Pandas        |

---

By combining **Snowpark**, **Pandas**, and **PySpark**, you can build flexible, scalable pipelines that leverage the strengths of each tool. Always minimize data transfer between systems and use Snowflake’s clustering and caching features for optimal performance.
---

Here’s a breakdown of **Snowpark use cases** for **data transformations**, **machine learning**, and **ETL pipelines**, with examples, best practices, and real-world applications.

---

### **1. Data Transformations**
#### **Use Case: Clean and Aggregate Sales Data**
**Problem**:  
You need to clean raw sales data (e.g., filter invalid entries, handle missing values), join with region metadata, and aggregate total sales per region.

#### **Solution with Snowpark**:
```python
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, sum as _sum

def transform_sales_data(session: Session):
    try:
        # Step 1: Read raw CSV from Snowflake stage
        raw_df = session.read.csv("@MY_STAGE/sales/sales.csv")
        raw_df = raw_df.to_df(["DATE", "REGION_ID", "AMOUNT", "QUANTITY"])

        # Step 2: Clean data
        cleaned_df = (
            raw_df
            .filter((col("AMOUNT").is_not_null()) & (col("QUANTITY") > 0))
            .with_column("AMOUNT", col("AMOUNT").cast("float"))
            .with_column("QUANTITY", col("QUANTITY").cast("int"))
        )

        # Step 3: Join with regions table
        regions_df = session.table("REGIONS")
        joined_df = cleaned_df.join(regions_df, cleaned_df["REGION_ID"] == regions_df["ID"], join_type="inner")

        # Step 4: Aggregate sales by region
        agg_df = (
            joined_df
            .group_by("REGION_NAME")
            .agg(_sum(col("AMOUNT")).alias("TOTAL_SALES"))
        )

        # Step 5: Save to target table
        agg_df.write.mode("overwrite").save_as_table("SALES_SUMMARY")
        return "Success"
    except Exception as e:
        session.rollback()
        return f"Error: {str(e)}"

# Register as a stored procedure
session.sproc.register(transform_sales_data, name="TRANSFORM_SALES", is_permanent=True, stage_location="@MY_STAGE/sp")
```

#### **Best Practices**:
- Use **clustering keys** on columns like `REGION_NAME` for faster aggregations.
- Filter early to reduce data processed.
- Use `.cache_result()` for reusable intermediate DataFrames.

---

### **2. Machine Learning (ML) Integration**
#### **Use Case: Preprocess Data for ML Training**
**Problem**:  
You need to preprocess data (e.g., normalize features, encode categories) before exporting it for ML training.

#### **Solution with Snowpark**:
```python
from snowflake.snowpark.functions import col, when
from snowflake.snowpark import Session

def preprocess_for_ml(session: Session):
    try:
        # Step 1: Load customer data
        df = session.table("CUSTOMERS")

        # Step 2: Clean and encode categorical data
        df_cleaned = (
            df
            .filter(col("AGE").is_not_null() & col("INCOME").is_not_null())
            .with_column("GENDER_CODE", when(col("GENDER") == "M", 0).otherwise(1))
        )

        # Step 3: Normalize numerical features
        df_normalized = (
            df_cleaned
            .with_column("INCOME_NORM", (col("INCOME") - 30000) / 70000)  # Example min-max scaling
        )

        # Step 4: Save preprocessed data
        df_normalized.write.mode("overwrite").save_as_table("ML_PREPROCESSED_DATA")
        return "Success"
    except Exception as e:
        session.rollback()
        return f"Error: {str(e)}"

session.sproc.register(preprocess_for_ml, name="PREPROCESS_ML_DATA", is_permanent=True, stage_location="@MY_STAGE/sp")
```

#### **Integration with ML Tools**:
- Export data to Pandas for training:
  ```python
  ml_df = session.table("ML_PREPROCESSED_DATA").to_pandas()
  from sklearn.model_selection import train_test_split
  X_train, X_test = train_test_split(ml_df, test_size=0.2)
  ```

- Use **Snowflake Cortex ML** for in-database model deployment:
  ```sql
  CREATE SNOWFLAKE.ML.MODEL customer_churn_model
  AS (
    SELECT * FROM ML_PREPROCESSED_DATA
  )
  TARGET_LABEL = 'CHURN'
  FUNCTION_NAME = 'PREDICT_CHURN';
  ```

#### **Best Practices**:
- Push preprocessing to Snowpark to avoid data movement.
- Use **UDFs** for custom feature engineering.
- Leverage **Cortex ML** for lightweight in-database models.

---

### **3. ETL Pipelines**
#### **Use Case: Automate Daily Sales Pipeline**
**Problem**:  
Automate a daily pipeline to load new sales data, join with customer demographics, and update a dashboard-ready table.

#### **Solution with Snowpark**:
```python
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col

def daily_sales_pipeline(session: Session):
    try:
        # Step 1: Load new sales data from stage
        new_sales = session.read.csv("@MY_STAGE/sales/daily/")
        new_sales = new_sales.to_df(["DATE", "CUSTOMER_ID", "AMOUNT"])

        # Step 2: Join with customer demographics
        customers = session.table("CUSTOMERS")
        joined = new_sales.join(customers, "CUSTOMER_ID", join_type="inner")

        # Step 3: Aggregate and enrich
        enriched = joined.with_column("CATEGORY", when(col("AMOUNT") > 1000, "HIGH").otherwise("LOW"))
        aggregated = enriched.group_by("CATEGORY").count()

        # Step 4: Merge into dashboard table
        dashboard = session.table("DASHBOARD_SALES")
        merged = (
            dashboard.merge(
                aggregated,
                dashboard["CATEGORY"] == aggregated["CATEGORY"],
                when_matched_update=[...]
            )
        )
        merged.write.mode("overwrite").save_as_table("DASHBOARD_SALES")
        return "Pipeline completed"
    except Exception as e:
        session.rollback()
        return f"Error: {str(e)}"

session.sproc.register(daily_sales_pipeline, name="RUN_DAILY_PIPELINE", is_permanent=True, stage_location="@MY_STAGE/sp")
```

#### **Automation Options**:
- **Snowflake Task**:
  ```sql
  CREATE TASK RUN_DAILY_ETL
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = 'USING CRON 0 8 * * * UTC'
  AS
    CALL RUN_DAILY_PIPELINE();
  ```

- **Airflow DAG**:
  ```python
  from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

  def run_pipeline():
      hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
      hook.run("CALL RUN_DAILY_PIPELINE()")

  PythonOperator(task_id="run_etl", python_callable=run_pipeline, dag=dag)
  ```

#### **Best Practices**:
- Use **merge** for upserts instead of full table overwrites.
- Monitor pipeline health via `QUERY_HISTORY`.
- Cache intermediate results with `.cache_result()`.

---

### **4. Best Practices Summary**
| Area               | Practice                                                                 |
|--------------------|--------------------------------------------------------------------------|
| **Performance**    | Push transformations to Snowflake; avoid `.to_pandas()` for large data. |
| **Security**       | Use least-privilege roles and secure credentials in secret managers.    |
| **Error Handling** | Use `try-except` and `.rollback()` for transaction safety.              |
| **Logging**        | Log success/failure events with Python’s `logging` module.              |
| **Scalability**    | Use clustering keys and multi-cluster warehouses for high concurrency.  |

---

### **5. When to Use Snowpark**
| Use Case                  | Recommendation                                                                 |
|---------------------------|----------------------------------------------------------------------------------|
| Small-scale transformations | Use Pandas for local processing.                                               |
| Large-scale transformations | Use Snowpark for in-database processing.                                       |
| ML Preprocessing            | Use Snowpark to clean/encode data, then export to Pandas or Spark.             |
| Real-time ML predictions    | Use Snowflake Cortex ML or external model APIs via stored procedures.          |
| ETL Pipelines               | Use Snowpark for scalable, automated pipelines with Snowflake Tasks or Airflow.|

---

By leveraging **Snowpark**, you can build **end-to-end data workflows**—from ingestion to transformation to ML—while staying within Snowflake’s secure, scalable environment. Always combine this with Snowflake’s native features like **clustering**, **materialized views**, and **result caching** for optimal performance.
---

Here’s a structured guide to **transforming data** and **creating UDFs** in **Snowpark**, with examples and best practices:

---

### **1. Transform Data: Calculate Average Salary per Department & Join**
#### **Objective**:  
Calculate **average salary per department** and join with a **departments table** to enrich results with department names.

#### **Code Example**:
```python
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, avg, udf

# 1. Setup Snowpark Session
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

try:
    # 2. Read Tables
    employees_df = session.table("EMPLOYEES")  # Columns: id, name, department_id, salary
    departments_df = session.table("DEPARTMENTS")  # Columns: id, name

    # 3. Aggregate Salaries by Department
    avg_salary_df = (
        employees_df
        .group_by("department_id")
        .agg(avg(col("salary")).alias("avg_salary"))
    )

    # 4. Join with Departments Table
    joined_df = (
        avg_salary_df.join(departments_df, avg_salary_df["department_id"] == departments_df["id"])
        .select(departments_df["name"].alias("department_name"), avg_salary_df["avg_salary"])
    )

    # 5. Save to Target Table
    joined_df.write.mode("overwrite").save_as_table("DEPARTMENT_AVG_SALARY")

    print("Average salary per department calculated and saved.")
except Exception as e:
    session.rollback()
    print(f"Error: {str(e)}")
finally:
    session.close()
```

#### **Key Concepts**:
- **Group By & Aggregation**: Use `group_by` and `agg(avg(...))` for efficient aggregations.
- **Join**: Use `join(...)` to enrich results with department names.
- **Session Management**: Always close the session and handle exceptions with `rollback()`.

---

### **2. Create a Python UDF to Categorize Salary Ranges**
#### **Objective**:  
Create a **UDF** to categorize salaries into ranges (e.g., Low, Medium, High).

#### **Code Example**:
```python
# 1. Define the UDF Function
def categorize_salary(salary: float) -> str:
    if salary < 50000:
        return "Low"
    elif 50000 <= salary < 100000:
        return "Medium"
    else:
        return "High"

# 2. Register the UDF in Snowpark
salary_category_udf = udf(
    categorize_salary,
    return_type=StringType(),
    input_types=[FloatType()],
    name="CATEGORIZE_SALARY",
    is_permanent=True,
    stage_location="@MY_STAGE/udfs"
)

# 3. Apply the UDF to a DataFrame
try:
    employees_with_category = (
        employees_df
        .with_column("salary_category", salary_category_udf(col("salary")))
        .select("name", "salary", "salary_category")
    )

    # Save results
    employees_with_category.write.mode("overwrite").save_as_table("EMPLOYEE_SALARY_CATEGORIES")
    print("Salary categories applied and saved.")
except Exception as e:
    session.rollback()
    print(f"Error: {str(e)}")
```

#### **Key Concepts**:
- **UDF Registration**: Use `udf(...)` to define return/input types and deploy to Snowflake.
- **Apply UDF**: Use `with_column(...)` to add a new column based on the UDF.
- **Permanent UDFs**: Set `is_permanent=True` and specify a stage for reuse across sessions.

---

### **3. Best Practices**
#### **Performance Optimization**
- **Clustering Keys**: Use clustering on `department_id` for faster joins/aggregations:
  ```sql
  ALTER TABLE EMPLOYEES CLUSTER BY (department_id);
  ```
- **Caching**: Cache intermediate DataFrames if reused:
  ```python
  employees_df.cache_result()
  ```

#### **Error Handling**
- Use `try-except` blocks and `session.rollback()` for transaction safety.
- Validate data (e.g., filter nulls) before transformations:
  ```python
  employees_df = employees_df.filter(col("salary").is_not_null())
  ```

#### **Security**
- Use **least-privilege roles** for Snowpark sessions.
- Store credentials in **secret managers** (e.g., AWS Secrets Manager, HashiCorp Vault).

#### **Logging**
- Log progress and errors using Python’s `logging` module:
  ```python
  import logging
  logging.basicConfig(level=logging.INFO)
  logging.info("Pipeline started")
  ```

---

### **4. Real-World Integration**
#### **Automation Options**
- **Snowflake Task** (scheduled execution):
  ```sql
  CREATE TASK UPDATE_SALARY_ANALYSIS
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = 'USING CRON 0 8 * * * UTC'
  AS
    CALL SYSTEM$EXECUTE_PYTHON_SCRIPT('salary_analysis_script.py');
  ```

- **Airflow DAG** (external orchestration):
  ```python
  from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

  def run_snowpark_pipeline():
      hook = SnowflakeHook(snowflake_conn_id="snowflake_default")
      hook.run("CALL UPDATE_SALARY_ANALYSIS()")

  PythonOperator(task_id="run_pipeline", python_callable=run_snowpark_pipeline, dag=dag)
  ```

---

### **5. Summary**
| Task               | Key Steps                                                                 |
|--------------------|---------------------------------------------------------------------------|
| **Data Transformation** | Use `group_by`, `agg`, and `join` to calculate and enrich results.     |
| **UDF Creation**        | Define a Python function and register it with `udf(...)`.               |
| **Best Practices**      | Use clustering, caching, error handling, and logging for robustness.    |

By combining **Snowpark’s DataFrame API** with **custom UDFs**, you can build scalable, maintainable pipelines for data transformation and analysis directly within Snowflake. Always leverage Snowflake’s native features like clustering and materialized views for optimal performance.

---


### **Interview Question 1: What’s the difference between Snowpark and traditional Snowflake SQL?**

#### **Key Differences**:
| **Aspect**               | **Snowpark**                                                                 | **Traditional Snowflake SQL**                                                                 |
|--------------------------|------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------|
| **Language & Syntax**    | Uses **Python/Java/Scala** with DataFrame APIs (similar to Pandas/Spark).    | Uses **SQL** (declarative, set-based operations).                                             |
| **Execution Model**      | **Lazily evaluated** DataFrames; operations are translated to SQL and executed in Snowflake. | Immediate execution of SQL queries (unless wrapped in stored procedures).                     |
| **Use Cases**            | Complex data engineering, ML pipelines, integration with Python libraries (e.g., NumPy). | Ad-hoc analysis, reporting, simple ETL.                                                       |
| **Integration**          | Supports **UDFs**, stored procedures, and integration with Pandas/ML workflows. | Pure SQL for querying, transformations, and DDL/DML.                                          |
| **Abstraction Level**    | Higher-level abstraction with DataFrame APIs (e.g., `filter`, `group_by`).   | Lower-level control with direct SQL syntax (e.g., `SELECT`, `JOIN`, `GROUP BY`).              |
| **Data Handling**        | Optimized for **large-scale data** without moving it out of Snowflake.       | Data remains in Snowflake, but complex logic may require external tools (e.g., Python scripts). |

#### **Example**:
- **Snowpark**:
  ```python
  df = session.table("SALES")
  df_filtered = df.filter(col("AMOUNT") > 1000)
  df_avg = df_filtered.group_by("REGION").agg(avg("AMOUNT"))
  ```
- **Traditional SQL**:
  ```sql
  SELECT REGION, AVG(AMOUNT) AS AVG_AMOUNT
  FROM SALES
  WHERE AMOUNT > 1000
  GROUP BY REGION;
  ```

---

### **Interview Question 2: How do you handle large-scale data transformations in Snowpark?**

#### **Strategies for Scalability**:
1. **Push Down Transformations**:
   - Let Snowflake handle filtering, aggregation, and joins (avoid `.to_pandas()` for large data).
   - Example:
     ```python
     df_cleaned = df.filter(col("STATUS") == "ACTIVE").select("ID", "AMOUNT")
     ```

2. **Clustering Keys**:
   - Define clustering keys on high-cardinality columns (e.g., `REGION`, `DATE`) to improve query performance.
   - Example:
     ```sql
     ALTER TABLE SALES CLUSTER BY (REGION, SALE_DATE);
     ```

3. **Caching Intermediate Results**:
   - Cache frequently used DataFrames to avoid recomputation.
   - Example:
     ```python
     df_cached = df_cleaned.cache_result()
     ```

4. **Partitioning & Pruning**:
   - Partition data by date or region and use filters to prune irrelevant partitions.

5. **UDFs for Custom Logic**:
   - Use UDFs for complex logic (e.g., categorizing data) to avoid moving data out of Snowflake.
   - Example:
     ```python
     @udf
     def categorize(amount: float) -> str:
         return "HIGH" if amount > 10000 else "LOW"
     df.withColumn("CATEGORY", categorize(col("AMOUNT")))
     ```

6. **Optimize Joins**:
   - Use `BROADCAST` hints for small dimension tables to optimize join performance.
   - Example:
     ```python
     df_joined = df.join(dim_df.hint("BROADCAST"), "ID")
     ```

7. **Stored Procedures**:
   - Encapsulate logic in stored procedures for reusability and performance.
   - Example:
     ```python
     session.sproc.register(my_pipeline, name="RUN_PIPELINE")
     ```

---

### **Interview Question 3: Explain how Snowflake’s architecture supports scalability.**

#### **Core Architectural Features**:
1. **Separation of Compute & Storage**:
   - **Storage**: Built on cloud object storage (AWS S3, Azure Blob, GCP Cloud Storage), scales infinitely.
   - **Compute**: Virtual Warehouses (clusters of compute resources) can be scaled up/down independently.

2. **Multi-Cluster Warehouses**:
   - Support high concurrency by dynamically adding clusters for workloads (e.g., separate warehouses for ETL and reporting).
   - Example:
     ```sql
     CREATE WAREHOUSE ELT_WH MAX_CLUSTER_COUNT = 5;
     ```

3. **Shared Data Architecture**:
   - All warehouses access the same data (no data duplication), ensuring consistency and reducing overhead.

4. **Automatic Scaling & Suspension**:
   - Warehouses auto-suspend after inactivity to save costs and scale based on query complexity.

5. **Zero-Copy Cloning**:
   - Instantly create clones of databases/tables for testing/development without duplicating storage.
   - Example:
     ```sql
     CREATE TABLE test_sales CLONE production_sales;
     ```

6. **Time Travel & Fail-Safe**:
   - Query historical data (up to 90 days) for recovery or auditing.
   - Example:
     ```sql
     SELECT * FROM sales AT (TIMESTAMP => '2023-12-31 23:59:59');
     ```

7. **Distributed Query Execution**:
   - Queries are parallelized across nodes in a warehouse for faster processing.

#### **Practical Implications**:
- **Scale Reads/Writes**: Add more warehouses or increase cluster count to handle concurrent ETL and analytics.
- **Cost Efficiency**: Pay only for compute used; storage scales automatically.
- **Performance**: Clustering keys and materialized views reduce data scanned for large datasets.

---

### **Summary**
| Question | Key Takeaway |
|---------|--------------|
| **Snowpark vs. SQL** | Snowpark enables programmatic data engineering with Python/Java, while SQL is ideal for declarative queries. |
| **Large-Scale Transformations** | Push down logic to Snowflake, use clustering, caching, and UDFs to avoid data movement. |
| **Scalability** | Snowflake scales via separation of compute/storage, multi-cluster warehouses, and distributed execution. |

By leveraging these principles, you can build **scalable, efficient data pipelines** in Snowflake, whether using **Snowpark** for complex transformations or **SQL** for ad-hoc analysis.

---

Here’s a structured guide to **design patterns** in data engineering, focusing on **modular code** and **DRY (Don’t Repeat Yourself)** principles, with examples in Python, Snowpark, SQL, and best practices for scalable pipelines.

---

### **1. Modular Code**
#### **What Is It?**
Breaking down code into **reusable, testable components** (functions, classes, modules) to simplify maintenance and promote reuse.

#### **Snowpark/Python Example**:
```python
# utils.py - Reusable utility functions
from snowflake.snowpark import Session

def create_session():
    connection_params = {
        "account": os.getenv("SNOWFLAKE_ACCOUNT"),
        "user": os.getenv("SNOWFLAKE_USER"),
        "password": os.getenv("SNOWFLAKE_PASSWORD"),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE")
    }
    return Session.builder.configs(connection_params).create()

def load_table(session: Session, table_name: str):
    return session.table(table_name)

# pipeline.py - Modular ETL steps
from utils import create_session, load_table

def transform_data(session: Session):
    raw_df = load_table(session, "RAW_DATA")
    cleaned_df = raw_df.filter(col("AMOUNT") > 0)
    return cleaned_df

def save_data(df, table_name: str):
    df.write.mode("overwrite").save_as_table(table_name)

def run_pipeline():
    session = create_session()
    try:
        cleaned = transform_data(session)
        save_data(cleaned, "CLEANED_DATA")
    finally:
        session.close()
```

#### **SQL Example**:
```sql
-- Reusable Views
CREATE OR REPLACE VIEW active_users AS
SELECT * FROM users WHERE status = 'active';

-- Stored Procedure for Aggregation
CREATE OR REPLACE PROCEDURE aggregate_sales()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
  CREATE OR REPLACE TABLE sales_summary AS
  SELECT region, SUM(amount) AS total
  FROM sales
  GROUP BY region;
END;
$$;
```

---

### **2. DRY Principles**
#### **What Is It?**
Avoid duplicating logic by **abstracting common patterns** into reusable components.

#### **Examples of DRY in Practice**:
##### **A. Parameterized Queries**
Avoid hardcoding values:
```python
# Snowpark Example
def filter_by_region(session: Session, region: str):
    return session.table("SALES").filter(col("REGION") == region)

na_sales = filter_by_region(session, "NA")
eu_sales = filter_by_region(session, "EU")
```

```sql
-- SQL Example
CREATE OR REPLACE PROCEDURE filter_sales(region STRING)
RETURNS TABLE (...)
LANGUAGE SQL
AS
$$
  SELECT * FROM sales WHERE region = :region;
$$;
```

##### **B. Configuration Files**
Store reusable parameters (e.g., warehouse sizes, file paths):
```yaml
# config.yaml
snowflake:
  warehouse:
    etl_wh: xsmall
    reporting_wh: medium
```

```python
import yaml

with open("config.yaml") as f:
    config = yaml.safe_load(f)

session.sql(f"ALTER WAREHOUSE {config['snowflake']['warehouse']['etl_wh']} RESUME").collect()
```

##### **C. Common Transformation Logic**
Abstract repeated logic into functions:
```python
# transforms.py
from snowflake.snowpark.functions import col, when

def add_category_flag(df, column: str, threshold: float):
    return df.withColumn(
        f"{column}_category",
        when(col(column) > threshold, "HIGH").otherwise("LOW")
    )
```

```python
# pipeline.py
from transforms import add_category_flag

df = add_category_flag(cleaned_df, "AMOUNT", 1000)
```

##### **D. Template-Based SQL**
Use templating engines (e.g., Jinja) for dynamic SQL:
```jinja
-- template.sql.j2
SELECT 
  {{ group_by_column }},
  SUM({{ metric_column }}) AS total_{{ metric_column }}
FROM {{ table_name }}
GROUP BY {{ group_by_column }};
```

```python
# Render template dynamically
template = Template(open("template.sql.j2").read())
query = template.render(
    group_by_column="REGION",
    metric_column="AMOUNT",
    table_name="SALES"
)
session.sql(query).collect()
```

---

### **3. Design Patterns for Data Pipelines**
#### **A. Strategy Pattern**  
Switch between different transformation logic:
```python
class DataTransformer:
    def __init__(self, strategy):
        self.strategy = strategy

    def transform(self, df):
        return self.strategy.transform(df)

class FilterStrategy:
    def transform(self, df):
        return df.filter(col("AMOUNT") > 0)

class EnrichStrategy:
    def transform(self, df):
        return df.withColumn("TAX", col("AMOUNT") * 0.1)
```

#### **B. Pipeline Pattern**  
Chain transformations declaratively:
```python
class Pipeline:
    def __init__(self, steps):
        self.steps = steps

    def run(self, df):
        for step in self.steps:
            df = step(df)
        return df

pipeline = Pipeline([
    lambda df: df.filter(col("AMOUNT") > 0),
    lambda df: df.withColumn("TAX", col("AMOUNT") * 0.1)
])
result = pipeline.run(raw_df)
```

#### **C. Factory Pattern**  
Abstract data source creation:
```python
class DataSourceFactory:
    @staticmethod
    def get_source(type_, session):
        if type_ == "snowflake":
            return SnowflakeSource(session)
        elif type_ == "parquet":
            return ParquetSource(session)

class SnowflakeSource:
    def read(self, table):
        return session.table(table)

class ParquetSource:
    def read(self, path):
        return session.read.parquet(path)
```

---

### **4. Best Practices**
| Area               | Practice                                                                 |
|--------------------|--------------------------------------------------------------------------|
| **Code Reuse**     | Extract common logic into functions, classes, or stored procedures.      |
| **Configuration**  | Use YAML/JSON for environment-specific settings (dev/stage/prod).        |
| **Error Handling** | Centralize logging and exception handling in base modules.               |
| **Testing**        | Write unit tests for individual components (e.g., `pytest` for Python).  |
| **Documentation**  | Document reusable modules with examples and usage guidelines.            |

---

### **5. Real-World Integration**
#### **Orchestration with Airflow**:
```python
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

def run_etl():
    from pipeline import run_pipeline
    run_pipeline()

dag = DAG("daily_etl", schedule_interval="@daily", start_date=datetime(2023, 1, 1))
PythonOperator(task_id="run_pipeline", python_callable=run_etl, dag=dag)
```

#### **Snowflake Tasks**:
```sql
-- Schedule via Snowflake Task
CREATE TASK daily_transform
  WAREHOUSE = etl_wh
  SCHEDULE = 'USING CRON 0 8 * * * UTC'
AS
  CALL SYSTEM$EXECUTE_PYTHON_SCRIPT('pipeline_script.py');
```

---

### **6. Summary Table**
| Pattern           | Use Case                                | Example Tools/Techniques                |
|-------------------|-----------------------------------------|------------------------------------------|
| **Modular Code**  | Break pipelines into reusable components  | Functions, classes, stored procedures    |
| **DRY Principles**| Avoid duplication with abstraction        | Templates, config files, UDFs            |
| **Strategy**      | Switch between transformation logic       | Strategy pattern in Python               |
| **Pipeline**      | Chain transformations declaratively       | Function chaining, DAGs in Airflow       |
| **Factory**       | Abstract data source creation             | Factory pattern for Snowflake/Parquet    |

By applying these patterns, you’ll build **scalable, maintainable data pipelines** that reduce redundancy, improve readability, and simplify debugging—whether using **Snowpark**, **SQL**, or hybrid workflows. Always combine with Snowflake’s native features like **clustering**, **materialized views**, and **result caching** for optimal performance.

---

Here’s a structured guide to **Git version control** for data engineering workflows, focusing on essential commands like **commit**, **branch**, **merge**, and best practices for collaboration and code management.

---

### **1. Basic Git Workflow for Data Engineering**
#### **Initialize & Stage Changes**
```bash
# Initialize a new Git repository
git init

# Clone an existing repository
git clone https://github.com/your-repo.git

# Check status of tracked/untracked files
git status

# Add specific files to the staging area
git add pipeline.py utils.py

# Add all changes to staging
git add .
```

#### **Commit Changes**
```bash
# Commit staged changes with a descriptive message
git commit -m "Refactor Snowpark ETL script for performance"

# Stage and commit in one step (for tracked files only)
git commit -am "Fix bug in data transformation logic"
```

#### **View History**
```bash
# View commit history
git log

# View changes in the last commit
git log -p

# Show a compact one-line history
git log --oneline
```

---

### **2. Branching & Merging**
#### **Create & Switch Branches**
```bash
# Create a new branch (e.g., for a feature or fix)
git branch feature/snowpark-optimization

# Switch to a branch
git checkout feature/snowpark-optimization

# Create and switch in one command
git checkout -b feature/snowpark-optimization
```

#### **Merge Branches**
```bash
# Merge a branch into the current branch
git checkout main
git merge feature/snowpark-optimization

# Resolve merge conflicts if any (see below)
```

#### **Delete Branches**
```bash
# Delete a merged branch
git branch -d feature/snowpark-optimization

# Force delete an unmerged branch
git branch -D feature/snowpark-optimization
```

---

### **3. Handling Merge Conflicts**
When two branches modify the same file:
```bash
# Example conflict markers in a file:
<<<<<<< HEAD
    # Code from main branch
=======
    # Code from feature branch
>>>>>>> feature/snowpark-optimization
```

#### **Steps to Resolve**:
1. Open conflicting files and manually edit to keep desired changes.
2. Mark conflicts as resolved:
   ```bash
   git add conflicted_file.py
   ```
3. Complete the merge:
   ```bash
   git commit
   ```

---

### **4. Remote Repositories (GitHub/GitLab)**
#### **Link & Push to Remote**
```bash
# Add a remote repository
git remote add origin https://github.com/your-username/your-repo.git

# Push local branch to remote
git push origin feature/snowpark-optimization

# Push all branches
git push --all origin
```

#### **Pull Changes from Remote**
```bash
# Fetch and merge changes from remote branch
git pull origin main

# Fetch without merging
git fetch origin
```

#### **Sync Forked Repositories**
```bash
# Add upstream remote (for forked repos)
git remote add upstream https://github.com/original-owner/original-repo.git

# Fetch upstream changes
git fetch upstream

# Merge upstream changes into main
git checkout main
git merge upstream/main
```

---

### **5. Advanced Git Commands**
#### **Amend Last Commit**
```bash
# Fix a mistake in the last commit
git commit --amend -m "Updated commit message"
```

#### **Stash Changes**
```bash
# Temporarily save uncommitted changes
git stash

# Apply stashed changes
git stash apply
```

#### **Rebase vs. Merge**
```bash
# Rebase current branch onto another (linear history)
git checkout feature/snowpark-optimization
git rebase main

# Resolve conflicts during rebase
git rebase --continue
```

#### **Tagging Releases**
```bash
# Create a tag for a release
git tag v1.0.0

# Push tags to remote
git push origin v1.0.0
```

---

### **6. Best Practices for Data Engineering**
| Practice              | Description                                                                 |
|-----------------------|-----------------------------------------------------------------------------|
| **Atomic Commits**    | Commit small, logical changes (e.g., "Fix Snowpark UDF for salary ranges"). |
| **Descriptive Messages** | Use clear, concise messages (e.g., "Optimize ETL pipeline for Snowflake"). |
| **Branch per Feature** | Isolate work in branches (e.g., `feature/data-transformation`).            |
| **Code Reviews**      | Use pull requests for peer review before merging to main.                   |
| **.gitignore**        | Exclude large files (e.g., datasets, logs, virtual environments):          |
  ```bash
  # .gitignore
  *.csv
  *.parquet
  __pycache__
  .env
  ```
| **CI/CD Integration** | Automate testing (e.g., unit tests for Snowpark scripts).                  |

---

### **7. Real-World Example: Data Engineering Workflow**
#### **Scenario**:  
You’re working on optimizing a Snowpark ETL script (`etl_script.py`) and need to collaborate with a teammate.

#### **Steps**:
1. **Create a Feature Branch**:
   ```bash
   git checkout -b feature/optimize-snowpark
   ```

2. **Make Changes**:
   - Modify `etl_script.py` to add clustering keys.
   - Test locally.

3. **Commit & Push**:
   ```bash
   git add etl_script.py
   git commit -m "Add clustering keys for Snowpark ETL"
   git push origin feature/optimize-snowpark
   ```

4. **Open Pull Request**:
   - Submit a PR on GitHub/GitLab for review.

5. **Resolve Conflicts**:
   - If conflicts arise with `main`, merge and resolve:
     ```bash
     git checkout feature/optimize-snowpark
     git merge main
     # Resolve conflicts and complete merge
     ```

6. **Merge to Main**:
   - After approval, merge the PR and delete the feature branch.

---

### **8. Key Takeaways**
- **Version control** is essential for tracking changes in code, especially for collaborative data engineering projects.
- Use **branches** to isolate work and **merge** only after testing.
- Follow **best practices** like atomic commits and `.gitignore` to keep repositories clean.
- Integrate with **remote repositories** (e.g., GitHub) for collaboration and **CI/CD** pipelines.

By mastering these Git commands and workflows, you’ll streamline collaboration, reduce errors, and maintain a clean history of changes in your data engineering projects.

---

Here’s a structured guide to **testing Python code** (using `unittest` and `pytest`) and **Agile methodology** (sprints, stand-ups, backlog grooming), tailored for **data engineering** workflows with practical examples and best practices.

---

### **1. Testing Python Code (unittest vs. pytest)**

#### **Why Testing Matters in Data Engineering**
- Ensures data pipelines, transformations, and integrations work as expected.
- Prevents regressions when modifying code (e.g., Snowpark scripts, ETL logic).
- Validates data quality (e.g., null checks, schema validation).

---

#### **A. `unittest`: Built-in Python Testing Framework**
**Example Test Case**:
```python
# test_data_transformations.py
import unittest
from transformations import clean_data, calculate_total_sales

class TestDataTransformations(unittest.TestCase):
    def setUp(self):
        """Set up test data before each test."""
        self.raw_data = [
            {"id": 1, "amount": 100, "status": "completed"},
            {"id": 2, "amount": None, "status": "pending"},
        ]

    def test_clean_data_removes_nulls(self):
        cleaned = clean_data(self.raw_data)
        self.assertEqual(len(cleaned), 1)  # Only 1 valid row

    def test_calculate_total_sales(self):
        total = calculate_total_sales(self.raw_data)
        self.assertEqual(total, 100)  # Only 1 valid amount

if __name__ == "__main__":
    unittest.main()
```

**Run Tests**:
```bash
python -m unittest test_data_transformations.py -v
```

**Key Features**:
- `setUp()` and `tearDown()` for setup/cleanup.
- Built-in assertions (`assertEqual`, `assertTrue`, `assertRaises`).
- Organized test classes and methods.

---

#### **B. `pytest`: Lightweight Third-Party Framework**
**Example Test Case**:
```python
# test_data_transformations.py
import pytest
from transformations import add_tax, validate_schema

def test_add_tax():
    assert add_tax(100) == 110  # 10% tax
    assert add_tax(0) == 0

def test_validate_schema_passes():
    data = {"id": 1, "name": "Product A"}
    assert validate_schema(data) is True

def test_validate_schema_fails():
    data = {"id": "invalid", "name": 123}
    with pytest.raises(ValueError):
        validate_schema(data)
```

**Run Tests**:
```bash
pytest test_data_transformations.py -v
```

**Advantages**:
- Simple syntax (no need for `self.assert...`).
- Rich ecosystem (plugins for coverage, parameterized tests, etc.).
- Supports fixtures for reusable setup.

**Fixtures Example**:
```python
# conftest.py (shared fixtures)
import pytest

@pytest.fixture
def sample_data():
    return [
        {"id": 1, "amount": 100},
        {"id": 2, "amount": None},
    ]
```

```python
# test_transformations.py
def test_clean_data(sample_data):
    cleaned = clean_data(sample_data)
    assert len(cleaned) == 1
```

---

#### **C. Best Practices for Testing Data Pipelines**
| Practice | Description |
|--------|-------------|
| **Test Coverage** | Use `coverage.py` to measure coverage: `coverage run -m pytest && coverage report`. |
| **Mock External Dependencies** | Use `unittest.mock` to simulate Snowflake queries or API calls. |
| **Parameterized Tests** | Test edge cases (e.g., empty datasets, invalid formats). |
| **Integration Tests** | Test full pipelines (e.g., end-to-end Snowpark workflows). |
| **CI/CD Integration** | Automate tests in pipelines (e.g., GitHub Actions, Airflow). |

---

### **2. Agile Methodology for Data Engineering**

#### **Why Agile Works for Data Projects**
- **Iterative Development**: Break large tasks (e.g., building a data lake) into smaller, deliverable chunks.
- **Collaboration**: Cross-functional teamwork between data engineers, analysts, and product owners.
- **Flexibility**: Adapt to changing requirements (e.g., new data sources, schema updates).

---

#### **A. Key Agile Concepts**
| Concept | Description | Example in Data Engineering |
|-------|-------------|-----------------------------|
| **Sprints** | Time-boxed work periods (1–4 weeks). | Build a Snowpark ETL pipeline in 2 weeks. |
| **Backlog** | Prioritized list of tasks. | Tasks: "Ingest CSV data," "Optimize query performance." |
| **Stand-ups** | Daily 15-min meetings to sync progress. | Discuss blockers like slow data ingestion. |
| **Sprint Planning** | Team selects tasks for the sprint. | Prioritize "Build data warehouse schema" over "Add documentation." |
| **Retrospective** | Review what went well and what to improve. | "We spent too much time debugging; add unit tests next sprint." |
| **User Stories** | Define requirements from the user’s perspective. | "As an analyst, I want cleaned sales data to generate reports." |

---

#### **B. Agile Ceremonies in Practice**
1. **Daily Stand-up**:
   - Each member answers:
     - What did I do yesterday?
     - What will I do today?
     - Are there any blockers?

2. **Sprint Planning**:
   - Product Owner prioritizes the backlog.
   - Team estimates effort (e.g., story points) and commits to tasks.

3. **Backlog Grooming**:
   - Refine and prioritize tasks.
   - Split large tasks (e.g., "Migrate all ETL to Snowpark" → "Migrate sales ETL," "Migrate inventory ETL").

4. **Sprint Review/Demo**:
   - Show completed work (e.g., a new data pipeline or dashboard).

5. **Retrospective**:
   - Reflect on processes and identify improvements.

---

#### **C. Tools for Agile in Data Engineering**
| Tool | Use Case |
|------|----------|
| **Jira** | Track user stories, bugs, and tasks. |
| **Trello** | Visualize backlog and sprint progress (Kanban board). |
| **Confluence** | Document technical designs and sprint plans. |
| **Slack/Microsoft Teams** | Daily communication and stand-ups. |
| **Azure DevOps** | Integrate Agile with CI/CD pipelines. |

---

#### **D. Best Practices for Agile Data Teams**
| Practice | Description |
|--------|-------------|
| **Small, Incremental Deliverables** | Focus on delivering value early (e.g., one pipeline per sprint). |
| **Collaborative Backlog Grooming** | Involve engineers, analysts, and stakeholders. |
| **Automate Testing in Sprints** | Include unit/integration tests in sprint goals. |
| **Cross-Functional Teams** | Pair data engineers with analysts to align priorities. |
| **Continuous Feedback** | Demo progress to stakeholders every sprint. |

---

### **3. Summary Table**
| Topic | Key Takeaways |
|-------|---------------|
| **Testing** | Use `unittest` or `pytest` to validate data transformations; automate tests in CI/CD. |
| **Agile** | Use sprints for iterative development, daily stand-ups for communication, and backlog grooming for prioritization. |
| **Data Engineering Fit** | Combine testing with Agile to build robust pipelines iteratively (e.g., test-driven development + sprint planning). |

By applying **unit testing** and **Agile principles**, you’ll ensure your data pipelines are reliable, maintainable, and aligned with business needs. Always adapt Agile practices to fit the unique challenges of data projects (e.g., handling evolving schemas or large-scale data).

---

### **1. Unit Test for a Python Function**  
**Objective**: Write a unit test for a Python function that calculates total sales after tax using `pytest`.  

#### **Code Example**  
```python
# transformations.py
def calculate_total_sales(items, tax_rate=0.1):
    """
    Calculate total sales after tax.
    
    Args:
        items: List of item prices (floats).
        tax_rate: Tax percentage (default 10%).

    Returns:
        Total sales amount after tax.
    """
    if not isinstance(items, list) or any(not isinstance(i, (int, float)) for i in items):
        raise ValueError("Items must be a list of numbers.")
    subtotal = sum(items)
    return subtotal * (1 + tax_rate)
```

```python
# test_transformations.py
import pytest
from transformations import calculate_total_sales

def test_calculate_total_sales():
    # Test valid input
    assert calculate_total_sales([100, 200, 300], 0.1) == 660.0  # 600 + 10% tax
    
    # Test empty list
    assert calculate_total_sales([], 0.1) == 0.0

    # Test invalid input
    with pytest.raises(ValueError):
        calculate_total_sales("not a list", 0.1)

    with pytest.raises(ValueError):
        calculate_total_sales([100, "invalid"], 0.1)
```

**Run Tests**:  
```bash
pytest test_transformations.py -v
```

---

### **2. Git Workflow for a Team Project**  
**Agile-Inspired Git Workflow** for collaborative data engineering projects.  

#### **Steps**:  
1. **Main Branches**:  
   - `main`: Production-ready code.  
   - `develop`: Latest development changes.  

2. **Feature Branches**:  
   - Create a branch for each feature/fix:  
     ```bash
     git checkout -b feature/clean-data develop
     ```

3. **Development**:  
   - Commit small, logical changes:  
     ```bash
     git add clean_data.py
     git commit -m "Add null handling in data cleaning function"
     ```

4. **Push to Remote**:  
   ```bash
   git push origin feature/clean-data
   ```

5. **Pull Request (PR)**:  
   - Open a PR on GitHub/GitLab for code review.  
   - Link to Agile user story (e.g., "As an analyst, I need cleaned data...").  

6. **Code Review & Merge**:  
   - Team reviews PR, suggests changes.  
   - Merge into `develop` after approval:  
     ```bash
     git checkout develop
     git merge --no-ff feature/clean-data
     ```

7. **Sprint Release**:  
   - Merge `develop` into `main` at the end of a sprint:  
     ```bash
     git checkout main
     git merge --no-ff develop
     git tag v1.0.0  # Tag release version
     ```

#### **Best Practices**:  
- **Atomic Commits**: Commit related changes together.  
- **.gitignore**: Exclude datasets, logs, and virtual environments.  
- **CI/CD Integration**: Automate testing on PRs (e.g., GitHub Actions).  

---

### **3. Design a Simple REST API for a Data Endpoint Using Flask**  
**Objective**: Build a Flask API for retrieving and adding sales data.  

#### **Code Example**  
```python
# app.py
from flask import Flask, request, jsonify

app = Flask(__name__)

# In-memory sales data
sales_data = [
    {"id": 1, "region": "NA", "amount": 1000},
    {"id": 2, "region": "EU", "amount": 1500},
]

@app.route("/sales", methods=["GET"])
def get_sales():
    region = request.args.get("region")
    if region:
        filtered = [s for s in sales_data if s["region"].lower() == region.lower()]
        return jsonify(filtered)
    return jsonify(sales_data)

@app.route("/sales", methods=["POST"])
def add_sale():
    new_sale = request.get_json()
    required_fields = ["id", "region", "amount"]
    if not all(f in new_sale for f in required_fields):
        return jsonify({"error": "Missing required fields"}), 400
    sales_data.append(new_sale)
    return jsonify({"message": "Sale added"}), 201

if __name__ == "__main__":
    app.run(debug=True)
```

#### **Test the API**  
- **Get Sales**:  
  ```bash
  curl http://localhost:5000/sales?region=NA
  ```

- **Add Sale**:  
  ```bash
  curl -X POST http://localhost:5000/sales \
       -H "Content-Type: application/json" \
       -d '{"id": 3, "region": "APAC", "amount": 2000}'
  ```

#### **Best Practices**:  
- **Validation**: Check input data (e.g., required fields).  
- **Error Handling**: Return appropriate HTTP status codes.  
- **Documentation**: Use Swagger/OpenAPI for API docs.  
- **Security**: Add authentication (e.g., Flask-JWT).  

---

### **Summary**  
| Task | Key Takeaways |  
|------|---------------|  
| **Unit Testing** | Use `pytest` for simple, parameterized tests; validate inputs and edge cases. |  
| **Git Workflow** | Follow feature branching, PR reviews, and sprint-based releases for collaboration. |  
| **Flask API** | Design RESTful endpoints with clear routes, validation, and error handling. |  

By applying these practices, you’ll ensure **reliable code**, **collaborative efficiency**, and **scalable data APIs** in data engineering workflows.

---

