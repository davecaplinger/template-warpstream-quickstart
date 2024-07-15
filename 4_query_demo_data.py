import duckdb
import os
from dotenv import load_dotenv

load_dotenv()
con = duckdb.connect("stats.db") # Connect to a persisted DuckDB database on the filesystem

tablename = os.getenv("db_table_name","page_actions")
queryres = con.execute(f'''
        SELECT * FROM  {tablename}
        ORDER BY page_id ASC;
    ''').fetchdf()

print("---UPDATED COUNTS---")
print(queryres)
