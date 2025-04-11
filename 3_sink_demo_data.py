import duckdb
import logging
import os
from quixstreams import Application
from quixstreams.kafka.configuration import ConnectionConfig
from dotenv import load_dotenv

load_dotenv() # for local dev, load env vars from a .env file
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

logger.info("Connecting to WarpStream...")
# Define your SASL configuration
connection = ConnectionConfig(
     bootstrap_servers=os.environ["bootstrap_server"],
     security_protocol=os.environ.get("security_protocol","SASL_SSL"),  # or "SASL_SSL" if using SSL
     sasl_mechanism=os.environ.get("sasl_mechanism", "PLAIN"),  # or any other supported mechanism
     sasl_username=os.environ.get("sasl_username", None),
     sasl_password=os.environ.get("sasl_password", None)
)

# Initialize the Quix Application with the connection configuration
app = Application(consumer_group="warpstream-consumer-v1e",
                  broker_address=connection,
                  auto_offset_reset="earliest")

input_topic = app.topic(os.getenv("processed_data_topic","processed_data")) # Define the input topic to consume from
tablename = os.getenv("db_table_name","page_actions") # The name of the table we want to write to
sdf = app.dataframe(input_topic) # Turn the data from the input topic into a streaming dataframe

con = duckdb.connect("stats.db") # Connect to a persisted DuckDB database on the filesystem

try:
    # Do a basic check if the target table exists and create it if not
    table_exists = con.execute(
        f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '\"{tablename}\"')").fetchone()[0]
    if not table_exists:
        con.execute(f'''
            CREATE TABLE "{tablename}" (
                page_id VARCHAR UNIQUE,
                count INTEGER
            );
        ''')
except duckdb.CatalogException as e:
     # If basic check failed, check for catalog error as a backup
    if "already exists" in str(e):
        print(f"Table '{tablename}' already exists, skipping creation.")
    else:
        raise  # Re-raise the exception if it's not about table existence

def insert_data(con, msg):
    # Insert data into the DB and if the page_id exists, update the count in the existing row
    con.execute(f'''
        INSERT INTO {tablename}(page_id, count) VALUES (?, ?)
        ON CONFLICT (page_id)
        DO UPDATE SET count = excluded.count;
        ''', (msg['page_id'], msg['action_count']))
    logger.info(f"Wrote record: {msg}")

sdf = sdf.update(lambda val: insert_data(con, val), stateful=False)

app.run(sdf)
