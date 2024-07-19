
# Quix Streams

This page describes how to use the Quix Streams Python library to read and aggregate data from WarpStream and ingest the aggregations into a local DuckDB database for offline querying.

Quix Streams is a cloud native library for processing data in Kafka using pure Python. It’s designed to give you the power of a distributed system in a lightweight library by combining the low-level scalability and resiliency features of Kafka with an easy to use Python interface.


## Prerequisites

* A WarpStream account - get access to WarpStream by registering on the [sign-up page.](https://console.warpstream.com/signup).
* A WarpStream cluster is up and running.
* Installations of Git and Python with the required Python modules: 
    * Quix Streams
    * DuckDB
    * python-dotenv

We recommend setting up a Python virtual environment before you install the required python Python modules. 

You can install the dependencies with the following command:
  
```bash
pip install quixstreams duckdb python-dotenv numpy pandas
```

## Step 1: Clone the demo repo 

The Quix team has prepared a demo repo to help you get started with Quix Streams and WarpStream. 

Open a terminal window, clone the following repo.

```bash
git clone https://github.com/quixio/template-warpstream-quickstart.git
cd template-warpstream-quickstart
```

The repo contains the following Python files that represent some basic steps in a data pipeline.

* `1_produce_demo_data.py`
* `2_process_demo_data.py`
* `3_sink_demo_data.py`
* `4_query_demo_data.py`


## Step 2: Create an environment file

Create a `.env` file to store the required environment variables. You can use the `.env_example` file in the repo as a reference or run the following command in a bash shell:

```bash
cat <<EOL > .env
sasl_username="<your_warpstream_username"
sasl_password="<your_warpstream_password"
bootstrap_server="serverless.warpstream.com:9092"
raw_data_topic="raw-page-actions"
processed_data_topic="page-action-counts"
consumer_group_name="warpstream-consumer-v1"
db_table_name="page_action_counts"
EOL

```

## Step 3: Produce some records

Use a Quix Streams producer application to continuously produce some demo data to a topic. When you run the application for the first time, Quix Streams ensures that the required topics are automatically created if they don’t exist already.

Open a terminal window inside the repo directory and run the following command:

`python 1_process_demo_data.py`

You should see some output that resembles the following example:

```
[2024-07-15 16:55:26,134] [INFO] [quixstreams] : Topics required for this application: "raw-page-actions"
[2024-07-15 16:55:27,516] [INFO] [quixstreams] : Validating Kafka topics exist and are configured correctly...
[2024-07-15 16:55:30,730] [INFO] [quixstreams] : Kafka topics validation complete
INFO:__main__:Publishing row: {"timestamp": 1721055330, "user_id": "user_47", "page_id": "page_2", "action": "scroll"}
INFO:__main__:Publishing row: {"timestamp": 1721055331, "user_id": "user_42", "page_id": "page_3", "action": "scroll"}
INFO:__main__:Publishing row: {"timestamp": 1721055332, "user_id": "user_24", "page_id": "page_9", "action": "click"}
```

Here’s an important excerpt from the code you just ran:
```python
from quixstreams import Application
from quixstreams.kafka.configuration import ConnectionConfig

# Code truncated for brevity - see the source file for the full code: https://github.com/quixio/template-warpstream-quickstart/blob/main/1_produce_demo_data.py
...

connection = ConnectionConfig(
     bootstrap_servers=os.environ["bootstrap_server"],
     security_protocol="SASL_SSL",
     sasl_mechanism="PLAIN",  # or any other supported mechanism
     sasl_username=os.environ["sasl_username"],
     sasl_password=os.environ["sasl_password"]
 )

# Initialize the Quix Application with the connection configuration
app = Application(broker_address=connection)
topic = app.topic(os.environ["raw_data_topic"])
# for more help using QuixStreams see docs: https://quix.io/docs/quix-streams/introduction.html

...
           logger.info(f"Publishing row: {json_data}")
            producer.produce(
                topic=topic.name,
                key=str(uuid.uuid4()),
                value=json_data, # Data is generated automatically with a generator function (see source file)
            )

```

The previous code initializes a connection to a serverless WarpStream cluster using the credentials defined in your `.env` file. Once a connection has been established, it continuously generates some synthetic page view data and produces it to a Kafka topic (the name of this topic is defined in the "raw_data_topic" environment variable).

Keep the producer terminal window open, and open a second terminal window.


## Step 4: Process the records

As you can see in the producer output, the synthetic data represents a very basic user activity log for a website. Suppose that you want to see the most active pages.  To do so, you could continuously count the number of user actions (regardless of type) and aggregate the counts by page.

You can achieve this with a simple Quix Streams processor application. 

To see it it action, run the following command in a second terminal window
`python 2_process_demo_data.py`

You should see some output that resembles the following example:
```
[2024-07-15 16:57:06,413] [INFO] [quixstreams] : Validating Kafka topics exist and are configured correctly...
[2024-07-15 16:57:09,619] [INFO] [quixstreams] : Kafka topics validation complete
[2024-07-15 16:57:09,620] [INFO] [quixstreams] : Initializing state directory at ".\state\warpstream-consumer-v1"
[2024-07-15 16:57:09,632] [INFO] [quixstreams] : Waiting for incoming messages
Received row: {'action_count': 1, 'page_id': 'page_3'}
Received row: {'action_count': 1, 'page_id': 'page_6'}
Received row: {'action_count': 2, 'page_id': 'page_3'}
```

Here are the important excerpts from the code:

```python
# Code truncated for brevity - see the source file for the full code: https://github.com/quixio/template-warpstream-quickstart/blob/main/2_process_demo_data.py
...
sdf = app.dataframe(input_topic)
sdf = sdf.group_by("page_id")

# Continuously count the number of actions by page
def count_messages(value: dict, state: State):
    current_total = state.get('action_count', default=0)
    current_total += 1
    state.set('action_count', current_total)
    return current_total

# Add the page_id into the payload
def add_key_to_payload(value, key, timestamp, headers):
    value['page_id'] = key
    return value

# Apply a custom function and inform StreamingDataFrame to provide a State instance to it using "stateful=True"
sdf["action_count"] = sdf.apply(count_messages, stateful=True)
sdf = sdf[["action_count"]]
sdf = sdf.apply(add_key_to_payload, metadata=True) # Adding the key (page_id) to the payload for better visibility
sdf = sdf.update(lambda row: print(f"Received row: {row}"))
sdf = sdf.to_topic(output_topic)
...

```

The line `'sdf = sdf.group_by("page_id")`’—this line rekeys the stream so that the messages are keyed by the page_id. The `count_messages` function then keeps a running total of the action counts. 

Under the hood, it is maintaining an entry for each message key (in this case page_id) in state, and updating the relevant action count whenever a new message is processed. This means that if the process is interrupted or restarted, it can pick up from where it left off. In essence, the state store serves a similar role to a lightweight database.

This repo includes a sample file [state_example.json](./state_example.json), to give you an idea what the state would look like for this example.

The aggregated results are then written to a second downstream topic using the` to_topic` function.

Keep the processor terminal window open, and open a third window


## Step 4: Sink the data to DuckDB

Although the state store is kind of like a database, you can’t query it (yet)—queryable state is on the Quix Streams roadmap however. In the meantime, you can sink the data to a “proper” lightweight database such as DuckDB and query it there. This is a useful exercise anyway, because sinking data to an external destination is a very common use case. 

For example, suppose that you want to build a Streamlit dashboard that charts the most active pages. It would be unwise to connect Streamlit directly to Kafka (using consumer code) because the data updates too frequently. Instead, you can sink the streaming data to a database first and have Streamlit poll the database at regular intervals.

To see how data sinking works, run our example application with the following command:
`python 3_sink_demo_data.py`

You should see some output that resembles the following example:
```
[2024-07-15 16:59:59,211] [INFO] [quixstreams] : Validating Kafka topics exist and are configured correctly...
[2024-07-15 17:00:02,517] [INFO] [quixstreams] : Kafka topics validation complete
[2024-07-15 17:00:02,517] [INFO] [quixstreams] : Initializing state directory at ".\state\warpstream-consumer-v1e"
[2024-07-15 17:00:02,529] [INFO] [quixstreams] : Waiting for incoming messages
INFO:__main__:Wrote record: {'action_count': 1, 'page_id': 'page_3'}
INFO:__main__:Wrote record: {'action_count': 1, 'page_id': 'page_6'}
INFO:__main__:Wrote record: {'action_count': 2, 'page_id': 'page_3'}
INFO:__main__:Wrote record: {'action_count': 1, 'page_id': 'page_2'}
```

Again, here are the important excerpts from the code:

```python
import duckdb
...
# Code truncated for brevity - see the source file for the full code: https://github.com/quixio/template-warpstream-quickstart/blob/main/3_sink_demo_data.py
...
tablename = os.getenv("db_table_name","page_actions") # The name of the table we want to write to
sdf = app.dataframe(input_topic) # Turn the data from the input topic into a streaming dataframe
con = duckdb.connect("stats.db") # Connect to a persisted DuckDB database on the filesystem
...
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
...
```

After initializing a consumer and a local DuckDB database, Quix Streams runs an INSERT query on every message received. It  updates the counts for existing page_ids and adds rows for new page_ids.

After the process runs for about 10 seconds and enough records have been inserted, terminate the process. 


## Step 5: Query the database

Now that you have the aggregates in a database, you can run a simple “SELECT ALL” query. You don’t have to worry about managing the aggregations as part of a query on the raw data. 

Note that, a local file-based DuckDB database cannot handle synchronous reads and writes, so make sure that the sink process has stopped before you query the database.

To see the aggregated page action counts, run the following command:
`python 4_query_demo_data.py`

You should see some output that resembles the following example:
```
---UPDATED COUNTS---
    page_id  count
0    page_0    181
1    page_1    148
2    page_2    172
3    page_3    172
4    page_4    180
5    page_5    180
6    page_6    194
7    page_7    165
8    page_8    159
9    page_9    164

```

Here’s the full query code.

```python
import duckdb
import os
from dotenv import load_dotenv

load_dotenv()
con = duckdb.connect("stats.db") # Connect to the persisted DuckDB database on the filesystem

tablename = os.getenv("db_table_name","page_actions")
queryres = con.execute(f'''
        SELECT * FROM  {tablename}
        ORDER BY page_id ASC;
    ''').fetchdf()

print("---UPDATED COUNTS---")
print(queryres)
```

## Next Steps

Well done! You've set up the key components of a Python-based stream processing pipeline using WarpStream as a broker and Quix Streams as a Kafka producer, consumer, and stream processor. 

You can then scale your pipeline by running containerized applications in any container orchestration platform from one of the big cloud providers, but this can add a lot of complexity. The simplest way to scale a pipeline is to run your applications in [Quix Cloud](https://quix.io/product). The Quix Cloud platform integrates seamlessly with the Quix Streams Python library and includes a lot of extra features that you would not get with a more generic product such as AWS Lambda or Google Cloud Run.

For example, you get a interactive visualization of your pipeline which makes it easy to inspect the data flowing through it.

![quix-pipeline-example](https://github.com/user-attachments/assets/19d58d70-c44b-4252-acf8-dcece52b5d9f)

To try Quix Cloud, visit the [sign-up page](https://portal.platform.quix.io/) and start a free trial.

Next, check out the WarpStream docs for configuring the [WarpStream Agent](https://docs.warpstream.com/warpstream/configuration/deploy), or review the [Quix docs](https://quix.io/docs/quix-streams/introduction.html) to learn more about what you can do with the Quix Streams.
