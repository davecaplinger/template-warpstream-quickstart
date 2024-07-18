import os
from quixstreams import Application, State
from quixstreams.kafka.configuration import ConnectionConfig
from dotenv import load_dotenv

# for local dev, load env vars from a .env file
load_dotenv()

# Define your SASL configuration
connection = ConnectionConfig(
     bootstrap_servers=os.environ["bootstrap_server"],
     security_protocol="SASL_SSL",
     sasl_mechanism="PLAIN",  # or any other supported mechanism
     sasl_username=os.environ["sasl_username"],
     sasl_password=os.environ["sasl_password"]
 )

# Initialize the Quix Application with the connection configuration
app = Application(consumer_group=os.getenv("consumer_group_name","default-consumer-group"),
                  broker_address=connection,
                  auto_offset_reset="earliest")

input_topic = app.topic(os.getenv("raw_data_topic","raw_data"))
output_topic = app.topic(os.getenv("processed_data_topic","processed_data"))
sdf = app.dataframe(input_topic)

sdf = sdf.group_by("page_id")

def count_messages(value: dict, state: State):
    current_total = state.get('action_count', default=0)
    current_total += 1
    state.set('action_count', current_total)
    return current_total

# Define a function to add the key to the payload
def add_key_to_payload(value, key, timestamp, headers):
    value['page_id'] = key
    return value

# Apply a custom function and inform StreamingDataFrame to provide a State instance to it using "stateful=True"
sdf["action_count"] = sdf.apply(count_messages, stateful=True)
sdf = sdf[["action_count"]]

sdf = sdf.apply(add_key_to_payload, metadata=True) # Adding the key (page_id) to the payload for better visibility

sdf = sdf.update(lambda row: print(f"Received row: {row}"))

sdf = sdf.to_topic(output_topic)

if __name__ == "__main__":
    app.run(sdf)
