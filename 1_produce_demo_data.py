#!/usr/bin/env python3
"""
This script produces demo data to a Kafka topic using the Quix Streams library.
The demo data consists of user actions on a web page, including the timestamp,
user ID, page ID, and action type (view, hover, scroll, click).
The script uses a JSON schema to serialize the data before sending it to the Kafka topic.
The script runs indefinitely, generating random user actions and sending them to the Kafka topic.
"""


import os
import random
import time
import json
import uuid
import logging
from quixstreams import Application
from quixstreams.kafka.configuration import ConnectionConfig
from quixstreams.models import (
    SchemaRegistryClientConfig,
    SchemaRegistrySerializationConfig,
    JSONSerializer
    )
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

### WARPSTREAM CONNECTION
# Define your SASL configuration
broker_connection = ConnectionConfig(
     bootstrap_servers=os.environ["bootstrap_server"],
     security_protocol=os.environ.get("security_protocol","SASL_SSL"),  # or "SASL_SSL" if using SSL
     sasl_mechanism=os.environ.get("sasl_mechanism", None),  # "PLAIN" or any other supported mechanism
     sasl_username=os.environ.get("sasl_username", None),
     sasl_password=os.environ.get("sasl_password", None)
)

schema_registry_client_config = SchemaRegistryClientConfig(
    url=os.environ.get("schema_registry_url", None)
)
schema_registry_username=os.environ.get("schema_registry_username", None)
schema_registry_password=os.environ.get("schema_registry_password", None)
if schema_registry_username and schema_registry_password:
    schema_registry_client_config = SchemaRegistryClientConfig(
        url=os.environ.get("schema_registry_url", None),
        basic_auth_user_info=f"{schema_registry_username}:{schema_registry_password}"
    )

# page_action_schema = {
#     "title": "pageActions",
#     "type": "object",
#     "properties": {
#         "timestamp": {"type": "integer"},
#         "user_id": {"type": "string"},
#         "page_id": {"type": "string"},
#         "action": {"type": "string"}
#     },
#     "required": ["timestamp", "user_id", "page_id", "action"]
# }

# Load the output schema from the file
page_action_schema = None
with open(os.environ.get("raw_data_schema_file","page_action_schema.json"), "r") as f:
    schema_file = f.read()
    page_action_schema = json.loads(schema_file)

user_action_schema_serializer = JSONSerializer(
    schema=page_action_schema,
    schema_registry_client_config=schema_registry_client_config
)

# Initialize the Quix Application with the connection configuration
app = Application(broker_address=broker_connection)
# Define destination topic and schema
output_topic = app.topic(
    name=os.getenv("raw_data_topic","pageActions"),
    value_serializer=user_action_schema_serializer,
    config={
        "auto.create.topics.enable": "true"
    }
)

def main():
    """
    Produce demo data to the Kafka topic.
    """
    actions = ['view', 'hover', 'scroll', 'click']
    num_users = 100
    num_pages = 9
    # create a pre-configured Producer object.
    with app.get_producer() as producer:
        while True:
            current_time = int(time.time())
            record = {
                "timestamp": current_time,
                "user_id": f"user_{random.randint(1, num_users)}",
                "page_id": f"page_{random.randint(0, num_pages)}",
                "action": random.choice(actions)
            }
            json_data = json.dumps(record)

            # publish the data to the topic
            logger.info(f"Publishing row: {json_data}")
            producer.produce(
                topic=output_topic.name,
                key=str(uuid.uuid4()),
                value=json_data,
            )

            time.sleep(random.uniform(0.5, 1.5))

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Exiting.")
