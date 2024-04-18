import json
import random
import requests
from sqlalchemy import text
from time import sleep
from data_transform import DateTimeEncoder
from db_connector import AWSDBConnector

random.seed(100)

def get_data(source_table=None, random_row=None):
    new_connector = AWSDBConnector()
    engine = new_connector.create_db_connector()
    with engine.connect() as connection:
        data_string = text(f"SELECT * FROM {source_table} LIMIT {random_row}, 1")
        selected_row = connection.execute(data_string)
        for row in selected_row:
            result = dict(row._mapping)
    
    return result

def post_data(data=None, stream_name=None, partition_key=None, invoke_url=None, headers=None):
    payload = json.dumps({
        "StreamName": stream_name,
        "Data": data,
        "PartitionKey": partition_key
    }, cls=DateTimeEncoder)
    print(f"payload: {payload}")
            
    response = requests.request("PUT", f"{invoke_url}/streams/{stream_name}/record", headers=headers, data=payload)
    print(response.status_code)

def run_posting_emulation():
    uuid = "0a55250cde99"
    invoke_url = "https://1kck6ch0ui.execute-api.us-east-1.amazonaws.com/production"
    headers = {'Content-Type': 'application/json'}

    while True:
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)
        
        pin_result = get_data(source_table="pinterest_data", random_row=random_row)
        geo_result = get_data(source_table="geolocation_data", random_row=random_row )
        user_result = get_data(source_table="user_data", random_row=random_row)

        post_data(data=pin_result, stream_name=f"streaming-{uuid}-pin", partition_key='partition-pin', invoke_url=invoke_url, headers=headers)
        post_data(data=geo_result, stream_name=f"streaming-{uuid}-geo", partition_key='partition-geo',invoke_url=invoke_url, headers=headers)
        post_data(data=user_result, stream_name=f"streaming-{uuid}-user", partition_key='partition-user',invoke_url=invoke_url, headers=headers)


if __name__ == "__main__":
    run_posting_emulation()

    

    
