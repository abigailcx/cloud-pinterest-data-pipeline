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

def post_data(data=None, topic_name=None, invoke_url=None, headers=None):
    payload = json.dumps({
        "records": [
                {       
                "value": data
                }
            ]
        }, cls=DateTimeEncoder)
    print(f"payload: {payload}")
            
    response = requests.request("POST", f"{invoke_url}/topics/{topic_name}", headers=headers, data=payload)
    print(response.status_code)

def run_posting_emulation():
    uuid = "0a55250cde99"
    invoke_url = "https://1kck6ch0ui.execute-api.us-east-1.amazonaws.com/production"
    headers = {'Content-Type': 'application/vnd.kafka.json.v2+json'}

    while True:
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)
        
        pin_result = get_data(source_table="pinterest_data", random_row=random_row)
        geo_result = get_data(source_table="geolocation_data", random_row=random_row )
        user_result = get_data(source_table="user_data", random_row=random_row)

        post_data(data=pin_result, topic_name=f"{uuid}.pin", invoke_url=invoke_url, headers=headers)
        post_data(data=geo_result, topic_name=f"{uuid}.geo", invoke_url=invoke_url, headers=headers)
        post_data(data=user_result, topic_name=f"{uuid}.user", invoke_url=invoke_url, headers=headers)


if __name__ == "__main__":
    run_posting_emulation()

    

    
