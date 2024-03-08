import random
import sqlalchemy
import yaml

random.seed(100)


class AWSDBConnector:

    def __init__(self, creds_file="db_creds.yaml"):
        self.creds_file = creds_file
    
    def get_creds(self):
        with open(self.creds_file, 'r') as f:
            creds = yaml.safe_load(f)
            return creds
        
    def create_db_connector(self):
        creds = self.get_creds()
        engine = sqlalchemy.create_engine(f"mysql+pymysql://{creds['USER']}:{creds['PASSWORD']}@{creds['HOST']}:{creds['PORT']}/{creds['DATABASE']}?charset=utf8mb4")
        return engine