import random
import sqlalchemy
import yaml

random.seed(100)


class AWSDBConnector:
    """Creates a connection to the AWS database"""
    def __init__(self, creds_file="db_creds.yaml"):
        self.creds_file = creds_file
    
    def get_creds(self):
        """Retrieves credentials from the database credentials file"""
        with open(self.creds_file, 'r') as f:
            creds = yaml.safe_load(f)
            return creds
        
    def create_db_connector(self):
        """Makes an engine connection using the credentials extracted from the database credentials file"""
        creds = self.get_creds()
        engine = sqlalchemy.create_engine(f"mysql+pymysql://{creds['USER']}:{creds['PASSWORD']}@{creds['HOST']}:{creds['PORT']}/{creds['DATABASE']}?charset=utf8mb4")
        return engine