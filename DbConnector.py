import os
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()
PASSWORD = os.getenv('PASSWORD')

class DbConnector:
    """
    Connects to the MongoDB server on Docker.
    Uses the root user to authenticate.

    Example:
    HOST = "localhost:55000"   # Host IP or 'localhost' for local connections
    USER = "root"              # Root user
    PASSWORD = "test123"       # Password for the root user
    """

    def __init__(self,
                 DATABASE='local_db',
                 HOST="localhost:55000",
                 USER="root",           # Use root as the username
                 PASSWORD=PASSWORD):    # Use the root password
        # Add `authSource=admin` to ensure MongoDB uses the admin database for authentication
        uri = "mongodb://%s:%s@%s/%s?authSource=admin" % (USER, PASSWORD, HOST, DATABASE)
        # Connect to the database
        try:
            self.client = MongoClient(uri)
            self.db = self.client[DATABASE]
            print("You are connected to the database:", self.db.name)
            print("-----------------------------------------------\n")
        except Exception as e:
            print("ERROR: Failed to connect to db:", e)

    def close_connection(self):
        # Close the DB connection
        self.client.close()
        print("\n-----------------------------------------------")
        print("Connection to %s-db is closed" % self.db.name)
