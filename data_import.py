# import os 
# filename = 'ecommerce-data.zip'
# output_dir = 'dataset/'
# os.system("unzip "+filename+ " -d" + output_dir)


from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

uri = "mongodb+srv://glee2dev:aiIgJmzlQm6pjCS1@reportingdata.pwt1o.mongodb.net/?retryWrites=true&w=majority&appName=ReportingData"

# Create a new client and connect to the server
client = MongoClient(uri, server_api=ServerApi('1'))

# Send a ping to confirm a successful connection
try:
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
except Exception as e:
    print(e)