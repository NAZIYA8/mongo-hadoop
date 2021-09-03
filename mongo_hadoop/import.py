'''
@Author: Naziya
@Date: 2021-09-02
@Last Modified by: Naziya
@Last Modified : 2021-09-02
@Title : Program Aim is to get the data from hdfs and import it to mongodb.
'''

from pymongo import MongoClient
import pandas
from dotenv import load_dotenv
load_dotenv()
import os 
import pandas as pd
import json

host = os.environ.get("HOST")
port = os.environ.get("PORT")
client = MongoClient(host,int(port))
db = client.test6

print("Creating collection emp_details2")
col = db.emp_details2
if col.drop(): 
    print('Deleted existing collection')
db.create_collection("emp_details2")

import pydoop.hdfs as hd
with hd.open("/mongo_import_export2/import_export2.csv") as f:
    df =  pd.read_csv(f)
    print(df)

records = json.loads(df.T.to_json()).values()
db.emp_details2.insert_many(df.to_dict('records'))

result = db.emp_details2.find()
for data in result:
    print(data)
print("Showed all documents\n")