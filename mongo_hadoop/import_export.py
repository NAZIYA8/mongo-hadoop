'''
@Author: Naziya
@Date: 2021-09-02
@Last Modified by: Naziya
@Last Modified : 2021-09-02
@Title : Program Aim is to import and export to csv and store it in hdfs.
'''


from pymongo import MongoClient
import time
import os
import pandas
from dotenv import load_dotenv
load_dotenv()
import os 
import pandas as pd
from subprocess import PIPE, Popen


host = os.environ.get("HOST")
port = os.environ.get("PORT")
client = MongoClient(host,int(port))
db = client.test5

try:
    print("Creating collection emp_details")
    col = db.emp_details
    if col.drop(): 
        print('Deleted existing collection')
    db.create_collection("emp_details")

    db.emp_details.insert_many([
            {"EmployeeNo" : "1","FirstName" : "Dolly","LastName": "Singh","Age" : "27","Salary": "30000",'Gender':'Female'},
            {"EmployeeNo" : "2","FirstName" : "Amit","LastName": "Sharma","Age" : "40","Salary": "50000",'Gender':'Male'},
            {"EmployeeNo" : "3","FirstName" : "Komal","LastName": "Pande","Age" : "20","Salary": "18000",'Gender':'Female'},
            {"EmployeeNo" : "4","FirstName" : "Ashima","LastName": "Arora","Age" : "25","Salary": "28000",'Gender':'Female'},
            {"EmployeeNo" : "5","FirstName" : "Sneha","LastName": "Joshi","Age" : "30","Salary": "45000",'Gender':'Female'},
            {"EmployeeNo" : "6","FirstName" : "Gautham","LastName": "Deshpande","Age" : "30","Salary": "40000",'Gender':'Male'}
            ])
    print("Inserted Multiple Documents\n")

    result = db.emp_details.find()
    for data in result:
        print(data)
    print("Showed all documents\n")

    cursor = col.find()
    mongo_docs = list(cursor)

    print("total docs:")
    print(len(mongo_docs))

    docs = pandas.DataFrame(columns=[])
    for num, doc in enumerate(mongo_docs): 
        doc_id = str(doc["_id"])
        doc["_id"]=doc_id
        series_obj = pandas.Series( doc, name=doc_id )
        docs = docs.append( series_obj )


    # export MongoDB documents to a CSV file
    docs.to_csv("import_export2.csv", ",")
    # export MongoDB documents to CSV
    csv_export = docs.to_csv(sep=",") 
    print("\nCSV data:")
    print(csv_export)

    # create path to your username on hdfs
    hdfs_path = os.path.join(os.sep, 'mongo_import_export2' )

    # put csv into hdfs
    put = Popen(["hadoop", "fs", "-put", 'import_export2.csv', hdfs_path], stdin=PIPE, bufsize=-1)
    put.communicate()
    print("Successfully stored in hdfs")

except Exception as err:
        print(err)