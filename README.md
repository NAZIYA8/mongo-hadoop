# mongo-hadoop

# Perform import export operation ie export data from mongodb database to csv file and store it in HDFS.

Version:
hadoop 3.3.1
MongoDB shell version v5.0.2

STEP1: 
Create a directory to store the data i.e. csv file into the hdfs.
$ hadoop fs -mkdir /mongo_import_export2

STEP2:
A directory will be created in the hdfs which is empty which can be checked using web console.

STEP3:
Write the python code to export the data to csv format from mongodb and store it in HDFS.

STEP4:
Run the python code and the output is displayed.

STEP5:
We can check whether data is stored  in HDFS or not by either using a web
console (http://localhost:9870) or viewing the files in HDFS by using the
command prompt. On the web console you can view your files in the “mongo_import_export2” directory.

# Read the data from hdfs and import it to mongodb database

Step1:
Start the hadoop daemons and check if it has started using jps 

Step2:
write a python code to read the csv file from hdfs and then create an empty collection in a database.
then import the data to the mongodb database.

Step3:
check if it is imported successfully in mongo shell using following commands.
> show dbs
> use test6
> show collections
> db.emp_details2.find({})

Hence you can see that the data has been successfully imported into mongdb 
