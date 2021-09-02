# mongo-hadoop

# Perform import export operation ie import data from mongodb database and export it to csv file and store it in HDFS.

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
