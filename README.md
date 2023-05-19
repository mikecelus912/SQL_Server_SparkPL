# SQL_Server_SparkPL
A simple ETL pipeline that uses pyodbc to connect to SSMS to extract data from the AdventureWorks2019 db. Data is extracted using dataframes converted into parquet files that can then be transformed utilizing Spark.

Tech stack: Python 3.10.9; PySpark 3.3.2; Java 1.8.0_331; Pycharm Community Edition 2023.1

Prerequisites:
Extract spark-3.3.2-bin-hadoop3-scala2.13.tgz into C:\Spark\

Set system environment variables for Windows:
  variable        value
  JAVA_HOME       C:\JaVA
  SPARK_HOME      C:\Spark
  PYTHONPATH      C:\Spark\python\lib\py4j-0.10.9.5-src.zip 
 
  PATH            C:\Spark\python
                  C:\Java\bin

Create Inbound rule in Windows Firewall Manager for Port 1443 to allow remote connection for SQL Server

Open SQL Server Configuration Manager and navigate to Network Configuration and Protocols for SQLEXPRESS. Make sure Protocol Name: TCP/IP and Status: Enabled

Run this script in SSMS:

USE [master]
GO
CREATE LOGIN [your_username] 
WITH PASSWORD=N'any_password_you_want_to_use'
	,DEFAULT_DATABASE=[AdventureWorks2019]
	,CHECK_EXPIRATION=OFF
	,CHECK_POLICY=OFF
USE AdventureWorks2019
GO
CREATE USER [your_username] FOR LOGIN [your_username]
GO 
USE AdventureWorks2019
GO
ALTER ROLE [db_datareader] ADD Member [your_username]
ALTER ROLE [db_datawriter] ADD Member [your_username]
GO
USE [master]
GO
GRANT CONNECT SQL TO [mike]
GO

The username and password you create in this script can be used in the credentials.yml file when establishing a connection to SSMS in the dbconnector.py script

Program Structure:
A dbconnector script to connect to MSSMS that stores data from every table into a df that is converted into a parquet file. The server and driver variables in #db connection information is your SSMS connection profile.

A sparkconnection script with a class to initialize the Spark connection and transform the parquet files into dfs and then temp views for transformations.

A login script with a login menu that checks user credentials stored in yaml config.

The main script to launch the program.

Note: yaml for this project does not contain the correct credentials for connecting to MS SQL Server --see Run this script in SSMS





