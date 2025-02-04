/*
===================================================================================================
Create Database and Schemas
===================================================================================================
Script Purpose:
      This script creates a new database named 'DataWarehouse', first checking whether it already exists.
      If the database is found, it is dropped and recreated.
      The script also defines three schemas within the database: 'bronze', 'silver', and 'gold'.

WARNING:
     Executing this script will permanently delete the 'DataWarehouse' database if it already exists.
     All stored data will be lost. Proceed with caution and ensure proper backups before running this script.

*/

USE master;
GO

Drop and Recreate the 'DataWarehouse' Database
--The script first checks whether the 'DataWarehouse' database exists.
--If it does, it is set to single-user mode with rollback enabled to terminate active connections, then dropped.


IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')  
BEGIN  
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;  
    DROP DATABASE DataWarehouse;  
END;  
GO  

Create the 'DataWarehouse' Database
A new instance of 'DataWarehouse' is created.

CREATE DATABASE DataWarehouse;  
GO  

USE DataWarehouse;  
GO  

CREATE SCHEMA bronze;  
GO  

CREATE SCHEMA silver;  
GO  

CREATE SCHEMA gold;  
GO  







