/*
====================================================================================================
CREATE DATABASE AND SCHEMAS
=============================================================================================

Script purpose:
	This scipt creates a new database named 'DataWarehouse' after checking if it already exists.
	if the database exists, it is dropped and recreated, Additionally , the script sets up three schemas
	within the database: 'broze','silver' and 'gold'.

WARNING:
	Running this scrips will drop the entire 'Datawarehouse' databse if it exists.
	All data in the datbase will be permanently deleted. processed with caution 
	and ensure you have proper backups before running this scripts.
*/


USE master;
GO

-- Drop and recreate the 'DataWarehouse' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWarehouse;
END;
GO
-- create a 'dataWarehouse' database
CREATE DATABASE DataWarehouse;

USE DataWarehouse;

-- create schemas
CREATE SCHEMA bronze;
GO  -- use as a separator to execute the program , first compelete the previous code then execute next

CREATE SCHEMA silver;

GO
CREATE SCHEMA gold;

