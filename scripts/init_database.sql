/* 
Create Database and Schemas
---------------------------------------------------------------------------------------------------------------------
This script creates a new database named 'DataWarehouse'. If the database already exists, it is dropped and recreated
with three schemas within the database: 'bronze', 'silver' and 'gold'.

WARNING:
Ensure you have the proper backups before running this script, as it will permanently delete all the data in the 
'DataWarehouse' database.
*/

USE master;
GO

-- Drop and recreate 'DataWarehouse' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
	ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DataWarehouse;
END;
GO

-- Create the Database 'DataWarehouse'

CREATE DATABASE DataWarehouse;
GO

-- Create Schema
USE DataWarehouse;
GO

CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO
