USE master;


-- drop and recreate the 'DataWarehouse' database
IF EXIST ( SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse'
  BEGIN
  
      ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
  END;
GO
  


-- Create the Database
CREATE DATABASE DataWarehouse;


USE DataWarehouse;


-- Create the 3 schemas
CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
