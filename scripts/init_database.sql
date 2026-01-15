-- Project: SQL Data Warehouse
-- Drop and Recreate the Database
-- Warning: This will delete all data in the database!
-- ========================================================

-- 1. Drop the existing database (if it exists)
-- Note: In PostgreSQL, you cannot drop a database while connected to it. 
-- You must run this command from a different database (e.g., 'postgres').

DROP DATABASE IF EXISTS data_warehouse;

-- 2. Recreate the database from scratch
CREATE DATABASE data_warehouse;

-- =============================================

-- Initializing the Bronze, Silver, and Gold schemas.
-- =============================================

-- Create Bronze Schema (Raw Data)
CREATE SCHEMA IF NOT EXISTS bronze;

-- Create Silver Schema (Cleaned & Standardized)
CREATE SCHEMA IF NOT EXISTS silver;

-- Create Gold Schema (Business-Ready & Analytics)
CREATE SCHEMA IF NOT EXISTS gold;


  


