# Azure Multi-Tenant Resource Monitor

## Purpose  
This script connects to multiple Azure AD tenants, enumerates resource groups and resources, and retrieves CPU & memory/DTU metrics for various resource types (Azure SQL Database, SQL Managed Instance, VM with SQL) over a defined date-range and interval.

## Setup  
1. Create a `.env` file with entries for each tenant (IDs, client IDs, secrets).  
2. Install dependencies:  
