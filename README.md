# Data-Migration-Script
Data Migration from Postgres to REDSHIFT
## Install Libraries
```
pip install -r requirements.txt
```
## Changes to Migration script
Add the Logstash orchestrator URL, and slack channel URL (if you need prompts on slack channel). Edit PostgreSQL and REDSHIFT Host, port, user and password.
## Edit Logstash parameters
In Logstash config (in migration script), edit schema name, table name, column names to move & column filter to apply (if needed).
## Changes to Validation script
Edit Host, port, username and password.
