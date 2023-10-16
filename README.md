# Data-Migration-Script
Data Migration from Postgres to REDSHIFT
## Install Libraries
```
pip install -r requirements.txt
```
## Changes to Migration script
Add the Logstash orchestrator URL and slack channel URL (if you need prompts on slack channel). Edit PostgreSQL and REDSHIFT Host, port, user and password.
## Edit Logstash parameters
In Logstash config (in migration script), edit table schema, table name, column names to move and column filters to apply (if needed).
