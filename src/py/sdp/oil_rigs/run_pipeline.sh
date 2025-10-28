#!/bin/bash
# check if a directory exists then remove it
if [ -d "spark-warehouse" ]; then
    rm -rf spark-warehouse
fi                  

# remove the metastore_db directory if it exists
if [ -d "metastore_db" ]; then
    rm -rf metastore_db
fi

spark-pipelines run --conf spark.sql.catalogImplementation=hive