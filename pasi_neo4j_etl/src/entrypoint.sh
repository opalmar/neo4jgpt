#!/bin/bash

# Run any setup steps or pre-processing tasks here
echo "Running ETL to move students data from csvs to Neo4j..."

# Run the ETL script
python pasi_bulk_csv_write.py