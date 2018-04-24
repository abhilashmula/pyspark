#!/bin/sh

dir_name=$1
table_name=$2
db_name=$3

parquet_file=$1$(hadoop fs -ls  $dir_name/*.parquet | awk -F'/' '{print $NF}' | head -1)
echo $parquet_file

echo "Creating Impala Table"

impala-shell -d $3 -q "create external table $table_name like parquet ' $1$(hadoop fs -ls  $dir_name/*.parquet | awk -F'/' '{print $NF}' | head -1) ' stored as parquet location '$1'"