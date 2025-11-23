#!/usr/bin/env bash

# unzipcsv2parquet.sh : unzip the csv files, then convert to parquet.

pfun-data-unzipcsv2parquet \
	--zip-path $(realpath "${PWD}/_raw_data/d1namo-ecg-glucose-data.zip") \
	--csv-path $(realpath "${PWD}/_raw_data/d1namo-ecg-glucose-data/csv_files") \
	--parquet-path $(realpath "${PWD}/_raw_data/d1namo-ecg-glucose-data/parquet_files")
