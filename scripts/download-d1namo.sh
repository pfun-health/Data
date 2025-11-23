#!/usr/bin/env bash

# download-d1namo.sh : download the d1namo ECG+CGM multimodal dataset from Kaggle

output_dir="_raw_data"
output_fn="d1namo-ecg-glucose-data.zip"
output_fpath="${output_dir}/${output_fn}"
SRC_URL="https://www.kaggle.com/api/v1/datasets/download/sarabhian/d1namo-ecg-glucose-data"

download_via_curl() {
	curl -L -o "${output_fpath}" \
		"${SRC_URL}"
}

download_via_aria2c() {
	aria2c --continue=true --out="${output_fpath}" \
		"${SRC_URL}"
}


download_via_aria2c