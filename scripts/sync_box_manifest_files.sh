#!/bin/bash
s3_base_path=$1
# light house labs sending the plate map files
declare -a labs=("MK" "RANDOX" "NC" "PLYM" "HSL" "BB" "GLS" "LSPA")

rclone delete sanger_s3:/$s3_base_path
# for each lab sync files to s3
for lab in "${labs[@]}"
do
    sftp_path="project-heron_box_manifests/$lab/current/"
    s3_path=$s3_base_path/ProcessingLabCode=$lab/
    echo "copying from $sftp_path to $s3_path"
    rclone sync --exclude Archive/ --max-age 24h sanger_sftp:/"$sftp_path" sanger_s3:$s3_path
done
