
#!/bin/bash
###############################################################################
#
#   Script Name          : sync_platemap_files.sh
#   Script Description   : sync platemap files between sftp and s3
#   Script Version       : 1.0
#   Script Usage         : ./sync_platemap_files.sh
#   Script Documentation : n/a
#
#   Version History
#   ---------------
#
#   Version    Date       Author             Change Notes
#   -------  ----------   ----------------   ----------------------------------
#    1.0     08/02/2022   Prasanth Kothuri   Initial Version
#    2.0     22/01/2023   Naga Morisetti     Added Archival and delta logic to pick files
###############################################################################

# light house labs sending the plate map files
declare -a centres=("project-heron_alderly-park" "project-heron/UK-Biocenter/Sanger Reports" "project-heron_glasgow" "project-heron_cambridge-az" "project-heron_randox" "project-heron_hsl" "project-heron_plym" "project-heron_brbr" "project-heron_lspa" "project-heron_newc")

if [[ "$env" == "prod" ]]; then
        bucket=dhsc-edge-internal
elif [[ "$env" == "dev" ]]; then
        bucket=dhsc-edge-test
fi

#move the latest processed files to archive
rclone copy sanger_s3:$bucket/platemap/staging sanger_s3:$bucket/platemap/archive
# delete previous run files
rclone delete sanger_s3:$bucket/platemap/staging
# for each lab sync files to s3
for centre in "${centres[@]}"
do
    lh=$(echo $centre  | cut -d'/' -f 2 | cut -d'_' -f 2)
    #delta - processed
    to_process=($(comm -23 <(rclone lsf --exclude=*/ --exclude=*report*.csv --max-age 7d sanger_sftp:/"$centre" | tr ' ' '\n' | sort) <(rclone lsf sanger_s3:$bucket/platemap/archive/$lh/ | tr ' ' '\n' | sort)));
#copy to_process files to staging
    for file_to_process in "${to_process[@]}"
      do
          rclone copy sanger_sftp:/"$centre"/$file_to_process sanger_s3:$bucket/platemap/staging/$lh/
      done
done
