covid_reports_base_path = "/root/covid_reports"
spark_submit_script = "/opt/covid_reports/spark_submit.sh"
spark_app_docker_image = "gitlab-registry.internal.sanger.ac.uk/pam-dt4/spark-service/spark-service-docker/spark-app:"
s3_endpoint_url = "https://cog.sanger.ac.uk"

email_from = "dt4_noreply@sanger.ac.uk"
#smtp server details
smtp_server = "mail.internal.sanger.ac.uk"
smtp_port=25
smtp_type="STARTTLS"

email_to_dev = "vk9@sanger.ac.uk,nl9@sanger.ac.uk,ra13@sanger.ac.uk,mp39@sanger.ac.uk"
email_id_heron_service = "heron-service-delivery@sanger.ac.uk"
email_id_pam_dt4 = "pam-dt4-data-engineering@sanger.ac.uk"
email_id_heron_sulston = "heron_sulston@sanger.ac.uk"
# invalid plates
invalid_subject = "[Pam-dt4-data-engineering] ERROR: Heron - Sample Priority Pipeline"
invalid_msg = """Some entries in today's box manifests failed to validate. A compilation of these entries is attached.
Entries with unacceptable optional fields will still been uploaded to labwhere if they were otherwise valid but the optional fields were not processed.
All other entries were not uploaded to labwhere.
In the case of both an optional field and a mandatory field being invalid, there will be seperate entries for the same record for the full schema and the mandatory schema."""
invalid_email_to = ",".join([email_id_heron_service, email_id_pam_dt4])
# express plates
express_subject = "[Pam-dt4-data-engineering] INFO: Heron - Express Plates"
express_msg = "Attached is a compiled list of all express plates in box manifests processed today."
express_email_to = ",".join([email_id_heron_service, email_id_pam_dt4])
voc_pangolin_git_alert_email = ",".join([email_id_heron_service, email_id_pam_dt4])
# aggregate plates
aggregate_subject = "[Pam-dt4-data-engineering] INFO: Heron - Box Manifest Upload Aggregates"
aggregate_msg = "Attached are the aggregate figures for today's box manifest uploads per lighthouse. Be aware that manifests or manifest entries that failed to validate will not be included."
aggregate_email_to = ",".join([email_id_heron_service, email_id_heron_sulston, email_id_pam_dt4])
# platemap validation
platemap_subject = "[Pam-dt4-data-engineering] INFO: Heron - Platemap Validation"
platemap_msg = "Attached are the platemap validation figures for today's platemaps received per lighthouse."
platemap_email_to = ",".join([email_id_heron_service, email_id_pam_dt4])
platemap_file_sync_script = "/opt/prefect/scripts/sync_platemap_files.sh"
platemap_feedback_processing_script = "/opt/prefect/scripts/process_platemap_feedback.py"
metadata_database_name = "nifi_pipeline_mesh"

platemap_notification_query = """
    SELECT 
        date(messageCreateDateUtc) as Date,
        SUBSTRING_INDEX(SUBSTRING_INDEX(file_name,'/',-2),'/',1) as Lab,
        SUBSTRING_INDEX(file_name,'/',-1) as FileName,
        count(1) as Total,
        SUM(
            CASE
                WHEN gsu_errors = "" THEN 1
                ELSE 0
                END
            ) as Passed,
        SUM(
            CASE
                WHEN SUBSTRING_INDEX(gsu_errors,'|',1) = "TYPE 0: Root Sample ID is Empty" THEN 1
                WHEN SUBSTRING_INDEX(SUBSTRING_INDEX(gsu_errors,'|',2),'|',-1) = "TYPE 3: Result missing" THEN 1
                ELSE 0
                END
            ) as FailedEmpty,
        SUM(
            CASE
                WHEN gsu_errors != "" 
                    AND SUBSTRING_INDEX(gsu_errors,'|',1) != "TYPE 0: Root Sample ID is Empty" 
                    AND SUBSTRING_INDEX(SUBSTRING_INDEX(gsu_errors,'|',2),'|',-1) != "TYPE 3: Result missing" 
                THEN 1
                ELSE 0
                END
            ) as FailedOther
    FROM create_platemap 
    WHERE date(messageCreateDateUtc) = subdate(current_date, 0) 
    GROUP BY date(messageCreateDateUtc),file_name
    ORDER BY Lab, file_name
    """

#Mesh database
s3_bucket_prod = "mesh-prod"
mysql_host_prod = "vm-mii-mesh-p1.internal.sanger.ac.uk"
s3_bucket_dev = "mesh-dev"
mysql_host_dev = "172.27.26.134"
mysql_user = "nifi_user"
mysql_database = "nifi_pipeline_mesh"


#NHSD pipeline variables
eng_table = "nhsd_data_eng"
sco_table = "nhsd_data_sco"
nhsd_ingested_files_name = "nhsd_ingested_files.list"
eng_file_type = "<WorkflowId>SANG_COVID19_ANTIGEN</WorkflowId>"
sco_file_type = "<WorkflowId>SANG_COVID19_ANTIGEN_PHS</WorkflowId>"
s3_path_nhsd_ind = "s3://dhsc-edge-prod/latest_nhsd_covid19.csv"
endpoint_url_nhsd_ind = "https://cog.sanger.ac.uk"