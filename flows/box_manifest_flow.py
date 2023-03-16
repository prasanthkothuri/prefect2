from prefect_shell import shell_run_command
from prefect import flow, task
from prefect_email import email_send_message, EmailServerCredentials
from sanger.covid.box_manifests.processing.config import BoxManifestConfig
import os
import pymysql
from datetime import datetime
from common.static_variables import *
import awswrangler as wr
import boto3

env = os.getenv("env")
config_path = "job_configs/covid/box_manifests/processing.json"
config: BoxManifestConfig = BoxManifestConfig.read_from_json(env, config_path)


@flow(name='box_manifest_filesync')
def box_manifest_filesync():
    @task
    def get_staging_location():
        bucket_name = config.input_tables.box_manifests_input.bucket
        bucket = config.common_config.buckets.s3.__getattribute__(bucket_name).split("//")[1]
        location = config.input_tables.box_manifests_input.location
        return f"{bucket}/{location}"

    staging_loc = get_staging_location()
    sync_box_manifest_result = shell_run_command(command=f"{sync_box_manifest_files} {staging_loc}")


@flow(name='box_manifest_notification')
def box_manifest_notification():
    @task
    def get_location(table_name):
        bucket_name = config.output_tables.__getattribute__(table_name).bucket
        bucket = config.common_config.buckets.s3.__getattribute__(bucket_name).split("//")[1]
        location = config.output_tables.__getattribute__(table_name).location
        return f"s3://{bucket}/{location}"

    @task
    def download_from_s3(job, output):
        wr.config.s3_endpoint_url = s3_endpoint_url
        s3path = f"{get_location(f'{job}')}/processed_date={datetime.now().strftime('%Y-%m-%d')}/"
        localpath = f"/tmp/{output}_{datetime.now().strftime('%Y-%m-%d')}.csv"
        s3_session = boto3.Session(aws_access_key_id=os.getenv("s3_access_key"),
                                   aws_secret_access_key=os.getenv("s3_secret_key"))
        s3object = wr.s3.list_objects(path=s3path, boto3_session=s3_session)
        if s3object:
            wr.s3.download(path=s3object[0], local_file=localpath, boto3_session=s3_session)
            return True
        else:
            return False

    @task
    def send_email(output):
        if "invalid" in output:
            subject = invalid_subject
            msg = invalid_msg
            email_to = invalid_email_to
        elif "express" in output:
            subject = express_subject
            msg = express_msg
            email_to = express_email_to
        elif "uploaded" in output:
            subject = aggregate_subject
            msg = aggregate_msg
            email_to = aggregate_email_to

        if "dev" in os.getenv("env"):
            email_to = email_to_dev

        email_server_credentials = EmailServerCredentials(
            smtp_server=smtp_server,
            smtp_port=smtp_port, smtp_type=smtp_type,
        )
        print("sending email")
        subject = email_send_message(
            email_server_credentials=email_server_credentials,
            subject=subject,
            msg=msg,
            email_to=email_to,
            email_from=email_from,
            attachments=[f"/tmp/{output}_{datetime.now().strftime('%Y-%m-%d')}.csv"]
        )
        print("email sent")

        invalid_download = download_from_s3("box_manifest_invalid", "invalid_plates")
        if invalid_download:
            invalid_plates = send_email("invalid_plates", upstream_tasks=[invalid_download])
        express_download = download_from_s3("box_manifest_express", "express_plates")
        if express_download:
            express_plates = send_email("express_plates", upstream_tasks=[express_download])
        aggregate_download = download_from_s3("box_manifest_aggregate_report", "uploaded_plates")
        if aggregate_download:
            aggregate_download = send_email("uploaded_plates", upstream_tasks=[aggregate_download])


@flow(name='box_manifest_processing')
def box_manifest_processing():
    @task
    def get_staging_location():
        bucket_name = config.input_tables.box_manifests_input.bucket
        bucket = config.common_config.buckets.s3.__getattribute__(bucket_name).split("//")[1]
        location = config.input_tables.box_manifests_input.location
        return f"{bucket}/{location}"

    command = shell_run_command(command=f"{spark_submit_script}  box_manifests processing processing")


@flow(name='box_manifest')
def box_manifest():
    box_manifest_filesync()
    box_manifest_processing()
    box_manifest_notification()


if __name__ == '__main__':
    box_manifest()
