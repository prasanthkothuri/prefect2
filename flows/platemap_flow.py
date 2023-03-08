from prefect_shell import shell_run_command
from prefect import flow, task
from prefect_email import  email_send_message, EmailServerCredentials
from sanger.covid.platemap.processing.config import PlateMapConfig
import os
import pymysql
from datetime import datetime
from common.static_variables import *
import awswrangler as wr


# flow to chain the tasks
@flow(name="platemap_filesync")
def platemap_filesync():
    # spark_submit.sh {pipeline} {app} {job_config}
    sync_platemap_result = shell_run_command(command=platemap_file_sync_script, return_state=True
                                             , return_all=True)
    return sync_platemap_result.result()


@task(name='download_from_db')
def download_from_db():
    # variables
    config_path = "job_configs/covid/platemap/processing.json"
    env = os.getenv("env")
    config = PlateMapConfig.read_from_json(env, config_path)
    host = config.common_config.databases.dt4_mysql.host
    port = config.common_config.databases.dt4_mysql.port
    user_name = config.common_config.databases.dt4_mysql.user_name
    password = config.common_config.databases.dt4_mysql.password

    localpath = f"/tmp/platemap_{datetime.now().strftime('%Y-%m-%d-%H')}.csv"
    mysql_conn = pymysql.connect(database=metadata_database_name,
                                 user=user_name,
                                 password=password,
                                 host=host,
                                 port=port,
                                 ssl={'ssl': 'TRUE'})
    df = wr.mysql.read_sql_query(
        sql=platemap_notification_query,
        con=mysql_conn,
    )
    if df.empty:
        return False
    else:
        df.to_csv(localpath, index=False)
        return True



@flow(name='platemap_notification')
def platemap_notification():
    subject = None
    if "dev" in os.getenv("env"):
        email_to = email_to_dev
    else:
        email_to = platemap_email_to

    email_server_credentials = EmailServerCredentials(
        smtp_server=smtp_server,
        smtp_port=smtp_port, smtp_type=smtp_type,
    )
    print("sending email")
    download_status = download_from_db()
    if download_status:
        print("sending email")
        subject = email_send_message(
            email_server_credentials=email_server_credentials,
            subject=platemap_subject,
            msg=platemap_msg,
            email_to=email_to,
            email_from=email_from,
            attachments=[f"/tmp/platemap_{datetime.now().strftime('%Y-%m-%d-%H')}.csv"]
        )
        print("email sent")
    return subject

@flow(name="platemap_processing")
def platemap_processing():
    processing_result = shell_run_command(command=f"{spark_submit_script} platemap processing processing",
                                          return_state=True
                                          , return_all=True)
    return processing_result.result()

@flow(name="platemap_feedback_processing")
def platemap_feedback_processing():
    processing_result = shell_run_command(command=f"python3 {platemap_feedback_processing_script}",
                                          return_state=True
                                          , return_all=True)
    return processing_result.result()

@flow(name="platemap")
def platemap():
    platemap_filesync()
    platemap_processing()
    platemap_notification()


if __name__ == "__main__":
    platemap()

