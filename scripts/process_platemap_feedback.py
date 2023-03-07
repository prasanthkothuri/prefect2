import os
import ssl
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import pymysql
import pandas as pd
import awswrangler as wr
import json
from io import StringIO
from ssl import create_default_context
from pika import BlockingConnection, ConnectionParameters, PlainCredentials, SSLOptions
from fastavro import json_reader, parse_schema
from datetime import datetime, timezone
from requests import get, packages
from urllib3.exceptions import InsecureRequestWarning
from sanger.covid.platemap.processing.config import PlateMapConfig
from common.constants import email_to_dev, smtp_server, email_from, invalid_email_to
from prefect.client.secrets import Secret


def send_email_notification(body):
    message = MIMEMultipart()
    message["Subject"] = "ERROR: Heron - Platemap Rejected"
    message["From"] = email_from
    if "prod" in os.getenv("env"):
        message["To"] = invalid_email_to
    else:
        message["To"] = email_to_dev
    message.attach(MIMEText(body, "html"))
    context = ssl.create_default_context()
    server = smtplib.SMTP(smtp_server, 25)
    server.starttls(context=context)
    server.login(Secret("EMAIL_USERNAME").get(), Secret("EMAIL_PASSWORD").get())
    server.send_message(message)


def process_create_platemap_feedback(body):
    read_schema_response = get(
        f"{redpanda_host}subjects/create-plate-map-feedback/versions/latest",
        headers={"X-API-KEY": redpanda_api_key}, verify=False).json()
    read_schema_obj = json.loads(read_schema_response["schema"])
    read_schema = parse_schema(read_schema_obj)
    string_reader = StringIO(body.decode("utf-8"))
    for record in json_reader(string_reader, read_schema):
        print(f"{record['sourceMessageUuid']}: processing create_platemap_feedback")
        select_query = f"""select * from create_platemap where lh_source_plate_uuid = '{record["sourceMessageUuid"]}'"""
        df = wr.mysql.read_sql_query(
            sql=select_query,
            con=mdb_mysql_conn
        )
        if not df.empty:
            print(f"{record['sourceMessageUuid']}: found")
            cursor = mdb_mysql_conn.cursor()
            cur = mlwh_mysql_conn.cursor()
            if record["operationWasErrorFree"] is True:
                print(f"{record['sourceMessageUuid']}: no errors")
                # update create_platemap table
                update_query = (f"update create_platemap set errors = 0, feedbackRcvdDateUtc = '{datetime.now()}'"
                                f""" where lh_source_plate_uuid = '{record["sourceMessageUuid"]}'""")
                cursor.execute(update_query)
                # update lighthouse_sample table
                # set is_current to False if that sample already exists
                for index, row in df.iterrows():
                    sql = """UPDATE lighthouse_sample SET is_current = %s 
                    WHERE lh_sample_uuid = %s AND root_sample_id = %s"""
                    cur.execute(sql, (0, row['lh_sample_uuid'], row['root_sample_id']))
                drop_columns = ['messageCreateDateUtc', 'fitToPick', 'errors', 'gsu_errors', 'file_name', 'line_number',
                                'feedbackRcvdDateUtc', 'current_rna_id']
                df = df.drop(columns=drop_columns)
                df['is_current'] = 1
                df['created_at'] = datetime.now()
                df['date_tested'] = df['date_tested'].apply(lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S %Z') if pd.notnull(x) else x)
                try:
                    wr.mysql.to_sql(
                        df=df,
                        con=mlwh_mysql_conn,
                        table="lighthouse_sample",
                        schema=mlwh_database,
                        mode="append",
                        use_column_names=True
                    )
                except:
                    print(f"{record['sourceMessageUuid']}: already processed")
                else:
                    print(f"{record['sourceMessageUuid']}: inserted {len(df.index)} samples")
            else:
                print(f"{record['sourceMessageUuid']}: ERRORS")
                select_query = (f"select distinct date(messageCreateDateUtc) as receivedDt, "
                                f"SUBSTRING_INDEX(file_name,'/',-1) as fileName, concat(source,' (',lab_id,')') as labId "
                                f"""from create_platemap where lh_source_plate_uuid = '{record["sourceMessageUuid"]}'""")
                cursor.execute(select_query)
                result = cursor.fetchone()
                if result is not None:
                    rcvddt = result[0]
                    filename = result[1]
                    labid = result[2]
                body = f"The platemap file {filename} received on {rcvddt} from {labid} is REJECTED due to the following errors<br><br>"
                update_query = (f"update create_platemap set errors = 1, feedbackRcvdDateUtc = '{datetime.now()}'"
                                f""" where lh_source_plate_uuid = '{record["sourceMessageUuid"]}'""")
                cursor.execute(update_query)
                for error in record["errors"]:
                    # print(record["sourceMessageUuid"] + ": " + str(error))
                    insert_query = (f"insert into create_platemap_errors values ('{record['sourceMessageUuid']}',"
                                    f"'{error['origin']}','{error['sampleUuid']}','{error['field']}',"
                                    f""""{error['description']}",'{datetime.now()}')""")
                    cursor.execute(insert_query)
                    body = body + json.dumps(record["errors"][0]) + "<br>"
                send_email_notification(body)
            cursor.close()
            cur.close()
        else:
            print(f"{record['sourceMessageUuid']}: NOT FOUND in DB")


def process_update_platemap_feedback(body):
    read_schema_response = get(
        f"{redpanda_host}subjects/update-plate-map-sample-feedback/versions/latest",
        headers={"X-API-KEY": redpanda_api_key}, verify=False).json()
    read_schema_obj = json.loads(read_schema_response["schema"])
    read_schema = parse_schema(read_schema_obj)
    string_reader = StringIO(body.decode("utf-8"))
    for record in json_reader(string_reader, read_schema):
        print(f"{record['sourceMessageUuid']}: processing update_platemap_feedback")
        select_query = "select * from update_platemap where messageUuid = '" + record["sourceMessageUuid"] + "'"
        df = wr.mysql.read_sql_query(
            sql=select_query,
            con=mdb_mysql_conn
        )
        if not df.empty:
            print(f"{record['sourceMessageUuid']}: found")
            cursor = mdb_mysql_conn.cursor()
            cur = mlwh_mysql_conn.cursor()
            if record["operationWasErrorFree"] is True:
                print(f"{record['sourceMessageUuid']}: no errors")
                # update platemap table
                update_query = (f"update update_platemap set errors = 0, feedbackRcvdDateUtc = "
                                f"'{datetime.now(timezone.utc)}' where messageUuid = '{record['sourceMessageUuid']}'")
                cursor.execute(update_query)
                # update lighthouse table
                drop_columns = ['messageUuid', 'messageCreateDateUtc', 'errors', 'feedbackRcvdDateUtc']
                df = df.drop(columns=drop_columns)
                df = df.rename(columns={"sampleUuid": "lh_sample_uuid",
                                        "preferentiallySequence": "preferentially_sequence"})
                df['updated_at'] = datetime.now()
                try:
                    for index, r in df.iterrows():
                        update_query = (f"""update lighthouse_sample set preferentially_sequence = 1, 
                        updated_at = '{datetime.now()}' where lh_sample_uuid = '{r["lh_sample_uuid"]}'""")
                        cur.execute(update_query)
                except Exception as e:
                    # print(e)
                    print(f"{record['sourceMessageUuid']}: already processed")
                else:
                    print(f"{record['sourceMessageUuid']}: updated {len(df.index)} samples")
            else:
                print(f"{record['sourceMessageUuid']}: ERRORS")
                update_query = (f"update update_platemap set errors = 1, feedbackRcvdDateUtc = "
                                f"'{datetime.now(timezone.utc)}' where messageUuid = '{record['sourceMessageUuid']}'")
                cursor.execute(update_query)
                for error in record["errors"]:
                    print(record["sourceMessageUuid"] + ": " + str(error))
                    insert_query = (f"insert into update_platemap_errors values ('{record['sourceMessageUuid']}',{error['typeId']},"
                                    f"""'{error['origin']}','{error['field']}',"{error['description']}",'{datetime.now()}')""")
                    cursor.execute(insert_query)
            cursor.close()
            cur.close()
        else:
            print(f"{record['sourceMessageUuid']}: NOT FOUND in DB")


def callback(ch, method, properties, body):
    packages.urllib3.disable_warnings(category=InsecureRequestWarning)
    if body:
        print(" [x] Received %r" % body)
        # print(f"message - {method.delivery_tag}")
        if properties.headers["subject"] == "create-plate-map-feedback":
            process_create_platemap_feedback(body)
        elif properties.headers["subject"] == "update-plate-map-sample-feedback":
            process_update_platemap_feedback(body)
    ch.basic_ack(delivery_tag=method.delivery_tag)
    if method.delivery_tag >= q_len:
        ch.stop_consuming()


if __name__ == "__main__":
    config_path = "job_configs/covid/platemap/processing.json"
    env = os.getenv("env")
    config: PlateMapConfig = PlateMapConfig.read_from_json(env, config_path)

    # rabbitmq variables
    queue = "heron.feedback"
    rabbitmq_user = config.common_config.databases.psd_rabbitmq.username
    rabbitmq_password = config.common_config.databases.psd_rabbitmq.password
    rabbitmq_host = config.common_config.databases.psd_rabbitmq.host
    rabbitmq_vhost = config.common_config.databases.psd_rabbitmq.virtual_host

    # redpanda variables
    redpanda_host = config.common_config.databases.psd_redpanda.host
    redpanda_api_key = config.common_config.databases.psd_redpanda.api_key

    # database variables
    mdb_host = config.common_config.databases.dt4_mysql.host
    mdb_port = config.common_config.databases.dt4_mysql.port
    mdb_user = config.common_config.databases.dt4_mysql.user_name
    mdb_password = config.common_config.databases.dt4_mysql.password
    mdb_database = "nifi_pipeline_mesh"

    mlwh_host = config.common_config.databases.mlwh_rw.host
    mlwh_port = config.common_config.databases.mlwh_rw.port
    mlwh_user = config.common_config.databases.mlwh_rw.user_name
    mlwh_password = config.common_config.databases.mlwh_rw.password
    if env == "dev":
        mlwh_database = "mlwhd_mlwarehouse_devdata"
    elif env == "prod":
        mlwh_database = "mlwarehouse"

    # database connections
    mdb_mysql_conn = pymysql.connect(database=mdb_database,
                                     user=mdb_user,
                                     password=mdb_password,
                                     host=mdb_host,
                                     port=mdb_port,
                                     ssl={'ssl': 'TRUE'},
                                     autocommit=True)
    mlwh_mysql_conn = pymysql.connect(database=mlwh_database,
                                      user=mlwh_user,
                                      password=mlwh_password,
                                      host=mlwh_host,
                                      port=mlwh_port,
                                      ssl={'ssl': 'TRUE'},
                                      autocommit=True)

    # rabbitmq connection
    credentials = PlainCredentials(rabbitmq_user, rabbitmq_password)
    ssl_options = SSLOptions(create_default_context(cafile="/etc/ssl/certs/ca-certificates.crt"))
    connection_params = ConnectionParameters(host=rabbitmq_host,
                                             port=5671,
                                             virtual_host=rabbitmq_vhost,
                                             credentials=credentials,
                                             ssl_options=ssl_options,
                                             heartbeat=20)
    connection = BlockingConnection(connection_params)
    channel = connection.channel()

    q = channel.queue_declare(queue, passive=True)
    q_len = q.method.message_count

    if q_len > 0:
        channel.basic_consume(queue=queue, on_message_callback=callback, auto_ack=False)
        try:
            channel.start_consuming()
        except KeyboardInterrupt:
            channel.stop_consuming()
    connection.close()
    print("Finished consuming.")
