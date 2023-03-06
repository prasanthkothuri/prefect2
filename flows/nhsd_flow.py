from prefect_shell import shell_run_command
from prefect import flow, task
import os
import pymysql
from datetime import datetime
from common.static_variables import *
import awswrangler as wr
import os
import pandas as pd
import boto3
import re
import pymysql as sql
from boto3 import s3
from sqlalchemy import create_engine, text
from sqlalchemy.types import Integer, Text, DateTime, Float
import numpy as np
from botocore.exceptions import ClientError
from sqlalchemy import exc
import pyarrow as pa


# nhsd_ingestion flow to chain the tasks
@flow(name="nhsd_ingestion")
def nhsd_ingestion():
    # variables
    endpoint_url = os.getenv("s3_endpoint")
    access_key = os.getenv("s3_access_key")
    secret_key = os.getenv("s3_secret_key")

    if "dev" in os.getenv("env"):
        s3_bucket = s3_bucket_dev
        mysql_host = mesh_mysql_host_dev
    else:
        s3_bucket = mesh_s3_bucket_prod
        mysql_host = mesh_mysql_host_prod

    mysql_password = os.getenv("mdb_mysql_password")
    mysql_user_in = mesh_mysql_user
    mysql_database_in = mesh_mysql_database
    eng_table_in = eng_table_nhsd
    sco_table_in = sco_table_nhsd
    nhsd_ingested_files_name_in = nhsd_ingested_files_name
    eng_file_type_in = eng_file_type
    sco_file_type_in = sco_file_type

    s3 = boto3.resource('s3',
                        aws_access_key_id=access_key,
                        aws_secret_access_key=secret_key,
                        endpoint_url=endpoint_url)

    @task(name='get_file_names')
    def get_file_names():
        files = s3.Bucket(s3_bucket).objects.all()
        file_names = []
        for my_bucket_object in files:
            file_name = my_bucket_object.key
            regex = re.search(r".ctl", str(file_name))

            if regex is not None:
                file_names.append(file_name)

        return file_names

    @task(name='search_files')
    def search_files(ctl_files, file_type, list_already_inserted):
        file_ingestion_list = []
        for each_file in ctl_files:
            if each_file not in list_already_inserted:
                s3_object = s3.Bucket(s3_bucket).Object(each_file).get()
                content = s3_object['Body'].readlines()
                try:
                    workflow_id = content[14]
                    regex = re.search(r"" + file_type, str(workflow_id))
                    if regex is not None:
                        file_ingestion_list.append(each_file[:-3] + "dat")
                except:
                    print(f"unable to process {each_file} file")
        return file_ingestion_list

    @task(name='insert_eng_data_into_mysql')
    def insert_eng_data_into_mysql(eng_files, list_already_inserted):
        print('--Ingesting England files--')
        sql_engine = create_engine(
            'mysql+pymysql://' + mysql_user_in + ':' + mysql_password + '@' + mysql_host + '/' + mysql_database_in,
            pool_recycle=3600, connect_args={'ssl': {'ssl': True}})
        db_connection = sql_engine.connect()
        for file in eng_files:
            if file not in list_already_inserted:
                s3_object = s3.Bucket(s3_bucket).Object(file).get()
                df = pd.read_csv(s3_object.get("Body"))
                df['SpecimenProcessedDate'] = pd.to_datetime(df['SpecimenProcessedDate'], errors='coerce').dt.strftime(
                    '%Y-%m-%d %H:%M:%S')
                df['TestStartDate'] = pd.to_datetime(df['TestStartDate'], errors='coerce').dt.strftime(
                    '%Y-%m-%d %H:%M:%S')
                df['TruncatedDateOfBirth'] = pd.to_datetime(df['TruncatedDateOfBirth'], errors='coerce').dt.strftime(
                    '%Y-%m-%d %H:%M:%S')
                df["SampleOfInterest"] = np.where(df["SampleOfInterest"] == True, 'true', None)

                # df.to_sql(eng_table, db_connection, if_exists='append', index=False)
                for i in range(len(df)):
                    try:
                        df.iloc[i:i + 1].to_sql(eng_table_nhsd, db_connection, if_exists='append', index=False)
                    except exc.IntegrityError:
                        pass  # or any other action
                list_already_inserted.append(file)
        db_connection.close()
        sql_engine.dispose()
        return list_already_inserted

    @task(name='insert_sco_data_into_mysql')
    def insert_sco_data_into_mysql(sco_files, list_already_inserted):
        print('--Ingesting Scotland files--')
        sql_engine = create_engine(
            'mysql+pymysql://' + mysql_user_in + ':' + mysql_password + '@' + mysql_host + '/' + mysql_database_in,
            pool_recycle=3600, connect_args={'ssl': {'ssl': True}})
        db_connection = sql_engine.connect()
        for file in sco_files:
            if file not in list_already_inserted:
                s3_object = s3.Bucket(s3_bucket).Object(file).get()
                df = pd.read_csv(s3_object.get("Body"))
                df = df[
                    ['Ch1Cq', 'Ch1Result', 'Ch1Target', 'Ch2Cq', 'Ch2Result', 'Ch2Target', 'Ch3Cq', 'Ch3Result',
                     'Ch3Target',
                     'Ch4Cq', 'Ch4Result', 'Ch4Target', 'OuterPostcode', 'ProcessingLabCode', 'ResultInfo',
                     'SpecimenID',
                     'SpecimenProcessedDate', 'TestResult', 'TestStartDate', 'VaccinationPeriod',
                     'HasRecentlyTravelled',
                     'RecentTravelDestination', 'PositiveTested', 'SampleOfInterest', 'VocOperation', 'VocAreaName']]
                df['SpecimenProcessedDate'] = pd.to_datetime(df['SpecimenProcessedDate'], errors='coerce').dt.strftime(
                    '%Y-%m-%d %H:%M:%S')
                df['TestStartDate'] = pd.to_datetime(df['TestStartDate'], errors='coerce').dt.strftime(
                    '%Y-%m-%d %H:%M:%S')
                df["SampleOfInterest"] = np.where(df["SampleOfInterest"] == True, 'true', None)

                # df.to_sql(sco_table, db_connection, if_exists='append', index=False)
                for i in range(len(df)):
                    try:
                        df.iloc[i:i + 1].to_sql(sco_table_nhsd, db_connection, if_exists='append', index=False)
                    except exc.IntegrityError:
                        pass  # or any other action
                list_already_inserted.append(file)
        db_connection.close()
        sql_engine.dispose()
        return list_already_inserted

    @task(name='write_to_s3')
    def write_to_s3(list):
        object = s3.Object(s3_bucket, nhsd_ingested_files_name)
        string_list = '\n'.join(list)
        object.put(Body=string_list)

    @task(name='get_processed_files')
    def get_processed_files():
        list_already_inserted = []
        try:
            s3_object = s3.Bucket(s3_bucket).Object(nhsd_ingested_files_name)
            list_already_inserted = s3_object.get()['Body'].read().decode('utf-8').splitlines()
        except ClientError as ex:
            if ex.response['Error']['Code'] == 'NoSuchKey':
                print('----First Time Ingestion---')
                list_already_inserted = []
        return list_already_inserted

    ctl_files = get_file_names()
    list_already_inserted = get_processed_files()
    eng_files = search_files(ctl_files, eng_file_type, list_already_inserted)
    sco_files = search_files(ctl_files, sco_file_type, list_already_inserted)
    list_already_inserted = insert_eng_data_into_mysql(eng_files, list_already_inserted)
    list_already_inserted = insert_sco_data_into_mysql(sco_files, list_already_inserted)
    write_to_s3(list_already_inserted)


@flow(name='nhsd_internal_delivery')
def nhsd_internal_delivery():
    s3_path = s3_path_nhsd_ind
    endpoint_url = endpoint_url_nhsd_ind
    nifi_user_password = os.getenv("mdb_mysql_password")
    query = nhsd_ind_query
    dtype = {
        'OuterPostcode': pa.dictionary(index_type=pa.int8(), value_type=pa.string()),
        'VaccinationStatus': pa.dictionary(index_type=pa.int8(), value_type=pa.string()),
        'TestCentreID': pa.dictionary(index_type=pa.int8(), value_type=pa.string()),
        'TestReason': pa.dictionary(index_type=pa.int8(), value_type=pa.string()),
        'VocAreaName': pa.dictionary(index_type=pa.int8(), value_type=pa.string()),
        'HasRecentlyTravelled': pa.dictionary(index_type=pa.int8(), value_type=pa.string())
    }
    grant_acl = {
        'Grants': [
            {
                'Grantee': {
                    'ID': 'pam-dt4-secure',
                    'Type': 'CanonicalUser',
                },
                'Permission': 'FULL_CONTROL'
            },
            {
                'Grantee': {
                    'ID': 'heronpipe',
                    'Type': 'CanonicalUser',
                },
                'Permission': 'READ'
            },
            {
                'Grantee': {
                    'ID': 'jc49',
                    'Type': 'CanonicalUser',
                },
                'Permission': 'READ'
            },
        ],
        'Owner': {
            'DisplayName': 'pam-dt4-secure',
            'ID': 'pam-dt4-secure'
        }
    }

    @task(name='mysql_fetch')
    def mysql_fetch(query):
        mysql_conn = pymysql.connect(database="nifi_pipeline_mesh", user="nifi_user", password=nifi_user_password,
                                     host="vm-mii-mesh-p1.internal.sanger.ac.uk", port=3306, ssl={'ssl': 'TRUE'})
        df = wr.mysql.read_sql_query(
            sql=query,
            con=mysql_conn,
            dtype=dtype
        )
        return df

    @task
    def print_output(output):
        print(output)

    @task(name='write_to_s3')
    def write_to_s3(results):
        wr.config.s3_endpoint_url = endpoint_url
        s3_session = boto3.Session(aws_access_key_id=os.getenv("s3_access_key"),
                                   aws_secret_access_key=os.getenv("s3_secret_key"))
        s3paths = wr.s3.to_csv(results, s3_path, boto3_session=s3_session, index=False)
        bucket = s3paths['paths'][0].split("/", 2)[2].split("/")[0]
        key = s3paths['paths'][0].split("/", 2)[2].split("/")[1]
        response = s3_session.resource('s3', endpoint_url=endpoint_url).Object(bucket, key).Acl().put(
            AccessControlPolicy=grant_acl)
        return response

    results = mysql_fetch(query)
    s3paths = write_to_s3(results)
    print_output(s3paths)


@flow(name='nhsd_seen_update')
def nhsd_seen_update():
    # variables
    if "dev" in os.getenv("env"):
        nifi_pipeline_mesh_host = mesh_mysql_host_dev
    else:
        nifi_pipeline_mesh_host = mesh_mysql_host_prod

    nifi_pipeline_mesh_password = os.getenv("mdb_mysql_password")
    nifi_pipeline_mesh_user = mesh_mysql_user
    nifi_pipeline_mesh_database = mesh_mysql_database

    eng_table = eng_table_nhsd
    sco_table = sco_table_nhsd

    mlw_host = mlw_host_nhsd
    mlw_password = os.getenv("mlwh_mysql_password")
    mlw_user = mlw_user_nhsd
    mlw_database = mlw_database_nhsd

    nhsd_seen_update_query = nhsd_seen_update_query_nhsd_seen_update
    @task(name='get_list_of_specimen_id')
    def get_list_of_specimen_id():
        connection = pymysql.connect(database=mlw_database, user=mlw_user, password=mlw_password, host=mlw_host,
                                     port=3435, ssl={'ssl': 'TRUE'})
        df = wr.mysql.read_sql_query(
            sql=nhsd_seen_update_query,
            con=connection
        )
        connection.close()
        list = df['SpecimenID'].to_list()

        return '(' + ','.join(f"'{x}'" for x in list) + ')'

    @task(name='update_seen')
    def update_seen(specimen_ids):
        print('---updating seen---')
        sql_engine = create_engine(
            'mysql+pymysql://' + nifi_pipeline_mesh_user + ':' + nifi_pipeline_mesh_password + '@' + nifi_pipeline_mesh_host + '/' + nifi_pipeline_mesh_database,
            pool_recycle=3600, connect_args={'ssl': {'ssl': True}})

        connection = sql_engine.connect()
        connection.execute(text('update ' + eng_table + ' set seen=1 where SpecimenID in ' + specimen_ids))
        print('--updated seen in Eng table--')

        connection.execute(text('update ' + sco_table + ' set seen=1 where SpecimenID in ' + specimen_ids))
        print('--updated seen in Sco table--')

        connection.close()

    specimen_ids = get_list_of_specimen_id()
    update_seen(specimen_ids)

@flow(name='nhsd')
def nhsd():
    nhsd_ingestion()
    nhsd_internal_delivery()
    nhsd_seen_update()


if __name__ == '__main__':
    nhsd()