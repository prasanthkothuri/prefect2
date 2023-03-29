from prefect import flow, task
import os
from common.static_variables import spark_app_docker_image, spark_submit_script
from prefect_shell import shell_run_command
from sanger.covid.cotrack.sample_fact.config import SampleFactConfig
from typing import Dict
from sqlalchemy import create_engine
from sqlalchemy.engine.base import Engine
from datetime import datetime
from prefect.client.orchestration import get_client
from prefect.server.schemas.sorting import FlowRunSort
import asyncio
import prefect.server.schemas as schemas
from prefect.task_runners import

def get_db_engine():
    config_path_events = "job_configs/covid/cotrack/sample_fact_processing.json"
    env = os.getenv("env")
    config: SampleFactConfig = SampleFactConfig.read_from_json(env, config_path_events)
    cotrack_pgdb = config.common_config.databases.__getattribute__(config.output_tables.processedSampleFact.db_type)
    pg_conf: Dict = {"host": cotrack_pgdb.host,
                     "port": cotrack_pgdb.port,
                     "user_name": cotrack_pgdb.user_name,
                     "password": cotrack_pgdb.password,
                     "database_name": cotrack_pgdb.database_name,
                     "schema": 'streaming',
                     "table": 'master_list_run'}
    conn_url = f"postgresql+psycopg2://{pg_conf['user_name']}:{pg_conf['password']}@{pg_conf['host']}:{pg_conf['port']}/{pg_conf['database_name']}"
    pg_engine: Engine = create_engine(conn_url)
    return pg_engine, pg_conf


@task(name='record_run_end', trigger=all_finished)
def record_run_end():
    this_flow_name='contrack_delta'
    pg_engine, pg_conf = get_db_engine()
    flows_to_check = ["streaming_events_delta", "streaming_iseq_delta", "streaming_samples_delta"]
    sub_flow_run_results = []
    success_ind = 1
    error_message = 'null'
    for fl in flows_to_check:
        fl_results = get_flow_run(fl)
        sub_flow_run_results.append(fl_results)
        if fl_results['state'].lower() == 'success':
            continue
        else:
            success_ind = 0
            error_message = f"'{fl_results['state_message']}'"

    this_flow_results = get_flow_run(this_flow_name)
    flow_run_id = this_flow_results['id']
    completed_at = f"'{datetime.now()}'" if success_ind == 1 else 'null'
    status = 'complete' if success_ind == 1 else 'error'
    error_message = 'null' if success_ind == 1 else error_message
    qry_stmt = f"update {pg_conf['schema']}.{pg_conf['table']} set status='{status}',error_message={error_message}, completed_at={completed_at}" \
               f" where flow_run_id='{flow_run_id}'"
    print(qry_stmt)
    conn = pg_engine.connect()
    conn.execute(qry_stmt)
    conn.close()


@task(name='record_run_start')
def record_run_start():
    schemas.filters.
    this_flow_name='contrack_delta'
    pg_engine, pg_conf = get_db_engine()
    flow_run_details = get_flow_run(this_flow_name)
    flow_name = flow_run_details['flow']['name']
    flow_run_id = flow_run_details['id']
    started_at = flow_run_details['start_time']
    load_type = 'delta'
    status = 'running'

    qry_stmt = f"insert into {pg_conf['schema']}.{pg_conf['table']}(flow_run_id,flow_name,started_at, load_type, status)" \
               f" values('{flow_run_id}','{flow_name}','{started_at}','{load_type}','{status}')"
    print(qry_stmt)
    conn = pg_engine.connect()
    conn.execute(qry_stmt)
    conn.close()


@task(name='get_flow_run')
def get_flow_run(flow_name):
    async def get_flow_runs():
        flow_filter: schemas.filters.FlowFilter = schemas.filters.FlowFilter(name='sequences')
        client = get_client()
        r = await client.read_flows(flow_filter=flow_filter)
        return r

    order = {"start_time": EnumValue("desc_nulls_last")}
    query = {
        "query": {
            with_args(
                "flow_run", {"where": {"flow": {"name": {"_eq": flow_name}}}, "order_by": order, "limit": 1})
            : {
                "flow": {"name": True},
                "id": True,
                "state": True,
                "start_time": True,
                "end_time": True,
                "state_message": True,
                "task_runs": {"task": {"name": True}, "start_time": True, "state": True, "state_message": True},
            }
        }
    }

    result = Client().graphql(query)
    flow_details = result.to_dict()
    flow_run = flow_details['data']['flow_run'][0]
    print(flow_run)
    return flow_run


@flow(name='streaming_events_delta')
def streaming_events_delta():
    shell_run_command(command=f"{spark_submit_script} cotrack events events_processing")


@flow(name='streaming_iseq_delta')
def streaming_iseq_delta():
    shell_run_command(command=f"{spark_submit_script} cotrack iseq_processing iseq_processing")


@flow(name='streaming_samples_delta')
def streaming_samples_delta():
    shell_run_command(command=f"{spark_submit_script} cotrack sample_fact sample_fact_processing")


@flow(name='contrack_delta')
def contrack_delta():
    record_run_start()
    streaming_events_delta()
    streaming_iseq_delta()
    streaming_samples_delta()
    record_run_end()
