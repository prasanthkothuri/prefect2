import os
from prefect import Flow, task
from prefect.tasks.shell import ShellTask
from prefect.schedules import CronSchedule
from prefect.tasks.prefect import StartFlowRun, GetFlowRun, FlowRunInfo
from prefect.run_configs import LocalRun
from prefect.storage import S3
from prefect.engine.state import Failed
from prefect.utilities.notifications import slack_notifier
from common.constants import covid_reports_base_path

# Create a Slack notifier handler
slack_handler = slack_notifier(only_states=[Failed])

# Configure S3 storage
storage = S3(
    bucket="prefect-flows-" + os.getenv("env"),
    client_options=dict(
        endpoint_url=os.getenv("s3_endpoint"),
        aws_access_key_id=os.getenv("s3_access_key"),
        aws_secret_access_key=os.getenv("s3_secret_key"),
    ),
)

# Define the flow schedule using CronSchedule
schedule = CronSchedule("30 07 * * 0-6")

# Define the ShellTask to execute the copy_priority_output_service.py script
copy_script = "sanger/covid/sample_priority/processing/table_services/output/copy_priority_output_service.py"
copy_report_task = ShellTask(
    name="copy_priority_report_to_devS3_and_sftp",
    return_all=True,
    log_stderr=True,
    stream_output=True,
    state_handlers=[slack_handler],
)

# Define tasks and flow for Prefect version 2
@task
def sample_priority_report_processing():
    # Placeholder for the processing task
    pass

# Start a new flow run using Prefect version 2
@task
def start_flow_run():
    return StartFlowRun(flow_name="sample_priority_report_processing")

# Get information about the started flow run
@task
def get_flow_run(flow_run_id):
    return GetFlowRun(flow_run_id=flow_run_id)

# Create the Prefect flow
with Flow("sample_priority_report", schedule=schedule, storage=storage) as f:
    flow_run_id = start_flow_run()
    flow_run_info = get_flow_run(flow_run_id=flow_run_id)
    sample_priority_report_processing.set_upstream(flow_run_info)
    result = copy_report_task(
        command=f"PYTHONPATH={covid_reports_base_path}:$PYTHONPATH python3 {covid_reports_base_path}/{copy_script}",
        upstream_tasks=[sample_priority_report_processing],
    )

# Define the flow run time configuration
f.run_config = LocalRun(labels=["dt4_local_" + os.getenv("env")])

# Register the flow with Prefect backend
f.register(
    project_name="COVID-" + os.getenv("env"),
    add_default_labels=False,
)

# Upload the flow to S3
f.storage.build()