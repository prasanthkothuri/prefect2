from prefect import task, Flow
from prefect.tasks.shell import ShellTask
from prefect.run_configs import DockerRun
from prefect.storage import S3
from prefect.engine.state import Failed
from prefect.utilities.notifications import slack_notifier
import os
from common.constants import spark_app_docker_image, spark_submit_script

# Configure S3 storage
storage = S3(
    bucket="prefect-flows-" + os.getenv("env"),
    client_options=dict(
        endpoint_url=os.getenv("s3_endpoint"),
        aws_access_key_id=os.getenv("s3_access_key"),
        aws_secret_access_key=os.getenv("s3_secret_key"),
    ),
)

# Create a Slack notifier handler
slack_handler = slack_notifier(only_states=[Failed])

# Define the ShellTask to execute spark-submit
spark_task = ShellTask(
    return_all=True,
    log_stderr=True,
    stream_output=True,
    state_handlers=[slack_handler],
)

# Define a task to redirect output to stdout
@task(log_stdout=True)
def print_output(output):
    print(output)

# Define the Prefect flow
with Flow("sample_priority_report_processing", storage=storage) as f:
    # spark_submit.sh {pipeline} {app} {job_config}
    processing_result = spark_task(
        command=f"/opt/covid_reports/spark_submit.sh sample_priority processing processing",
        task_args=dict(name="sample_priority_report_processing"),
    )

    # Task to print the output of the spark_task to stdout
    print_output(processing_result)

# Define the flow run time configuration
f.run_config = DockerRun(
    labels=["dt4_k8s_" + os.getenv("env")],
    image=f"{spark_app_docker_image}{os.getenv('image_tag')}",
)

# Register the flow with the Prefect backend
f.register(
    project_name="COVID-" + os.getenv("env"),
    add_default_labels=False,
)

# Upload the flow to S3
f.storage.build()
