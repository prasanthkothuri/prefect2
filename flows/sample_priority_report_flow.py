from prefect import task, flow
import os
from common.static_variables import spark_app_docker_image, spark_submit_script
from prefect_shell import shell_run_command
from sanger.covid.sample_priority.processing.config import SamplePriorityConfig

config_path = "job_configs/covid/sample_priority/processing.json"
env = os.getenv("env")
config: SamplePriorityConfig = SamplePriorityConfig.read_from_json(env, config_path)
bucket = config.common_config.buckets.s3.__getattribute__(
    config.output_tables.SamplePriorityConfig.bucket).split("//")[1]
copy_script = "sanger/covid/sample_priority/processing/table_services/output/copy_priority_output_service.py"
@flow(name='sample_priority_report')
def sample_priority_report():
    sample_priority_report_processing()
    #copy_report_task
    shell_run_command(command=f"PYTHONPATH={covid_reports_base_path}:$PYTHONPATH python3 {covid_reports_base_path}/{copy_script}")

@flow(name='sample_priority_report_processing')
def sample_priority_report_processing():
    shell_run_command(command=f"{spark_submit_script} sample_priority processing processing task_args=dict(name='sample_priority_report_processing')")

if __name__ == '__main__':
    sample_priority_report()
