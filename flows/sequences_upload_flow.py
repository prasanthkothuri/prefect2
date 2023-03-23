from prefect import task, flow
import os
from common.static_variables import spark_app_docker_image, spark_submit_script
from prefect_shell import shell_run_command
from sanger.covid.sequences.processing.sequences_config import SequencesConfig

config_path = "job_configs/covid/sequences/processing.json"
env = os.getenv("env")
config: SequencesConfig = SequencesConfig.read_from_json(env, config_path)
bucket = config.common_config.buckets.s3.__getattribute__(
    config.output_tables.sequencesToUploadS3.bucket).split("//")[1]


@flow(name='sequences_download')
def sequences_download():
    """
     This Prefect flow automates the sequences upload to CLIMB.
    """
    shell_run_command(command=f"{spark_submit_script} sequences processing processing")


@flow(name='sequences_upload')
def sequences_upload():
    # Upload to climb
    shell_run_command(command=f"rclone copy sanger_s3:{bucket}/sequences/upload/ climb_sftp:upload/ "
                              f"--ignore-existing --log-level INFO --progress --stats-one-line")

    # clean up the s3 location after upload
    shell_run_command(command=f"rclone purge sanger_s3:{bucket}/sequences/upload/ "
                              f"--log-level INFO --progress --stats-one-line")


@flow(name='sequences')
def sequences():
    sequences_download()
    sequences_upload()


if __name__ == '__main__':
    sequences()
