from prefect import task, flow
import os
from common.static_variables import spark_app_docker_image, spark_submit_script
from prefect_shell import shell_run_command


@flow(name='covid_ops_data_process')
def covid_ops_data_process():
    processing_result = shell_run_command(command=f"{spark_submit_script} covid_operations processing processing")


@flow(name='covid_ops_internal_delivery')
def covid_ops_internal_delivery():
    internal_delivery_result = shell_run_command(command=f"{spark_submit_script} covid_operations  internal_delivery "
                                                         f"internal_delivery")


@flow(name='covid_ops_dhsc_delivery')
def covid_ops_dhsc_delivery():
    dhsc_delivery_result = shell_run_command(command=f"{spark_submit_script} "
                                                     f"covid_operations "
                                                     f"dhsc_delivery "
                                                     f"dhsc_delivery")


# flow to chain the tasks
@flow(name='covid_ops_processing')
def covid_ops_processing():
    #covid_ops_data_process()
    covid_ops_internal_delivery()
    covid_ops_dhsc_delivery()


if __name__ == '__main__':
    covid_ops_processing()
