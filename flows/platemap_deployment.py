from flows.platemap_flow import platemap
from prefect.filesystems import RemoteFileSystem
from prefect.infrastructure.docker import DockerContainer
from prefect.deployments import Deployment
from prefect.server.schemas.schedules import CronSchedule
from flows.platemap_flow import platemap_feedback_processing

import os

env = os.environ['env']
remote_file_system_block = RemoteFileSystem.load(f"{env}-flow-storage")
docker_container_block = DockerContainer.load(f"{env}-docker-block")

print("deployment started")

platemap_deployment = Deployment.build_from_flow(
    flow=platemap,
    name="platemap",
    work_pool_name=f"{env}-agent-pool",
    work_queue_name="default",
    storage=remote_file_system_block,
    infrastructure= docker_container_block,
    schedule=(CronSchedule(cron="0 0 * * *", timezone="Europe/London")),
    apply=True
)

platemap_fb_deployment = Deployment.build_from_flow(
    flow=platemap_feedback_processing,
    name="platemap",
    work_pool_name=f"{env}-agent-pool",
    work_queue_name="default",
    storage=remote_file_system_block,
    infrastructure= docker_container_block,
    schedule=(CronSchedule(cron="0 0 * * *", timezone="Europe/London")),
    apply=True
)

print("deployment completed and applied")
