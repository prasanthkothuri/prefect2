from flows.nhsd_flow import nhsd, nhsd_internal_delivery
from prefect.filesystems import RemoteFileSystem
from prefect.infrastructure.docker import DockerContainer
from prefect.deployments import Deployment
from prefect.server.schemas.schedules import CronSchedule

import os

env = os.environ['env']
remote_file_system_block = RemoteFileSystem.load(f"{env}-flow-storage")
docker_container_block = DockerContainer.load(f"{env}-docker-block")

print("deployment started")

deployment_nhsd_ingest = Deployment.build_from_flow(
    flow=nhsd,
    name="nhsd",
    work_pool_name=f"{env}-agent-pool",
    work_queue_name="default",
    storage=remote_file_system_block,
    infrastructure= docker_container_block,
    schedule=(CronSchedule(cron="20,50 * * * *", timezone="Europe/London")),
    apply=True
)

deployment_nhsd_internal_del = Deployment.build_from_flow(
    flow=nhsd_internal_delivery,
    name="nhsd",
    work_pool_name=f"{env}-agent-pool",
    work_queue_name="default",
    storage=remote_file_system_block,
    infrastructure= docker_container_block,
    schedule=(CronSchedule(cron="45 06 * * *", timezone="Europe/London")),
    apply=True
)

print("deployment completed and applied")
