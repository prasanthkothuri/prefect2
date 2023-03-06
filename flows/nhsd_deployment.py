from flows.nhsd_flow import nhsd
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
    flow=nhsd,
    name="nhsd",
    work_queue_name="default-agent-pool",
    storage=remote_file_system_block,
    infrastructure= docker_container_block,
    schedule=(CronSchedule(cron="0 0 * * *", timezone="Europe/London")),
    apply=True
)

print("deployment completed and applied")
