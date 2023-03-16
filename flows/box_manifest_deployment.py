from flows.box_manifest_flow import box_manifest
from prefect.filesystems import RemoteFileSystem
from prefect.infrastructure.docker import DockerContainer
from prefect.deployments import Deployment
from prefect.server.schemas.schedules import CronSchedule

import os

env = os.environ['env']
remote_file_system_block = RemoteFileSystem.load(f"{env}-flow-storage")
docker_container_block = DockerContainer.load(f"{env}-docker-block")

print("deployment started")

deployment_boxmanifest = Deployment.build_from_flow(
    flow=box_manifest,
    name="box_manifest",
    work_pool_name=f"{env}-agent-pool",
    work_queue_name="default",
    storage=remote_file_system_block,
    infrastructure= docker_container_block,
    schedule=(CronSchedule(cron="20 07 * * 0-6", timezone="Europe/London")),
    apply=True
)

print("deployment completed and applied")
