import os
from prefect.infrastructure import DockerContainer

# get environment variables
env = os.environ['env']
s3_access_key = os.environ['s3_access_key']
s3_secret_key = os.environ['s3_secret_key']
s3_endpoint = os.environ['s3_endpoint']
k8s_apiserver = os.environ['K8S_APISERVER']
history_server_ip = os.environ['history_server_ip']
history_server_port = os.environ['history_server_port']
image_tag = os.environ['image_tag']
image = f"gitlab-registry.internal.sanger.ac.uk/pam-dt4/spark-service/spark-service-docker/spark-app:{image_tag}"
mdb_mysql_password = os.environ['mdb_mysql_password']

# set the variables required for docker block
dockerenv = {"env": env,
             "NAMESPACE": f"dt4{env}",
             "KUBECONFIG": f"/k8s/dt4{env}.config",
             "K8S_APISERVER": k8s_apiserver,
             "s3_endpoint": s3_endpoint,
             "s3_access_key": s3_access_key,
             "s3_secret_key": s3_secret_key,
             "history_server_port": history_server_port,
             "history_server_ip": history_server_ip,
             "PYTHONPATH": "/opt/covid_reports:/opt/prefect",
             "mdb_mysql_password": mdb_mysql_password
             }
volumes = [
    "/k8s:/k8s",
    "/root/.config:/root/.config"
]

# create the docker infrastructure block
dBlock = DockerContainer(auto_remove=True,
                         env=dockerenv,
                         image=image,
                         volumes=volumes,
                         image_pull_policy='ALWAYS')

try:
    dBlock.save(f"{env}-docker-block")
except Exception as e:
    print(f"Unable to create docker infra block, details: {e}")
