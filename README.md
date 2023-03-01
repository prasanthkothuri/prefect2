## prefect

Repository for prefect flows, agents etc

### developing prefect flows
```
pip install -r requirements.txt
```
docs - https://docs.prefect.io/

### create storage and docker infra blocks
```
. env
python3 infra/storage_block.py
python3 infra/docker_block.py
```
### create work pool
```
. env
python3 agents/create_workpool.py
```
### start agent
```
./agents/StartAgent.sh
```
### Access Development

prefect - http://172.27.24.162:4200/

### Deploy to development
CI/CD pipeline to be built once all the flows are migrated to prefect2

### Access Dev. logs

#### Spark

```
#get the pod for the driver
kubectl get pods -n dt4dev | grep {job_name}
#access spark driver logs
kubectl logs -f {pod_name} -n dt4dev
```

#### Prefect

```
ssh 172.27.24.162
tail -f /tmp/prefect.agent."$env".log
tail -f /tmp/prefect.server."$env".log
```