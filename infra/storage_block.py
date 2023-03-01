from prefect.filesystems import RemoteFileSystem
import os

# get environment variables
env = os.environ['env']
s3_endpoint = os.environ['s3_endpoint']
s3_access_key = os.environ['s3_access_key']
s3_secret_key = os.environ['s3_secret_key']

# create storage block
storage_block = RemoteFileSystem(
    basepath=f"s3://prefect2-flows-{env}",
    settings=dict(
        use_ssl=False,
        key=s3_access_key,
        secret=s3_secret_key,
        client_kwargs=dict(endpoint_url=s3_endpoint),
    ),
)
# save storage block
try:
    storage_block.save(f"{env}-flow-storage")
except Exception as e:
    print(e)
